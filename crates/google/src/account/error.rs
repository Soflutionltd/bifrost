//! Gmail-side translation boundary from `crate::Error` to the public
//! `AccountError`.
//!
//! The new error model has exactly one public path to `AccountError`:
//! `bifrost_types::AccountErrorBuilder`. This module funnels every
//! Gmail-internal failure through that builder, deriving recovery
//! centrally rather than emitting `RecoveryClass` values directly.
//!
//! Transport-only signals delegate to `bifrost_net::into_account_error`
//! so the net layer's transmission-state evidence and retry-after
//! parsing are not duplicated. Where Gmail has a more precise
//! interpretation (Gmail JSON error reason, history-id staleness,
//! cursor envelope failure), this module builds directly with
//! `AccountErrorBuilder` and attaches `WireCause::Gmail(signal)`.
//!
//! Audit boundary: the 2026-07 google bug sweep did NOT line-audit this
//! module; it was skipped as already test-dense. A later auditor should read
//! it fresh rather than assuming that sweep covered it.

use bifrost_types::{
    AccessCause, AccessErrorKind, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountOperation, AttemptCause, AuthCause, AuthErrorKind, Cause, CursorScope, DiagnosticText,
    ErrorScope, GmailSignal, InternalCause, InternalErrorKind, ItemOutcome, MutationSuccess,
    ObjectId, Protocol, ProtocolErrorKind, Provider, RequestCause, RequestErrorKind, ResourceKind,
    RetryHint, ServerCause, ServerErrorKind, StateCause, SyncStateErrorKind, ThrottleScope,
    TransmissionState, WireCause,
};
use bifrost_types::{BatchFailure, BatchItemId, BatchSuccess, TransportErrorKind};

use crate::error::{
    Error, GmailCursorFailure, GmailErrorEnvelope, GmailLocalError, GmailResponseError,
    ProviderFault,
};

/// Resource hint that drives `NotFound` and `PermissionDenied` payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GmailResource {
    Account,
    Message,
    Thread,
    Label,
    Draft,
    Identity,
    Vacation,
    Blob,
    PubSubWatch,
    Calendar,
    Contact,
}

impl GmailResource {
    fn to_resource_kind(self) -> Option<ResourceKind> {
        match self {
            Self::Message => Some(ResourceKind::Message),
            Self::Thread => Some(ResourceKind::Thread),
            Self::Label => Some(ResourceKind::Mailbox),
            Self::Draft => Some(ResourceKind::Draft),
            Self::Identity => Some(ResourceKind::Identity),
            Self::Vacation => Some(ResourceKind::Vacation),
            Self::PubSubWatch => Some(ResourceKind::PushSubscription),
            Self::Calendar => Some(ResourceKind::Calendar),
            Self::Contact => Some(ResourceKind::Contact),
            // `Blob` NotFound semantically means the parent message
            // cannot be opened; classify as the parent's message kind so
            // consumer routing surfaces the message-not-found UX.
            Self::Blob => Some(ResourceKind::Message),
            Self::Account => Some(ResourceKind::Account),
        }
    }
}

/// Per-call context for translating a Gmail error.
///
/// Constructors below should be preferred over building this by hand;
/// they encode the operation/scope/resource defaults for each Gmail
/// entry point.
#[derive(Clone, Debug)]
pub(crate) struct GmailErrorContext {
    pub(crate) surface: crate::quota::ApiSurface,
    observed_headers: Option<crate::error::GmailResponseHeaders>,
    pub(crate) operation: AccountOperation,
    pub(crate) scope: Option<ErrorScope>,
    pub(crate) resource: Option<GmailResource>,
    pub(crate) throttle_scope: Option<ThrottleScope>,
    pub(crate) idempotency_override: Option<bool>,
    /// True when the failing request targets `users.history.list`. The
    /// 404/410/`historyNotFound`/`failedPrecondition` mappings need
    /// this signal to route to `SyncState(CursorInvalid)` rather than
    /// `NotFound` or a generic provider refusal.
    pub(crate) history_endpoint: bool,
    /// Optional secondary diagnostic identifier (currently
    /// the Gmail attachment / blob id) that the central error model
    /// does not represent as an `ErrorScope`. Surfaced as
    /// `DiagnosticText::support_only` so support exports retain it.
    pub(crate) diagnostic_id: Option<String>,
}

impl GmailErrorContext {
    pub(crate) fn base(operation: AccountOperation) -> Self {
        Self {
            surface: crate::quota::ApiSurface::Gmail,
            observed_headers: None,
            operation,
            scope: None,
            resource: None,
            throttle_scope: Some(ThrottleScope::Account),
            idempotency_override: None,
            history_endpoint: false,
            diagnostic_id: None,
        }
    }

    pub(crate) fn open() -> Self {
        let mut ctx = Self::base(AccountOperation::Discover);
        ctx.resource = Some(GmailResource::Account);
        ctx
    }

    pub(crate) fn inventory() -> Self {
        let mut ctx = Self::base(AccountOperation::SyncInventory);
        ctx.scope = Some(ErrorScope::Cursor(CursorScope::Account));
        ctx.resource = Some(GmailResource::Message);
        ctx
    }

    /// The label-vocabulary refresh `repair_inventory` needs before it can
    /// answer any repair request. Sibling of `hydrate_labels`: the failing
    /// call is `labels.list`, so the resource is `Label` (not `Message`,
    /// which would classify a 404/403 as a missing or forbidden MESSAGE),
    /// and the scope is the account rather than
    /// `Cursor(CursorScope::Account)` - a transient `labels.list` fault must
    /// not hand bifrost-sync the scope coordinate its cursor directives are
    /// routed by. The operation stays the repair lane's own
    /// `SyncInventory`.
    pub(crate) fn repair_labels() -> Self {
        let mut ctx = Self::base(AccountOperation::SyncInventory);
        ctx.scope = Some(ErrorScope::Account);
        ctx.resource = Some(GmailResource::Label);
        ctx
    }

    pub(crate) fn changes() -> Self {
        let mut ctx = Self::base(AccountOperation::SyncChanges);
        ctx.scope = Some(ErrorScope::Cursor(CursorScope::Account));
        ctx.history_endpoint = true;
        ctx
    }

    pub(crate) fn establish_cursor() -> Self {
        let mut ctx = Self::base(AccountOperation::EstablishCursor);
        ctx.scope = Some(ErrorScope::Cursor(CursorScope::Account));
        ctx
    }

    pub(crate) fn hydrate_message(id: impl Into<String>) -> Self {
        let mut ctx = Self::base(AccountOperation::HydrateMessage);
        ctx.scope = Some(ErrorScope::Message {
            id: (id.into()).into(),
        });
        ctx.resource = Some(GmailResource::Message);
        ctx
    }

    /// The label-vocabulary refresh a hydration batch needs before it can
    /// canonicalize flags. The failure belongs to the account-wide
    /// `labels.list` resource, not to any one message of the batch that
    /// happened to be waiting on it: scoping it to an arbitrary id would
    /// name a message the request never touched, and there is no
    /// message-collection `ErrorScope` to name the batch as a whole.
    pub(crate) fn hydrate_labels() -> Self {
        let mut ctx = Self::base(AccountOperation::HydrateMessage);
        ctx.scope = Some(ErrorScope::Account);
        ctx.resource = Some(GmailResource::Label);
        ctx
    }

    pub(crate) fn hydrate_thread(id: impl Into<String>) -> Self {
        let mut ctx = Self::base(AccountOperation::HydrateThread);
        ctx.scope = Some(ErrorScope::Thread {
            id: (id.into()).into(),
        });
        ctx.resource = Some(GmailResource::Thread);
        ctx
    }

    pub(crate) fn mutation(operation: AccountOperation) -> Self {
        let mut ctx = Self::base(operation);
        ctx.resource = Some(GmailResource::Message);
        ctx
    }

    pub(crate) fn push_subscribe() -> Self {
        let mut ctx = Self::base(AccountOperation::PushSubscribe);
        // A Gmail watch covers the account cursor. If renewal reports a
        // deleted or expired subscription, CursorInvalid needs this coordinate
        // to derive RestartScope(Account) instead of failing error construction.
        ctx.scope = Some(ErrorScope::Cursor(CursorScope::Account));
        ctx.resource = Some(GmailResource::PubSubWatch);
        ctx
    }

    pub(crate) fn push_unsubscribe() -> Self {
        let mut ctx = Self::base(AccountOperation::PushUnsubscribe);
        ctx.resource = Some(GmailResource::PubSubWatch);
        ctx
    }

    pub(crate) fn open_blob(id: impl Into<String>) -> Self {
        let id = id.into();
        let mut ctx = Self::base(AccountOperation::OpenBlob);
        ctx.resource = Some(GmailResource::Blob);
        // Blob lives below a message in Gmail. Anchor diagnostics on
        // the parent message scope so consumer routing can find the
        // owning message; the blob id itself is preserved as a
        // diagnostic note.
        ctx.scope = Some(ErrorScope::Message {
            id: (id.clone()).into(),
        });
        ctx.diagnostic_id = Some(id);
        ctx
    }

    pub(crate) fn open_blob_range() -> Self {
        let mut ctx = Self::base(AccountOperation::OpenBlobRange);
        ctx.resource = Some(GmailResource::Blob);
        ctx
    }

    pub(crate) fn open_raw_rfc822(id: impl Into<String>) -> Self {
        let id = id.into();
        let mut ctx = Self::base(AccountOperation::OpenRawRfc822);
        ctx.resource = Some(GmailResource::Message);
        ctx.scope = Some(ErrorScope::Message {
            id: (id.clone()).into(),
        });
        ctx.diagnostic_id = Some(id);
        ctx
    }

    pub(crate) fn send() -> Self {
        let mut ctx = Self::base(AccountOperation::Send);
        ctx.idempotency_override = Some(false);
        ctx
    }

    pub(crate) fn draft(operation: AccountOperation) -> Self {
        let mut ctx = Self::base(operation);
        // Drafts other than DraftSend are write-but-not-side-effectful
        // outside Gmail; only DraftSend needs explicit non-idempotent.
        if matches!(operation, AccountOperation::DraftSend) {
            ctx.idempotency_override = Some(false);
        }
        ctx.resource = Some(GmailResource::Draft);
        ctx
    }

    pub(crate) fn search() -> Self {
        Self::base(AccountOperation::Search)
    }

    pub(crate) fn search_messages() -> Self {
        Self::base(AccountOperation::SearchMessages)
    }

    pub(crate) fn containers_list() -> Self {
        let mut ctx = Self::base(AccountOperation::ContainersList);
        ctx.resource = Some(GmailResource::Label);
        ctx
    }

    pub(crate) fn container(op: AccountOperation) -> Self {
        let mut ctx = Self::base(op);
        ctx.resource = Some(GmailResource::Label);
        ctx
    }

    pub(crate) fn identities_list() -> Self {
        let mut ctx = Self::base(AccountOperation::IdentitiesList);
        ctx.resource = Some(GmailResource::Identity);
        ctx
    }

    pub(crate) fn identity_update() -> Self {
        let mut ctx = Self::base(AccountOperation::IdentityUpdate);
        ctx.resource = Some(GmailResource::Identity);
        ctx
    }

    pub(crate) fn vacation_get() -> Self {
        let mut ctx = Self::base(AccountOperation::VacationGet);
        ctx.resource = Some(GmailResource::Vacation);
        ctx
    }

    pub(crate) fn vacation_set() -> Self {
        let mut ctx = Self::base(AccountOperation::VacationSet);
        ctx.resource = Some(GmailResource::Vacation);
        ctx
    }

    pub(crate) fn calendar_collection(operation: AccountOperation) -> Self {
        let mut ctx = Self::base(operation);
        ctx.surface = crate::quota::ApiSurface::Calendar;
        ctx.scope = Some(ErrorScope::CalendarCollection);
        ctx.resource = Some(GmailResource::Calendar);
        ctx
    }

    pub(crate) fn calendar_event(operation: AccountOperation, id: impl Into<String>) -> Self {
        let mut ctx = Self::base(operation);
        ctx.surface = crate::quota::ApiSurface::Calendar;
        ctx.scope = Some(ErrorScope::Calendar {
            id: (id.into()).into(),
        });
        ctx.resource = Some(GmailResource::Calendar);
        ctx
    }

    pub(crate) fn contact_collection(operation: AccountOperation) -> Self {
        let mut ctx = Self::base(operation);
        ctx.surface = crate::quota::ApiSurface::People;
        ctx.scope = Some(ErrorScope::ContactCollection);
        ctx.resource = Some(GmailResource::Contact);
        ctx
    }

    pub(crate) fn contact(operation: AccountOperation, id: impl Into<String>) -> Self {
        let mut ctx = Self::base(operation);
        ctx.surface = crate::quota::ApiSurface::People;
        ctx.scope = Some(ErrorScope::Contact {
            id: (id.into()).into(),
        });
        ctx.resource = Some(GmailResource::Contact);
        ctx
    }
}

/// Translate a `crate::Error` into the public `AccountError`. This is
/// the sole boundary; no other public path produces an `AccountError`
/// in this crate.
#[must_use]
pub(crate) fn into_account_error(error: Error, mut ctx: GmailErrorContext) -> AccountError {
    match error {
        Error::Net(net_error) => translate_net_error(net_error, &ctx),
        Error::ObservedNet(observed) => {
            ctx.observed_headers = Some(observed.headers);
            translate_net_error(observed.error, &ctx)
        }
        // `Error::Response` is a terminal HTTP response; the request was
        // acknowledged. `Error::JsonDecode` / `Error::Base64` happen
        // post-200 (decoding a successful response body), so they are
        // also classified as `Acknowledged`.
        Error::Response(resp) => translate_response(*resp, &ctx),
        Error::JsonDecode { source, .. } => parse_failed(
            &ctx,
            format!("Gmail JSON decode: {source}"),
            Some(TransmissionState::Acknowledged),
        ),
        Error::Base64 { encoding, source } => parse_failed(
            &ctx,
            format!("Gmail {encoding} decode failed: {source}"),
            Some(TransmissionState::Acknowledged),
        ),
        Error::Local(local) => translate_local(local, &ctx),
    }
}

/// Translate a per-batch mutation error into `ItemOutcome` lanes.
///
/// Returns `Err(AccountError)` when the failure terminates the stream
/// (retryable transport / auth / server-rate / quota / unknown-pre-
/// commit). Returns `Ok(Vec<ItemOutcome<MutationSuccess>>)` when the
/// failure is per-item (permanent provider refusal of a transmitted
/// batch, 404 for every id, etc.). `AccountError` is cloneable, so the
/// per-id outcomes carry direct clones - no template helper is needed.
pub(crate) fn mutation_error(
    ids: &[ObjectId],
    error: Error,
    ctx: GmailErrorContext,
) -> Result<Vec<ItemOutcome<MutationSuccess>>, AccountError> {
    let account_error = into_account_error(error, ctx);
    if terminates_mutation_stream(&account_error) {
        return Err(account_error);
    }

    // Build per-id outcomes. 404 for a transmitted batch is a
    // `NotFound(Message)` per id; any other permanent provider error
    // also lands as `Failed` per id with the cloned error.
    let outcomes = ids
        .iter()
        .map(|id| {
            ItemOutcome::Failed(BatchFailure::new(
                BatchItemId(id.0.clone()),
                account_error.clone(),
            ))
        })
        .collect();
    Ok(outcomes)
}

/// The terminal error for a mutation batch whose request had already
/// been dispatched when the account shut down.
///
/// The lane follows the EVIDENCE, not the loop that caught the error:
/// the bytes left this process and the answer was never read, which is
/// `TransmissionState::InFlight`, so the central mapping routes a
/// non-idempotent mutation to reconciliation rather than to a blind
/// retry. Reporting these ids `Failed` would assert the write did not
/// land, which we cannot know, and dropping them would lose the write
/// silently.
pub(crate) fn shutdown_inflight_error(operation: AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(TransportErrorKind::Network),
        Cause::Transport(bifrost_types::TransportCause::new(
            bifrost_types::TransportKind::Network,
            Some(DiagnosticText::support_only(
                "google account closed while a mutation request was in flight",
            )),
        )),
    )
    .provider(Provider::Gmail)
    .protocol(Protocol::Gmail)
    .operation(operation)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .native_code("account_closed_in_flight")
    .try_build()
    .expect("valid account error classification")
}

/// Build per-id `MutationSuccess::Skipped` outcomes for a legitimate
/// empty patch.
pub(crate) fn skipped_outcomes(ids: &[ObjectId]) -> Vec<ItemOutcome<MutationSuccess>> {
    ids.iter()
        .map(|id| {
            ItemOutcome::Succeeded(BatchSuccess::new(
                BatchItemId(id.0.clone()),
                MutationSuccess::Skipped,
            ))
        })
        .collect()
}

/// Detection helper for the TRASH-fallback path. Returns true only when
/// Gmail's structured reason is `insufficientPermissions` or
/// `forbidden` - i.e. the token cannot permanently delete and the driver
/// should retry the batch as a TRASH label patch.
///
/// A 403 that carries no parseable Gmail envelope, or one whose envelope
/// names no reason we recognize, is NOT a fallback trigger. A proxy or
/// policy layer answering 403 says nothing about delete scope, and
/// treating it as one silently downgrades a permanent delete into a move
/// to Trash that is then reported as applied.
///
/// The 403 can reach us in several shapes. `post_empty_json` converts a
/// terminal HTTP error into `Error::Response` itself, but a 403 that
/// surfaces from bifrost-net's retry loop retains its net error (with or
/// without observation metadata): a plain
/// `Status { code: 403, .. }`, or - when the final attempt was a
/// rate-limit / budget-exhaustion - wrapped in `RateLimited` /
/// `RetryBudgetExhausted` with the 403 preserved as `final_response`.
/// All of these are the same logical permission failure, so the detector
/// looks through every shape rather than only `Error::Response`, staying
/// consistent with how the classifier (which refines the wrapped final
/// response via the Gmail body) sees a 403.
pub(crate) fn is_batch_delete_scope_failure(error: &Error) -> bool {
    let reason = match error {
        // `Error::Response` carries a pre-parsed envelope; use it directly.
        Error::Response(resp) => {
            if resp.status != 403 {
                return false;
            }
            match resp.envelope.as_ref() {
                None => return false,
                Some(env) => env.primary_reason(),
            }
        }
        // The bifrost-net shapes preserve only the raw body bytes; parse
        // the Gmail envelope out of them.
        _ => {
            let Some((status, body)) = response_parts(error) else {
                return false;
            };
            if status != 403 {
                return false;
            }
            match parse_envelope(body) {
                None => return false,
                Some(env) => {
                    return matches!(
                        env.primary_reason(),
                        Some("forbidden") | Some("insufficientPermissions")
                    );
                }
            }
        }
    };
    matches!(reason, Some("forbidden") | Some("insufficientPermissions"))
}

/// Terminal response evidence shared by mutation decisions and translation.
/// Local hold refusals do not expose an earlier response as their own status.
pub(crate) fn response_parts(error: &Error) -> Option<(u16, &[u8])> {
    let net = match error {
        Error::Response(response) => return Some((response.status, &response.body)),
        Error::Net(net) => net,
        Error::ObservedNet(observed) => &observed.error,
        _ => return None,
    };
    match net {
        bifrost_net::Error::Status { code, body, .. } => Some((code.as_u16(), body.as_ref())),
        bifrost_net::Error::RateLimited { final_response, .. } => {
            Some((final_response.status.as_u16(), final_response.body.as_ref()))
        }
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            ..
        } => Some((final_response.status.as_u16(), final_response.body.as_ref())),
        _ => None,
    }
}

/// Build a Gmail-specific `AccountError` for the TRASH fallback path
/// when both primary delete and fallback fail. The fallback error is
/// the outermost interpretation; the primary delete's outermost cause
/// is attached as a secondary cause so the support export carries
/// both signals.
///
/// Uses `AccountError::into_builder()` to preserve all fallback fields
/// and derived values, then pushes the primary's outermost cause as
/// secondary evidence and adds a diagnostic note.
pub(crate) fn merge_delete_fallback_error(
    fallback: AccountError,
    primary: &AccountError,
) -> AccountError {
    let primary_outermost = primary.chain().outermost().clone();
    let primary_kind = primary
        .telemetry_fields()
        .native_code
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{:?}", primary.kind()));
    fallback
        .into_builder()
        .push_cause(primary_outermost)
        .text(DiagnosticText::support_only(format!(
            "primary batchDelete failed before TRASH fallback: {primary_kind}"
        )))
        .try_build()
        .expect("valid account error classification")
}

// ---------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------

fn terminates_mutation_stream(err: &AccountError) -> bool {
    use bifrost_types::RecoveryClass;
    match err.recovery() {
        RecoveryClass::Retry(_)
        | RecoveryClass::Reconcile(_)
        | RecoveryClass::Engine(_)
        | RecoveryClass::AuthLost
        | RecoveryClass::NeedsAdminConsent { .. }
        | RecoveryClass::NeedsPolicyChange
        | RecoveryClass::NoPermission { .. } => true,
        RecoveryClass::Unsupported(_)
        | RecoveryClass::ClientBug
        | RecoveryClass::ProviderContractViolation
        | RecoveryClass::ProviderRefused
        | RecoveryClass::UnknownPermanent
        | RecoveryClass::InternalFailure => false,
        // RecoveryClass is non-exhaustive. A newly introduced class must
        // retain its recovery verdict at the stream boundary until its
        // mutation fan-out behavior is deliberately classified here.
        _ => true,
    }
}

fn translate_net_error(net_error: bifrost_net::Error, ctx: &GmailErrorContext) -> AccountError {
    if let bifrost_net::Error::LocalHold { ref key, .. } = net_error {
        let budget = (ctx.surface == crate::quota::ApiSurface::Gmail)
            .then(|| bifrost_types::ThrottleBudget::new(key.clone()));
        let error = bifrost_net::into_account_error_with_throttle_scope(
            net_error,
            bifrost_net::NetErrorContext {
                provider: Some(Provider::Gmail),
                protocol: Protocol::Gmail,
                operation: ctx.operation,
                scope: ctx.scope.clone(),
            },
            ctx.throttle_scope.or(Some(ThrottleScope::Account)),
        );
        let mut builder = error.into_builder();
        if let Some(budget) = budget {
            builder = builder.throttle_budget(budget);
        }
        if let Some(idempotent) = ctx.idempotency_override {
            builder = builder.idempotency_override(idempotent);
        }
        return builder
            .try_build()
            .expect("valid local hold classification");
    }
    // For HTTP status responses the bifrost-net translator does generic
    // HTTP mapping. Gmail JSON reason codes are more precise, so we
    // peek at the body first.
    if let bifrost_net::Error::Status {
        ref code,
        ref body,
        ref headers,
    } = net_error
        && let Some(refined) =
            refine_status_with_gmail_body(code.as_u16(), headers, body.as_ref(), ctx)
    {
        return refined;
    }

    // For rate-limit / retry-budget exhaustion paths where the net
    // error carries a final response, attempt to refine using the
    // Gmail JSON reason. If parsing fails, the net-derived
    // classification stands.
    if let Some(refined) = try_refine_net_with_final_response(&net_error, ctx) {
        return refined;
    }

    let net_ctx = bifrost_net::NetErrorContext {
        provider: Some(Provider::Gmail),
        protocol: Protocol::Gmail,
        operation: ctx.operation,
        scope: ctx.scope.clone(),
    };
    bifrost_net::into_account_error_with_throttle_scope(
        net_error,
        net_ctx,
        ctx.throttle_scope.or(Some(ThrottleScope::Account)),
    )
}

fn try_refine_net_with_final_response(
    net_error: &bifrost_net::Error,
    ctx: &GmailErrorContext,
) -> Option<AccountError> {
    let response = match net_error {
        bifrost_net::Error::RateLimited { final_response, .. } => Some(final_response),
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(response),
            ..
        } => Some(response),
        _ => None,
    }?;
    refine_status_with_gmail_body(
        response.status.as_u16(),
        &response.headers,
        response.body.as_ref(),
        ctx,
    )
}

fn refine_status_with_gmail_body(
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
    ctx: &GmailErrorContext,
) -> Option<AccountError> {
    let env = parse_envelope(body)?;
    let resp_headers = ctx
        .observed_headers
        .clone()
        .unwrap_or_else(|| crate::error::GmailResponseHeaders::from_headers(headers));
    let resp = GmailResponseError {
        service: crate::error::GmailService::GmailApi,
        status,
        headers: resp_headers,
        body: bytes::Bytes::copy_from_slice(body),
        envelope: Some(env),
    };
    Some(translate_response(resp, ctx))
}

fn parse_envelope(body: &[u8]) -> Option<GmailErrorEnvelope> {
    #[derive(serde::Deserialize)]
    struct Wrapper {
        error: GmailErrorEnvelope,
    }
    serde_json::from_slice::<Wrapper>(body)
        .ok()
        .map(|w| w.error)
}

fn translate_response(resp: GmailResponseError, ctx: &GmailErrorContext) -> AccountError {
    let status = resp.status;
    let env = resp.envelope.as_ref();
    let reason = env
        .and_then(GmailErrorEnvelope::primary_reason)
        .map(str::to_owned);
    let message = env
        .and_then(GmailErrorEnvelope::primary_message)
        .map(str::to_owned);
    let signal = reason
        .as_deref()
        .map(map_reason_to_signal)
        .unwrap_or_else(|| GmailSignal::Unknown {
            code: status.to_string(),
        });

    let project = resp.headers.project.as_ref().filter(|_| {
        ctx.surface == crate::quota::ApiSurface::Gmail
            && crate::quota::project_kind(status, &resp.body).is_some()
    });
    let retry_hint = project
        .map(|observation| RetryHint::At(observation.deadline.at))
        .or_else(|| {
            resp.headers
                .retry_after
                .and(resp.headers.retry_at)
                .filter(|_| is_retry_after_applicable(status, &signal))
                .map(RetryHint::At)
        });

    let (kind, cause) = classify_response(status, &signal, ctx, retry_hint);

    let mut builder = AccountErrorBuilder::new(kind, cause)
        .provider(Provider::Gmail)
        .protocol(Protocol::Gmail)
        .operation(ctx.operation)
        .status(Some(status));

    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }

    // Native code: prefer Gmail reason, then top-level status, then HTTP status.
    let native = reason
        .clone()
        .or_else(|| env.and_then(|e| e.status.clone()))
        .unwrap_or_else(|| status.to_string());
    builder = builder.native_code(native);

    builder = builder.push_cause(Cause::Wire(WireCause::Gmail(signal.clone())));

    // the response was acknowledged. Push the attempt
    // evidence so non-idempotent ops with a `Server(Error)` cause
    // route to `Retry::SameRequest` (commit-rejection) rather than
    // `Reconcile(TransportDropAfterSend)`.
    builder = builder.push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::Acknowledged,
    )));

    if let Some(id) = resp.headers.request_id.as_deref() {
        builder = builder.request_id(id.to_owned());
    }
    if let Some(id) = resp.headers.trace_id.as_deref() {
        builder = builder.trace_id(id.to_owned());
    }

    if let Some(msg) = message {
        builder = builder.text(DiagnosticText::support_only(msg));
    } else if !resp.body.is_empty() {
        builder = builder.text(DiagnosticText::support_only(format!(
            "gmail response body: {}",
            String::from_utf8_lossy(resp.body.as_ref()),
        )));
    }

    if let Some(extra) = ctx.diagnostic_id.as_deref() {
        builder = builder.text(DiagnosticText::support_only(format!(
            "gmail diagnostic id: {extra}"
        )));
    }

    if let Some(project) = project {
        builder = builder.throttle_budget(project.budget.clone());
    }

    if let Some(throttle) = throttle_scope_for(status, &signal, ctx) {
        builder = builder.throttle_scope(throttle);
    }

    if let Some(idem) = ctx.idempotency_override {
        builder = builder.idempotency_override(idem);
    }

    builder
        .try_build()
        .expect("valid account error classification")
}

fn classify_response(
    status: u16,
    signal: &GmailSignal,
    ctx: &GmailErrorContext,
    retry_hint: Option<RetryHint>,
) -> (AccountErrorKind, Cause) {
    // First, dispatch on Gmail reason (most precise).
    match signal {
        GmailSignal::InvalidQuery => {
            return malformed_kind_cause(format!("HTTP {status} invalidQuery"));
        }
        GmailSignal::InvalidCredentials | GmailSignal::AuthError => {
            return (
                AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
                Cause::Auth(AuthCause::ReauthorizationRequired),
            );
        }
        GmailSignal::Forbidden => {
            return (
                AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
                Cause::Access(AccessCause::PermissionDenied {
                    resource: ctx.resource.and_then(GmailResource::to_resource_kind),
                }),
            );
        }
        GmailSignal::QuotaExceeded | GmailSignal::DailyLimitExceeded => {
            return (
                AccountErrorKind::Server(ServerErrorKind::QuotaExhausted),
                Cause::Server(ServerCause::QuotaExhausted { retry_hint }),
            );
        }
        GmailSignal::RateLimitExceeded | GmailSignal::UserRateLimitExceeded => {
            return (
                AccountErrorKind::Server(ServerErrorKind::RateLimited),
                Cause::Server(ServerCause::RateLimited { retry_hint }),
            );
        }
        GmailSignal::BackendError => {
            return (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_hint }),
            );
        }
        GmailSignal::FailedPrecondition if ctx.history_endpoint => {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            );
        }
        // outside the history endpoint a `failedPrecondition`
        // signals an etag/version mismatch on the targeted resource,
        // not a malformed client request.
        GmailSignal::FailedPrecondition => {
            return (
                AccountErrorKind::ConcurrencyConflict,
                Cause::State(StateCause::ConcurrencyConflict),
            );
        }
        GmailSignal::HistoryNotFound => {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            );
        }
        GmailSignal::NotFound if ctx.history_endpoint => {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            );
        }
        GmailSignal::NotFound => return not_found_kind_cause(ctx),
        GmailSignal::PreconditionFailed => {
            return (
                AccountErrorKind::ConcurrencyConflict,
                Cause::State(StateCause::ConcurrencyConflict),
            );
        }
        GmailSignal::PubSubSubscriptionDeleted | GmailSignal::PubSubSubscriptionExpired
            if matches!(ctx.operation, AccountOperation::PushSubscribe) =>
        {
            return (
                AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                Cause::State(StateCause::CursorInvalid),
            );
        }
        GmailSignal::PubSubSubscriptionDeleted | GmailSignal::PubSubSubscriptionExpired => {
            return (
                AccountErrorKind::Server(ServerErrorKind::Unavailable),
                Cause::Server(ServerCause::Unavailable { retry_hint }),
            );
        }
        GmailSignal::Unknown { code } => {
            // Specific stable Google reasons mapped explicitly per
            // the Gmail plan; these intentionally use Unknown { code }
            // and rely on `native_code` for downstream filters.
            match code.as_str() {
                "invalidArgument" => {
                    return malformed_kind_cause(format!("HTTP {status} invalidArgument"));
                }
                "insufficientPermissions" => {
                    return (
                        AccountErrorKind::Authorization(AccessErrorKind::InsufficientScope),
                        Cause::Access(AccessCause::InsufficientScope {
                            needed: gmail_scope_for(ctx.operation),
                        }),
                    );
                }
                "domainPolicy" => {
                    return (
                        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked),
                        Cause::Access(AccessCause::PolicyBlocked),
                    );
                }
                "conditionNotMet" => {
                    return (
                        AccountErrorKind::ConcurrencyConflict,
                        Cause::State(StateCause::ConcurrencyConflict),
                    );
                }
                "notAuthorizedToAccessThisResource" => {
                    return (
                        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
                        Cause::Access(AccessCause::PermissionDenied {
                            resource: ctx.resource.and_then(GmailResource::to_resource_kind),
                        }),
                    );
                }
                _ => {}
            }
            // Fall through to HTTP status mapping.
        }
        _ => {}
    }

    // HTTP status fallback (used when no recognized Gmail reason
    // matched, e.g. an unknown reason with a stable status code).
    classify_by_status(status, ctx, retry_hint)
}

fn classify_by_status(
    status: u16,
    ctx: &GmailErrorContext,
    retry_hint: Option<RetryHint>,
) -> (AccountErrorKind, Cause) {
    match status {
        400 if ctx.history_endpoint => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        400 | 422 => malformed_kind_cause(format!("HTTP {status}")),
        401 => (
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired),
            Cause::Auth(AuthCause::ReauthorizationRequired),
        ),
        403 => (
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied),
            Cause::Access(AccessCause::PermissionDenied {
                resource: ctx.resource.and_then(GmailResource::to_resource_kind),
            }),
        ),
        404 if ctx.history_endpoint => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        404 => not_found_kind_cause(ctx),
        410 if ctx.history_endpoint => (
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
            Cause::State(StateCause::CursorInvalid),
        ),
        412 => (
            AccountErrorKind::ConcurrencyConflict,
            Cause::State(StateCause::ConcurrencyConflict),
        ),
        413 => malformed_kind_cause("HTTP 413 payload too large".to_owned()),
        429 => (
            AccountErrorKind::Server(ServerErrorKind::RateLimited),
            Cause::Server(ServerCause::RateLimited { retry_hint }),
        ),
        500..=599 => (
            AccountErrorKind::Server(ServerErrorKind::Unavailable),
            Cause::Server(ServerCause::Unavailable { retry_hint }),
        ),
        other => (
            AccountErrorKind::Server(ServerErrorKind::Error {
                status: Some(other),
            }),
            Cause::Server(ServerCause::Error {
                status: Some(other),
            }),
        ),
    }
}

/// pick the OAuth scope `needed` for an `InsufficientScope`
/// `AccessCause` based on which operation tripped it. Returns the
/// shortest scope that grants the operation according to Google's
/// documented scope matrix.
fn gmail_scope_for(op: AccountOperation) -> &'static str {
    match op {
        // Composition / send flows.
        AccountOperation::Send => "gmail.send",
        AccountOperation::DraftCreate
        | AccountOperation::DraftUpdate
        | AccountOperation::DraftDiscard
        | AccountOperation::DraftSend => "gmail.compose",
        // Label CRUD lives under the labels scope.
        AccountOperation::ContainerCreate
        | AccountOperation::ContainerRename
        | AccountOperation::ContainerMove
        | AccountOperation::ContainerDelete
        | AccountOperation::ContainersList => "gmail.labels",
        // Settings (sendAs / vacation).
        AccountOperation::IdentitiesList | AccountOperation::IdentityUpdate => {
            "gmail.settings.basic"
        }
        AccountOperation::VacationGet
        | AccountOperation::VacationSet
        | AccountOperation::FiltersList
        | AccountOperation::FilterCreate
        | AccountOperation::FilterUpdate
        | AccountOperation::FilterDelete
        | AccountOperation::FilterValidate => "gmail.settings.basic",
        // Read-only hydration / inventory paths.
        AccountOperation::SyncInventory
        | AccountOperation::SyncChanges
        | AccountOperation::Hydrate
        | AccountOperation::HydrateThread
        | AccountOperation::HydrateMessage
        | AccountOperation::OpenBlob
        | AccountOperation::OpenBlobRange
        | AccountOperation::OpenRawRfc822
        | AccountOperation::Search
        | AccountOperation::SearchMessages
        | AccountOperation::Discover
        | AccountOperation::EstablishCursor
        | AccountOperation::DiscoverCursorScopes
        | AccountOperation::DiscoverMemberships
        | AccountOperation::ScopeLifecycle
        | AccountOperation::QuotaGet => "gmail.readonly",
        // Organization-directory lookup uses the People directory scope.
        AccountOperation::DirectorySearch => "directory.readonly",
        // `users.watch` reads the mailbox to seed the watch and emits
        // message bodies/labels on the topic, so the metadata scope is
        // insufficient; advertise the broader read scope.
        AccountOperation::PushSubscribe
        | AccountOperation::PushUnsubscribe
        | AccountOperation::PushStream => "gmail.readonly",
        // Drive hosting (`host_attachment`) uploads to and shares from
        // Google Drive, not Gmail; it needs a Drive scope.
        AccountOperation::HostAttachment => "drive.file",
        // Everything else - flag mutation, label membership, move,
        // destroy, etc. - requires gmail.modify.
        AccountOperation::UpdateFlags
        | AccountOperation::BulkMove
        | AccountOperation::BulkDestroy
        | AccountOperation::AddToContainer
        | AccountOperation::RemoveFromContainer
        | AccountOperation::SetKeyword
        | AccountOperation::SetLabelMembership
        | AccountOperation::SetCategory
        | AccountOperation::SetExtendedProperty
        | AccountOperation::SetIsRead
        | AccountOperation::AttachmentUpload
        | AccountOperation::Close
        | AccountOperation::Expunge => "gmail.modify",
        // AccountOperation is #[non_exhaustive] from bifrost-types; new
        // ops default to gmail.modify until explicit mapping is added.
        _ => "gmail.modify",
    }
}

fn malformed_kind_cause(detail: String) -> (AccountErrorKind, Cause) {
    (
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail),
        }),
    )
}

fn not_found_kind_cause(ctx: &GmailErrorContext) -> (AccountErrorKind, Cause) {
    // every typed `GmailResource` now maps to a real
    // `ResourceKind` (Blob -> Message because a missing blob means
    // the parent message can't be opened, PubSubWatch ->
    // PushSubscription, Account, etc.). The fallback to `Message` only
    // fires when no `GmailResource` was attached at all - which is a
    // producer bug rather than a documented coercion; every `not_found`
    // path in this crate sets `ctx.resource`.
    let what = ctx
        .resource
        .and_then(GmailResource::to_resource_kind)
        .unwrap_or(ResourceKind::Message);
    let id = ctx.scope.as_ref().and_then(|scope| match scope {
        ErrorScope::Message { id } => Some(id.0.clone()),
        ErrorScope::Mailbox { id } => Some(id.0.clone()),
        ErrorScope::Thread { id } => Some(id.0.clone()),
        ErrorScope::Calendar { id } => Some(id.0.clone()),
        ErrorScope::Contact { id } => Some(id.0.clone()),
        ErrorScope::Account
        | ErrorScope::Cursor(_)
        | ErrorScope::CalendarCollection
        | ErrorScope::ContactCollection => None,
        _ => None,
    });
    (
        AccountErrorKind::NotFound(what),
        Cause::Request(RequestCause::NotFound { what, id }),
    )
}

fn is_retry_after_applicable(status: u16, signal: &GmailSignal) -> bool {
    matches!(status, 429 | 500..=599)
        || matches!(
            signal,
            GmailSignal::RateLimitExceeded
                | GmailSignal::UserRateLimitExceeded
                | GmailSignal::QuotaExceeded
                | GmailSignal::DailyLimitExceeded
                | GmailSignal::BackendError
        )
}

fn throttle_scope_for(
    status: u16,
    signal: &GmailSignal,
    ctx: &GmailErrorContext,
) -> Option<ThrottleScope> {
    let is_throttle_status = matches!(status, 429)
        || matches!(
            signal,
            GmailSignal::RateLimitExceeded
                | GmailSignal::UserRateLimitExceeded
                | GmailSignal::QuotaExceeded
                | GmailSignal::DailyLimitExceeded
        );
    if is_throttle_status {
        ctx.throttle_scope.or(Some(ThrottleScope::Account))
    } else {
        None
    }
}

fn parse_failed(
    ctx: &GmailErrorContext,
    detail: String,
    attempt: Option<TransmissionState>,
) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Gmail,
            detail: Some(DiagnosticText::support_only(detail.clone())),
        }),
    )
    .provider(Provider::Gmail)
    .protocol(Protocol::Gmail)
    .operation(ctx.operation)
    .text(DiagnosticText::support_only(detail));
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    // post-200 decode failures (`JsonDecode`, `Base64`) get
    // `Acknowledged` because the response was received before the
    // decode failed.
    if let Some(state) = attempt {
        builder = builder.push_cause(Cause::Attempt(AttemptCause::new(state)));
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

fn malformed_request(ctx: &GmailErrorContext, detail: String) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.clone()),
        }),
    )
    .provider(Provider::Gmail)
    .protocol(Protocol::Gmail)
    .operation(ctx.operation)
    .text(DiagnosticText::support_only(detail));
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

/// A provider response broke its contract: `Protocol(_)` by `fault`, the
/// provider's fault, never the caller's (`Request`) or this crate's
/// (`Internal`). `state` is this exchange's transmission evidence:
/// `Acknowledged` when the offending response answered this exchange,
/// `Unsent` when the offending value came from an earlier one and this
/// exchange was refused before it was sent.
fn provider_fault(
    ctx: &GmailErrorContext,
    fault: ProviderFault,
    detail: String,
    state: TransmissionState,
) -> AccountError {
    let kind = match fault {
        ProviderFault::MissingField => ProtocolErrorKind::MissingField,
        ProviderFault::ParseFailed => ProtocolErrorKind::ParseFailed,
        ProviderFault::ContractViolation => ProtocolErrorKind::ContractViolation,
    };
    let detail = DiagnosticText::support_only(detail);
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Protocol(kind),
        Cause::Wire(WireCause::MalformedResponse {
            protocol: Protocol::Gmail,
            detail: Some(detail.clone()),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(state)))
    .provider(Provider::Gmail)
    .protocol(Protocol::Gmail)
    .operation(ctx.operation)
    .text(detail);
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

fn translate_local(local: GmailLocalError, ctx: &GmailErrorContext) -> AccountError {
    match local {
        GmailLocalError::Unsupported { operation, detail } => {
            let mut builder = AccountErrorBuilder::new(
                AccountErrorKind::Unsupported(operation),
                Cause::Request(RequestCause::Unsupported { operation }),
            )
            .provider(Provider::Gmail)
            .protocol(Protocol::Gmail)
            .operation(operation);
            if let Some(scope) = ctx.scope.clone() {
                builder = builder.scope(scope);
            }
            if let Some(text) = detail {
                builder = builder.text(DiagnosticText::support_only(text));
            }
            builder
                .try_build()
                .expect("valid account error classification")
        }
        GmailLocalError::InvalidRequest { operation, detail } => {
            let mut new_ctx = ctx.clone();
            new_ctx.operation = operation;
            malformed_request(&new_ctx, detail)
        }
        GmailLocalError::InvalidCursor { kind, detail } => {
            let (sync_kind, cause) = match kind {
                GmailCursorFailure::ProtocolMismatch
                | GmailCursorFailure::EnvelopeMismatch
                | GmailCursorFailure::SchemaMismatch => (
                    AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
                    Cause::State(StateCause::SchemaIncompatible),
                ),
                GmailCursorFailure::MalformedPayload => (
                    AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid),
                    Cause::State(StateCause::CursorInvalid),
                ),
            };
            let mut builder = AccountErrorBuilder::new(sync_kind, cause)
                .provider(Provider::Gmail)
                .protocol(Protocol::Gmail)
                .operation(ctx.operation)
                .scope(ErrorScope::Cursor(CursorScope::Account))
                .text(DiagnosticText::support_only(detail));
            builder = builder.native_code(match kind {
                GmailCursorFailure::ProtocolMismatch => "cursor_protocol_mismatch",
                GmailCursorFailure::EnvelopeMismatch => "cursor_envelope_mismatch",
                GmailCursorFailure::SchemaMismatch => "cursor_schema_mismatch",
                GmailCursorFailure::MalformedPayload => "cursor_malformed",
            });
            builder
                .try_build()
                .expect("valid account error classification")
        }
        GmailLocalError::AccountIdentityMismatch {
            cursor_email,
            profile_email,
        } => AccountErrorBuilder::new(
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
            Cause::State(StateCause::SchemaIncompatible),
        )
        .provider(Provider::Gmail)
        .protocol(Protocol::Gmail)
        .operation(ctx.operation)
        .scope(ErrorScope::Cursor(CursorScope::Account))
        .native_code("cursor_account_identity_mismatch")
        .text(DiagnosticText::support_only(format!(
            "cursor account {cursor_email} != open account {profile_email}"
        )))
        .try_build()
        .expect("valid account error classification"),
        // Every producer raises this after a complete response lacked the
        // field, so the evidence is `Acknowledged` (see the variant's doc).
        GmailLocalError::MissingField { field, detail } => provider_fault(
            ctx,
            ProviderFault::MissingField,
            format!("missing field `{field}`: {detail}"),
            TransmissionState::Acknowledged,
        ),
        GmailLocalError::BlobRangeUnsupported { blob_id } => {
            let mut builder = AccountErrorBuilder::new(
                AccountErrorKind::Unsupported(AccountOperation::OpenBlobRange),
                Cause::Request(RequestCause::Unsupported {
                    operation: AccountOperation::OpenBlobRange,
                }),
            )
            .provider(Provider::Gmail)
            .protocol(Protocol::Gmail)
            .operation(AccountOperation::OpenBlobRange)
            .text(DiagnosticText::support_only(format!(
                "gmail blob {blob_id} does not support range reads"
            )));
            if let Some(scope) = ctx.scope.clone() {
                builder = builder.scope(scope);
            }
            builder
                .try_build()
                .expect("valid account error classification")
        }
        // Client-side failures (`reference/error-model.md`): terminal
        // `InternalFailure`, remediation `ReportBug`, never the provider.
        GmailLocalError::Internal { detail } => {
            internal(ctx, InternalErrorKind::InvariantViolated, detail, None)
        }
        GmailLocalError::PageBudgetExceeded { detail } => {
            internal(ctx, InternalErrorKind::LimitExceeded, detail, None)
        }
        GmailLocalError::LimitExceededAfterResponse { detail } => internal(
            ctx,
            InternalErrorKind::LimitExceeded,
            detail,
            Some(TransmissionState::Acknowledged),
        ),
        // A whole response arrived and broke its contract: the provider's
        // fault, never the caller's (`Request`) or this crate's (`Internal`).
        GmailLocalError::ProviderResponse { fault, detail } => {
            provider_fault(ctx, fault, detail, TransmissionState::Acknowledged)
        }
        // The provider supplied the value earlier; this exchange was refused
        // before it was sent.
        GmailLocalError::ProviderValueUnusable { fault, detail } => {
            provider_fault(ctx, fault, detail, TransmissionState::Unsent)
        }
        // The provider handed back a page token it had already served: a
        // complete response carried it, so the evidence is `Acknowledged`.
        GmailLocalError::PageTokenRepeated { detail } => {
            let detail = DiagnosticText::support_only(detail);
            let mut builder = AccountErrorBuilder::new(
                AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation),
                Cause::Wire(WireCause::MalformedResponse {
                    protocol: Protocol::Gmail,
                    detail: Some(detail.clone()),
                }),
            )
            .push_cause(Cause::Attempt(AttemptCause::new(
                TransmissionState::Acknowledged,
            )))
            .provider(Provider::Gmail)
            .protocol(Protocol::Gmail)
            .operation(ctx.operation)
            .text(detail);
            if let Some(scope) = ctx.scope.clone() {
                builder = builder.scope(scope);
            }
            builder
                .try_build()
                .expect("valid account error classification")
        }
    }
}

fn internal(
    ctx: &GmailErrorContext,
    kind: InternalErrorKind,
    detail: String,
    attempt: Option<TransmissionState>,
) -> AccountError {
    let detail = DiagnosticText::support_only(detail);
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Internal(kind),
        Cause::Internal(InternalCause::new(kind, Some(detail.clone()))),
    )
    .provider(Provider::Gmail)
    .protocol(Protocol::Gmail)
    .operation(ctx.operation)
    .text(detail);
    if let Some(state) = attempt {
        builder = builder.push_cause(Cause::Attempt(AttemptCause::new(state)));
    }
    if let Some(scope) = ctx.scope.clone() {
        builder = builder.scope(scope);
    }
    builder
        .try_build()
        .expect("valid account error classification")
}

fn map_reason_to_signal(reason: &str) -> GmailSignal {
    match reason {
        "invalidQuery" => GmailSignal::InvalidQuery,
        "failedPrecondition" => GmailSignal::FailedPrecondition,
        "invalidCredentials" => GmailSignal::InvalidCredentials,
        "authError" => GmailSignal::AuthError,
        "quotaExceeded" => GmailSignal::QuotaExceeded,
        "dailyLimitExceeded" => GmailSignal::DailyLimitExceeded,
        "rateLimitExceeded" => GmailSignal::RateLimitExceeded,
        "userRateLimitExceeded" => GmailSignal::UserRateLimitExceeded,
        "forbidden" => GmailSignal::Forbidden,
        "notFound" => GmailSignal::NotFound,
        "preconditionFailed" => GmailSignal::PreconditionFailed,
        "backendError" => GmailSignal::BackendError,
        "historyNotFound" => GmailSignal::HistoryNotFound,
        "pubsubSubscriptionDeleted" => GmailSignal::PubSubSubscriptionDeleted,
        "pubsubSubscriptionExpired" => GmailSignal::PubSubSubscriptionExpired,
        other => GmailSignal::Unknown {
            code: other.to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{
        EngineDirective, RecoveryClass, RetryDisposition, RetryReason, TransportErrorKind,
    };
    use bytes::Bytes;

    fn gmail_response(status: u16, body: &str) -> Error {
        Error::response_from_parts(
            crate::error::GmailService::GmailApi,
            status,
            crate::error::GmailResponseHeaders::default(),
            Bytes::copy_from_slice(body.as_bytes()),
        )
    }

    fn body_with_reason(reason: &str) -> String {
        format!(
            r#"{{"error":{{"code":429,"message":"x","errors":[{{"domain":"usageLimits","reason":"{reason}"}}]}}}}"#
        )
    }

    #[test]
    fn generic_throttle_honors_context_scope() {
        let mut ctx = GmailErrorContext::inventory();
        ctx.throttle_scope = Some(ThrottleScope::Mailbox);
        let error = translate_net_error(
            bifrost_net::Error::Status {
                code: reqwest::StatusCode::TOO_MANY_REQUESTS,
                headers: reqwest::header::HeaderMap::new(),
                body: bytes::Bytes::from_static(b"proxy throttle"),
            },
            &ctx,
        );
        assert_eq!(
            error.telemetry_fields().throttle_scope,
            Some(ThrottleScope::Mailbox)
        );
    }

    #[test]
    fn invalid_query_maps_to_malformed_request() {
        let err = gmail_response(400, &body_with_reason("invalidQuery"));
        let acc = into_account_error(err, GmailErrorContext::search());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        assert!(acc.recovery().is_terminal());
    }

    #[test]
    fn invalid_argument_unknown_signal_is_malformed() {
        let err = gmail_response(400, &body_with_reason("invalidArgument"));
        let acc = into_account_error(err, GmailErrorContext::search());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
        // The builder sets native_code on diagnostics; telemetry exposes it.
        assert_eq!(acc.telemetry_fields().native_code, Some("invalidArgument"));
    }

    #[test]
    fn invalid_credentials_maps_to_reauth() {
        let err = gmail_response(401, &body_with_reason("invalidCredentials"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
        assert!(matches!(acc.recovery(), RecoveryClass::AuthLost));
    }

    #[test]
    fn auth_error_maps_to_reauth() {
        let err = gmail_response(401, &body_with_reason("authError"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        ));
    }

    #[test]
    fn forbidden_maps_to_permission_denied() {
        let err = gmail_response(403, &body_with_reason("forbidden"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
        ));
    }

    #[test]
    fn insufficient_permissions_maps_to_insufficient_scope() {
        let err = gmail_response(403, &body_with_reason("insufficientPermissions"));
        let acc = into_account_error(
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        );
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::InsufficientScope)
        ));
    }

    #[test]
    fn domain_policy_maps_to_policy_blocked() {
        let err = gmail_response(403, &body_with_reason("domainPolicy"));
        let acc = into_account_error(err, GmailErrorContext::send());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
        ));
    }

    #[test]
    fn quota_exceeded_maps_to_quota_with_throttle() {
        let err = gmail_response(429, &body_with_reason("quotaExceeded"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
        ));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.reason, RetryReason::QuotaExhausted);
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    }

    #[test]
    fn rate_limit_exceeded_maps_to_rate_limited() {
        let err = gmail_response(429, &body_with_reason("rateLimitExceeded"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::RateLimited)
        ));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.reason, RetryReason::RateLimited);
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    }

    #[test]
    fn user_rate_limit_uses_account_throttle() {
        let err = gmail_response(429, &body_with_reason("userRateLimitExceeded"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    }

    #[test]
    fn history_failed_precondition_maps_to_cursor_invalid() {
        let err = gmail_response(400, &body_with_reason("failedPrecondition"));
        let acc = into_account_error(err, GmailErrorContext::changes());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
        assert!(matches!(
            acc.recovery(),
            RecoveryClass::Engine(EngineDirective::RestartScope(CursorScope::Account))
        ));
    }

    #[test]
    fn history_404_maps_to_cursor_invalid() {
        let err = gmail_response(404, r#"{"error":{"code":404,"message":"x"}}"#);
        let acc = into_account_error(err, GmailErrorContext::changes());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
    }

    #[test]
    fn history_410_maps_to_cursor_invalid() {
        let err = gmail_response(410, r#"{"error":{"code":410}}"#);
        let acc = into_account_error(err, GmailErrorContext::changes());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
    }

    #[test]
    fn non_history_404_maps_to_not_found() {
        let err = gmail_response(404, r#"{"error":{"code":404}}"#);
        let acc = into_account_error(err, GmailErrorContext::hydrate_message("m1"));
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::NotFound(ResourceKind::Message)
        ));
    }

    #[test]
    fn http_500_maps_to_server_unavailable() {
        let err = gmail_response(500, "internal server error");
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.reason, RetryReason::ServerUnavailable);
    }

    #[test]
    fn http_503_maps_to_server_unavailable() {
        let err = gmail_response(503, "");
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
    }

    #[test]
    fn unparseable_body_falls_back_by_status() {
        let err = gmail_response(500, "<html>oops</html>");
        let acc = into_account_error(err, GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
        // Support-only text retains the raw body.
        let support = acc.support_consented();
        assert!(
            support
                .support_text
                .iter()
                .any(|t| t.contains("html") || t.contains("oops"))
        );
    }

    #[test]
    fn net_transport_delegates_with_gmail_context() {
        let net_err = bifrost_net::Error::Network {
            message: "dns failure".to_owned(),
            transmission_state: bifrost_types::TransmissionState::Unsent,
            source: None,
        };
        let acc = into_account_error(Error::Net(net_err), GmailErrorContext::inventory());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Transport(TransportErrorKind::Network)
        ));
        assert_eq!(acc.provider(), Some(Provider::Gmail));
        assert_eq!(acc.protocol(), Some(Protocol::Gmail));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.disposition, RetryDisposition::SameRequest);
    }

    #[test]
    fn cursor_protocol_mismatch_maps_to_schema_incompatible() {
        let err = Error::Local(GmailLocalError::InvalidCursor {
            kind: GmailCursorFailure::ProtocolMismatch,
            detail: "expected gmail".to_owned(),
        });
        let acc = into_account_error(err, GmailErrorContext::establish_cursor());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
        assert!(matches!(
            acc.recovery(),
            RecoveryClass::Engine(EngineDirective::SchemaIncompatible)
        ));
    }

    #[test]
    fn cursor_malformed_payload_maps_to_cursor_invalid() {
        let err = Error::Local(GmailLocalError::InvalidCursor {
            kind: GmailCursorFailure::MalformedPayload,
            detail: "truncated".to_owned(),
        });
        let acc = into_account_error(err, GmailErrorContext::establish_cursor());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
        ));
        assert!(matches!(
            acc.recovery(),
            RecoveryClass::Engine(EngineDirective::RestartScope(CursorScope::Account))
        ));
    }

    #[test]
    fn cursor_account_identity_mismatch_maps_to_schema_incompatible() {
        let err = Error::Local(GmailLocalError::AccountIdentityMismatch {
            cursor_email: "old@example.com".to_owned(),
            profile_email: "new@example.com".to_owned(),
        });
        let acc = into_account_error(err, GmailErrorContext::changes());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[test]
    fn unsupported_local_maps_to_unsupported() {
        let err = Error::unsupported(AccountOperation::SyncInventory);
        let acc = into_account_error(
            err,
            GmailErrorContext::base(AccountOperation::SyncInventory),
        );
        assert!(matches!(acc.kind(), AccountErrorKind::Unsupported(_)));
    }

    #[test]
    fn mutation_empty_patch_yields_skipped() {
        let ids = vec![ObjectId("m1".into()), ObjectId("m2".into())];
        let outcomes = skipped_outcomes(&ids);
        assert_eq!(outcomes.len(), 2);
        assert!(matches!(
            outcomes[0],
            ItemOutcome::Succeeded(BatchSuccess {
                output: MutationSuccess::Skipped,
                ..
            })
        ));
    }

    #[test]
    fn mutation_permanent_404_becomes_failed_per_id() {
        let ids = vec![ObjectId("m1".into()), ObjectId("m2".into())];
        let err = gmail_response(404, &body_with_reason("notFound"));
        let outcomes = mutation_error(
            &ids,
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        )
        .expect("per-item failures");
        assert_eq!(outcomes.len(), 2);
        assert!(matches!(
            outcomes[0],
            ItemOutcome::Failed(BatchFailure { .. })
        ));
    }

    #[test]
    fn mutation_429_terminates_stream() {
        let ids = vec![ObjectId("m1".into())];
        let err = gmail_response(429, &body_with_reason("rateLimitExceeded"));
        let result = mutation_error(
            &ids,
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        );
        assert!(result.is_err(), "rate-limit must terminate stream");
    }

    #[test]
    fn mutation_auth_loss_terminates_stream() {
        let ids = vec![ObjectId("m1".into())];
        let err = gmail_response(401, &body_with_reason("authError"));
        let result = mutation_error(
            &ids,
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        );
        assert!(result.is_err(), "auth loss must terminate stream");
    }

    #[test]
    fn batch_delete_403_with_forbidden_triggers_fallback() {
        let err = gmail_response(403, &body_with_reason("forbidden"));
        assert!(is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_403_with_insufficient_permissions_triggers_fallback() {
        let err = gmail_response(403, &body_with_reason("insufficientPermissions"));
        assert!(is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_500_does_not_trigger_fallback() {
        let err = gmail_response(500, "x");
        assert!(!is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_unparseable_response_403_does_not_trigger_fallback() {
        let err = gmail_response(403, "proxy policy response");
        assert!(!is_batch_delete_scope_failure(&err));
    }

    /// A well-formed Gmail envelope that names no reason we recognize is
    /// not evidence about delete scope either, on either shape the 403
    /// can arrive in.
    #[test]
    fn batch_delete_403_without_a_recognized_reason_does_not_trigger_fallback() {
        let body = r#"{"error":{"code":403,"message":"blocked by policy","errors":[{"domain":"global","reason":"domainPolicy"}]}}"#;
        assert!(!is_batch_delete_scope_failure(&gmail_response(403, body)));

        let wrapped = Error::Net(bifrost_net::Error::Status {
            code: reqwest::StatusCode::FORBIDDEN,
            body: Bytes::copy_from_slice(body.as_bytes()),
            headers: reqwest::header::HeaderMap::new(),
        });
        assert!(!is_batch_delete_scope_failure(&wrapped));

        // The envelope parses but names no reason at all - the shape that
        // the fallback used to treat as a permission failure.
        let reasonless = r#"{"error":{"code":403,"message":"blocked by policy"}}"#;
        assert!(!is_batch_delete_scope_failure(&gmail_response(
            403, reasonless
        )));
        let wrapped_reasonless = Error::Net(bifrost_net::Error::Status {
            code: reqwest::StatusCode::FORBIDDEN,
            body: Bytes::copy_from_slice(reasonless.as_bytes()),
            headers: reqwest::header::HeaderMap::new(),
        });
        assert!(!is_batch_delete_scope_failure(&wrapped_reasonless));
    }

    fn final_response(status: u16, body: &str) -> bifrost_net::FinalResponse {
        bifrost_net::FinalResponse {
            status: reqwest::StatusCode::from_u16(status).expect("status"),
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::copy_from_slice(body.as_bytes()),
        }
    }

    #[test]
    fn batch_delete_wrapped_net_status_403_triggers_fallback() {
        // bifrost-net keeps a terminal HTTP failure as `Error::Net(Status)`
        // when the retry loop didn't repackage it; the detector must look
        // through it.
        let err = Error::Net(bifrost_net::Error::Status {
            code: reqwest::StatusCode::FORBIDDEN,
            body: Bytes::copy_from_slice(body_with_reason("insufficientPermissions").as_bytes()),
            headers: reqwest::header::HeaderMap::new(),
        });
        assert!(is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_wrapped_net_status_403_no_body_does_not_trigger_fallback() {
        let err = Error::Net(bifrost_net::Error::Status {
            code: reqwest::StatusCode::FORBIDDEN,
            body: Bytes::new(),
            headers: reqwest::header::HeaderMap::new(),
        });
        assert!(!is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_rate_limited_403_final_response_triggers_fallback() {
        // A 429 retry loop whose final attempt returned 403 lands in
        // `RateLimited { final_response }`.
        let err = Error::Net(bifrost_net::Error::RateLimited {
            retry_after: None,
            final_response: final_response(403, &body_with_reason("forbidden")),
        });
        assert!(is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_retry_budget_exhausted_403_triggers_fallback() {
        let err = Error::Net(bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response(403, &body_with_reason("forbidden"))),
            retry_after_history: Vec::new(),
        });
        assert!(is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_wrapped_net_status_429_does_not_trigger_fallback() {
        // A genuine rate-limit (not a permission failure) must not be
        // misread as a scope failure.
        let err = Error::Net(bifrost_net::Error::Status {
            code: reqwest::StatusCode::TOO_MANY_REQUESTS,
            body: Bytes::copy_from_slice(body_with_reason("rateLimitExceeded").as_bytes()),
            headers: reqwest::header::HeaderMap::new(),
        });
        assert!(!is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_transport_failure_does_not_trigger_fallback() {
        let err = Error::Net(bifrost_net::Error::Network {
            message: "dns".to_owned(),
            transmission_state: bifrost_types::TransmissionState::Unsent,
            source: None,
        });
        assert!(!is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn batch_delete_403_quota_reason_does_not_trigger_fallback() {
        // A 403 whose reason is a quota signal is not a delete-scope
        // failure; only forbidden/insufficientPermissions qualify.
        let err = Error::Net(bifrost_net::Error::Status {
            code: reqwest::StatusCode::FORBIDDEN,
            body: Bytes::copy_from_slice(body_with_reason("dailyLimitExceeded").as_bytes()),
            headers: reqwest::header::HeaderMap::new(),
        });
        assert!(!is_batch_delete_scope_failure(&err));
    }

    #[test]
    fn blob_range_unsupported_maps_to_unsupported_open_blob_range() {
        let err = Error::Local(GmailLocalError::BlobRangeUnsupported {
            blob_id: "b1".to_owned(),
        });
        let acc = into_account_error(err, GmailErrorContext::open_blob_range());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Unsupported(AccountOperation::OpenBlobRange)
        ));
    }

    #[test]
    fn invalid_blob_id_maps_to_malformed_request() {
        let err = Error::invalid_request(AccountOperation::OpenBlob, "bad json");
        let acc = into_account_error(err, GmailErrorContext::open_blob("b1"));
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Request(RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn gmail_base64_decode_maps_to_protocol_parse_failed() {
        use base64::Engine;
        let decode_err = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode("!!!not-base64!!!")
            .expect_err("invalid base64");
        let err = Error::base64url(decode_err);
        let acc = into_account_error(err, GmailErrorContext::hydrate_message("m1"));
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        ));
    }

    #[test]
    fn send_uses_non_idempotent_context() {
        let err = gmail_response(500, "internal");
        let acc = into_account_error(err, GmailErrorContext::send());
        // Server(Unavailable) + non-idempotent + Acknowledged should
        // be `Retry(SameRequest)` (commit-rejection semantics).
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.disposition, RetryDisposition::SameRequest);
        assert_eq!(acc.operation(), Some(AccountOperation::Send));
    }

    #[test]
    fn request_id_header_preserved() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::HeaderName::from_static("x-goog-request-id"),
            reqwest::header::HeaderValue::from_static("abc-123"),
        );
        let resp_headers = crate::error::GmailResponseHeaders::from_headers(&headers);
        let resp = GmailResponseError {
            service: crate::error::GmailService::GmailApi,
            status: 500,
            headers: resp_headers,
            body: bytes::Bytes::from_static(b"x"),
            envelope: None,
        };
        let acc = translate_response(resp, &GmailErrorContext::inventory());
        let view = acc.telemetry_fields();
        assert_eq!(view.request_id, Some("abc-123"));
    }

    #[test]
    fn unknown_gmail_reason_preserves_native_code() {
        let err = gmail_response(418, &body_with_reason("teapot"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        // The builder sets native_code from the Gmail reason; telemetry
        // exposes it verbatim so dashboards can filter on unknown
        // reasons until the gap is closed.
        assert_eq!(acc.telemetry_fields().native_code, Some("teapot"));
    }

    // ---------------------------------------------------------------
    // Phase 5C decision coverage
    // ---------------------------------------------------------------

    /// every HTTP `Response` carries an
    /// `AttemptCause(Acknowledged)` so the central recovery mapping
    /// can distinguish acknowledged-vs-unsent for non-idempotent ops.
    #[test]
    fn response_pushes_acknowledged_attempt_cause() {
        let err = gmail_response(500, "x");
        let acc = into_account_error(err, GmailErrorContext::inventory());
        let attempt = acc
            .chain()
            .iter()
            .find_map(|cause| match cause {
                bifrost_types::Cause::Attempt(a) => Some(a.transmission_state),
                _ => None,
            })
            .expect("response carries an Attempt cause");
        assert_eq!(attempt, bifrost_types::TransmissionState::Acknowledged);
    }

    /// post-200 decode failures classify as Acknowledged.
    #[test]
    fn json_decode_pushes_acknowledged_attempt_cause() {
        let source = serde_json::from_slice::<serde_json::Value>(b"not json").expect_err("decode");
        let err = Error::JsonDecode {
            service: crate::error::GmailService::GmailApi,
            source,
        };
        let acc = into_account_error(err, GmailErrorContext::inventory());
        let attempt = acc
            .chain()
            .iter()
            .find_map(|cause| match cause {
                bifrost_types::Cause::Attempt(a) => Some(a.transmission_state),
                _ => None,
            })
            .expect("decode error carries an Attempt cause");
        assert_eq!(attempt, bifrost_types::TransmissionState::Acknowledged);
    }

    /// Every `missing_field` producer raises it after a complete response
    /// lacked the field, so it must carry `Acknowledged` evidence; before,
    /// it carried none and read as `Unsent`.
    #[test]
    fn missing_field_pushes_acknowledged_attempt_cause() {
        let acc = into_account_error(
            Error::missing_field("id", "created Gmail filter has no id"),
            GmailErrorContext::base(AccountOperation::FilterCreate),
        );
        assert_eq!(
            acc.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::MissingField)
        );
        let attempt = acc
            .chain()
            .iter()
            .find_map(|cause| match cause {
                bifrost_types::Cause::Attempt(a) => Some(a.transmission_state),
                _ => None,
            })
            .expect("a missing field carries an Attempt cause");
        assert_eq!(attempt, bifrost_types::TransmissionState::Acknowledged);
    }

    /// A value an earlier response supplied, refused before this exchange
    /// is sent: the provider's fault, but `Unsent`, not `Acknowledged`.
    #[test]
    fn provider_value_unusable_is_a_protocol_fault_with_unsent_evidence() {
        let acc = into_account_error(
            Error::provider_value_unusable(ProviderFault::ParseFailed, "profile email"),
            GmailErrorContext::base(AccountOperation::HostAttachment),
        );
        assert_eq!(
            acc.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
        );
        let attempt = acc.chain().iter().find_map(|cause| match cause {
            bifrost_types::Cause::Attempt(a) => Some(a.transmission_state),
            _ => None,
        });
        assert_eq!(attempt, Some(bifrost_types::TransmissionState::Unsent));
    }

    /// drafts route to `NotFound(Draft)`, not `Message`.
    #[test]
    fn not_found_draft_uses_draft_resource() {
        let err = gmail_response(404, &body_with_reason("notFound"));
        let acc = into_account_error(err, GmailErrorContext::draft(AccountOperation::DraftUpdate));
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::NotFound(ResourceKind::Draft)
        ));
    }

    /// identities route to `NotFound(Identity)`.
    #[test]
    fn not_found_identity_uses_identity_resource() {
        let err = gmail_response(404, &body_with_reason("notFound"));
        let acc = into_account_error(err, GmailErrorContext::identity_update());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::NotFound(ResourceKind::Identity)
        ));
    }

    /// vacation responder routes to `NotFound(Vacation)`.
    #[test]
    fn not_found_vacation_uses_vacation_resource() {
        let err = gmail_response(404, &body_with_reason("notFound"));
        let acc = into_account_error(err, GmailErrorContext::vacation_get());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::NotFound(ResourceKind::Vacation)
        ));
    }

    /// Pub/Sub watch routes to `NotFound(PushSubscription)`.
    #[test]
    fn not_found_pubsub_uses_push_subscription_resource() {
        let err = gmail_response(404, &body_with_reason("notFound"));
        let acc = into_account_error(err, GmailErrorContext::push_unsubscribe());
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::NotFound(ResourceKind::PushSubscription)
        ));
    }

    #[test]
    fn not_found_calendar_event_uses_calendar_resource_and_scope_id() {
        let err = gmail_response(404, &body_with_reason("notFound"));
        let acc = into_account_error(
            err,
            GmailErrorContext::calendar_event(AccountOperation::EventGet, "calendar::event"),
        );
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::NotFound(ResourceKind::Calendar)
        ));
        assert!(acc.chain().iter().any(|cause| {
            matches!(
                cause,
                Cause::Request(RequestCause::NotFound {
                    what: ResourceKind::Calendar,
                    id: Some(id),
                }) if id == "calendar::event"
            )
        }));
    }

    #[test]
    fn not_found_contact_uses_contact_resource_and_scope_id() {
        let err = gmail_response(404, &body_with_reason("notFound"));
        let acc = into_account_error(
            err,
            GmailErrorContext::contact(AccountOperation::ContactGet, "people/c1"),
        );
        assert!(matches!(
            acc.kind(),
            AccountErrorKind::NotFound(ResourceKind::Contact)
        ));
        assert!(acc.chain().iter().any(|cause| {
            matches!(
                cause,
                Cause::Request(RequestCause::NotFound {
                    what: ResourceKind::Contact,
                    id: Some(id),
                }) if id == "people/c1"
            )
        }));
    }

    /// `InsufficientScope::needed` reflects the operation.
    #[test]
    fn insufficient_scope_needed_for_send_is_gmail_send() {
        let err = gmail_response(403, &body_with_reason("insufficientPermissions"));
        let acc = into_account_error(err, GmailErrorContext::send());
        let outermost = acc.chain().outermost();
        match outermost {
            bifrost_types::Cause::Access(bifrost_types::AccessCause::InsufficientScope {
                needed,
            }) => assert_eq!(*needed, "gmail.send"),
            other => panic!("expected InsufficientScope, got {other:?}"),
        }
    }

    #[test]
    fn insufficient_scope_needed_for_container_is_gmail_labels() {
        let err = gmail_response(403, &body_with_reason("insufficientPermissions"));
        let acc = into_account_error(
            err,
            GmailErrorContext::container(AccountOperation::ContainerCreate),
        );
        let outermost = acc.chain().outermost();
        match outermost {
            bifrost_types::Cause::Access(bifrost_types::AccessCause::InsufficientScope {
                needed,
            }) => assert_eq!(*needed, "gmail.labels"),
            other => panic!("expected InsufficientScope, got {other:?}"),
        }
    }

    #[test]
    fn insufficient_scope_needed_for_inventory_is_readonly() {
        let err = gmail_response(403, &body_with_reason("insufficientPermissions"));
        let acc = into_account_error(err, GmailErrorContext::inventory());
        let outermost = acc.chain().outermost();
        match outermost {
            bifrost_types::Cause::Access(bifrost_types::AccessCause::InsufficientScope {
                needed,
            }) => assert_eq!(*needed, "gmail.readonly"),
            other => panic!("expected InsufficientScope, got {other:?}"),
        }
    }

    /// `failedPrecondition` outside the history endpoint
    /// routes to `ConcurrencyConflict`, not `Request(Malformed)`.
    #[test]
    fn non_history_failed_precondition_maps_to_concurrency_conflict() {
        let err = gmail_response(412, &body_with_reason("failedPrecondition"));
        let acc = into_account_error(
            err,
            GmailErrorContext::mutation(AccountOperation::UpdateFlags),
        );
        assert!(matches!(acc.kind(), AccountErrorKind::ConcurrencyConflict));
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        assert_eq!(advice.disposition, RetryDisposition::AfterStateRefresh);
        assert_eq!(advice.reason, RetryReason::ConcurrencyConflict);
    }

    /// `Retry-After` flows structurally through `ServerCause`
    /// and surfaces as `RetryAdvice::retry_hint`.
    #[test]
    fn retry_after_propagates_to_retry_hint() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("42"),
        );
        let now = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        let resp_headers = crate::error::GmailResponseHeaders::from_headers_at(&headers, now);
        let body = body_with_reason("rateLimitExceeded");
        let resp = GmailResponseError {
            service: crate::error::GmailService::GmailApi,
            status: 429,
            headers: resp_headers,
            body: bytes::Bytes::copy_from_slice(body.as_bytes()),
            envelope: super::parse_envelope(body.as_bytes()),
        };
        let acc = translate_response(resp, &GmailErrorContext::inventory());
        let RecoveryClass::Retry(advice) = acc.recovery() else {
            panic!("expected retry");
        };
        let hint = advice
            .retry_hint
            .expect("retry hint flows from Retry-After");
        assert_eq!(hint.min_delay(now), std::time::Duration::from_secs(42));
    }

    /// a 401 from Pub/Sub watch translates as terminal so
    /// the renewer can emit `WatchEvent::Terminated(AccountError)`
    /// rather than swallowing the failure into `Disconnected`.
    #[test]
    fn pubsub_auth_failure_is_terminal() {
        let err = gmail_response(401, &body_with_reason("authError"));
        let acc = into_account_error(err, GmailErrorContext::push_subscribe());
        assert!(acc.recovery().is_terminal());
        assert!(matches!(acc.recovery(), RecoveryClass::AuthLost));
    }

    /// a 500 from Pub/Sub watch is transient (Retry) so the
    /// renewer keeps trying and emits `Disconnected` instead of
    /// `Terminated`.
    #[test]
    fn pubsub_5xx_is_transient_retry() {
        let err = gmail_response(503, "");
        let acc = into_account_error(err, GmailErrorContext::push_subscribe());
        assert!(acc.recovery().is_retryable());
    }

    /// scope helper maps each gmail operation to a scope.
    #[test]
    fn gmail_scope_for_covers_canonical_operations() {
        assert_eq!(gmail_scope_for(AccountOperation::Send), "gmail.send");
        assert_eq!(
            gmail_scope_for(AccountOperation::DraftCreate),
            "gmail.compose"
        );
        assert_eq!(
            gmail_scope_for(AccountOperation::ContainerCreate),
            "gmail.labels"
        );
        assert_eq!(
            gmail_scope_for(AccountOperation::UpdateFlags),
            "gmail.modify"
        );
        assert_eq!(
            gmail_scope_for(AccountOperation::HydrateMessage),
            "gmail.readonly"
        );
    }

    /// `host_attachment` uploads to Google Drive, so an InsufficientScope
    /// must advertise a Drive scope - not `gmail.modify`.
    #[test]
    fn gmail_scope_for_host_attachment_is_drive() {
        assert_eq!(
            gmail_scope_for(AccountOperation::HostAttachment),
            "drive.file"
        );
    }

    /// `users.watch` reads message content, so the metadata scope is
    /// insufficient; the hint must advertise the broader read scope.
    #[test]
    fn gmail_scope_for_push_subscribe_is_readonly() {
        assert_eq!(
            gmail_scope_for(AccountOperation::PushSubscribe),
            "gmail.readonly"
        );
        assert_eq!(
            gmail_scope_for(AccountOperation::PushUnsubscribe),
            "gmail.readonly"
        );
    }
}
