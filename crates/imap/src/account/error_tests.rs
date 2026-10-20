//! Synthetic classification tests for the IMAP -> AccountError boundary.
//!
//! Tests construct `crate::Error` values directly and verify the
//! resulting `AccountError` kind, cause chain shape, scope/operation,
//! attempt evidence, and derived recovery. No live server, no mock
//! harness.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use bifrost_types::{
    AccessErrorKind, AccountErrorKind, AccountOperation, AuthErrorKind, Cause, CursorScope,
    EngineDirective, ErrorScope, FolderId, ImapResponseCode, ProtocolErrorKind, RecoveryClass,
    RequestCause, RequestErrorKind, ResourceKind, ServerErrorKind, StrategyDowngrade,
    SyncStateErrorKind, ThrottleScope, TransmissionState, TransportErrorKind, WireCause,
};

use super::{ImapErrorContext, into_account_error, shared_folder_error, strategy_failure};
use crate::Error;
use crate::account::sieve::SieveResponseCode;
use crate::types::{MailboxName, ResponseCode};

fn mailbox(name: &str) -> MailboxName {
    MailboxName::new(name).expect("valid mailbox name")
}

fn folder_cursor(name: &str) -> CursorScope {
    CursorScope::Folder(FolderId(name.to_owned()))
}

#[test]
fn io_unsent_builds_transport_network_with_retry() {
    let err = Error::io(std::io::Error::other("refused")).with_attempt(TransmissionState::Unsent);
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Network)
    ));
    assert!(account.recovery().is_retryable());
    let attempt = account.chain().iter().find_map(|c| match c {
        Cause::Attempt(a) => Some(a.transmission_state),
        _ => None,
    });
    assert_eq!(attempt, Some(TransmissionState::Unsent));
}

#[test]
fn io_inflight_non_idempotent_reconciles() {
    let err = Error::io(std::io::Error::other("eof")).with_attempt(TransmissionState::InFlight);
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::BulkMove));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Network)
    ));
    assert!(account.recovery().requires_reconciliation());
}

/// `Transport(_)` + `Acknowledged` is rejected by `try_build`
/// (`TransportAcknowledged`), and this boundary `.expect`s - so producing
/// that pair would panic rather than misclassify. `Error::with_attempt`
/// accepts any state on the transport variants and the driver does apply
/// `Acknowledged` after a tagged response, so the pair is constructible
/// even though no path builds it today.
///
/// It must demote to `InFlight`, not drop the cause: dropping would leave
/// `derive` reading its `Unsent` default and blind-retry a non-idempotent
/// operation, where `InFlight` reconciles instead.
#[test]
fn a_transport_error_claiming_acknowledged_demotes_to_inflight() {
    let err =
        Error::io(std::io::Error::other("reset")).with_attempt(TransmissionState::Acknowledged);
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::BulkMove));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Network)
    ));
    let attempt = account.chain().iter().find_map(|cause| match cause {
        Cause::Attempt(attempt) => Some(attempt.transmission_state),
        _ => None,
    });
    assert_eq!(
        attempt,
        Some(TransmissionState::InFlight),
        "acknowledged is a contradiction on a transport failure"
    );
    assert!(
        account.recovery().requires_reconciliation(),
        "a non-idempotent op must reconcile, never blind-retry"
    );
}

#[test]
fn timeout_inflight_idempotent_retries() {
    let err = Error::timeout().with_attempt(TransmissionState::InFlight);
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Timeout)
    ));
    assert!(account.recovery().is_retryable());
}

#[test]
fn closed_inflight_carries_attempt_cause() {
    let err = Error::closed().with_attempt(TransmissionState::InFlight);
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Transport(TransportErrorKind::Network)
    ));
    assert!(account.chain().iter().any(|c| matches!(
        c,
        Cause::Attempt(a) if a.transmission_state == TransmissionState::InFlight
    )));
}

// --- ManageSieve response codes (RFC 5804 1.3, imap-S1) ---

fn sieve_error(
    code: Option<SieveResponseCode>,
    op: AccountOperation,
) -> bifrost_types::AccountError {
    into_account_error(
        Error::Sieve {
            code,
            message: "sieve says no".into(),
        },
        ImapErrorContext::operation(op),
    )
}

/// The imap-S1 defect. `TRYLATER` explicitly means "transient, retry
/// later"; unparsed it fell through to `Server(Error { status: None })`
/// with an `Acknowledged` attempt, which derives terminal
/// `ProviderRefused` - so the engine never retried a server that asked
/// to be retried.
#[test]
fn sieve_trylater_is_retryable_not_terminal() {
    let account = sieve_error(
        Some(SieveResponseCode::TryLater),
        AccountOperation::FilterCreate,
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Server(ServerErrorKind::Unavailable)
    ));
    assert!(
        account.recovery().is_retryable(),
        "TRYLATER asks to be retried"
    );
    assert_eq!(account.telemetry_fields().native_code, Some("TRYLATER"));
}

#[test]
fn sieve_quota_is_quota_exhausted_and_throttles_the_account() {
    let account = sieve_error(
        Some(SieveResponseCode::Quota),
        AccountOperation::FilterCreate,
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
    ));
    if let RecoveryClass::Retry(advice) = account.recovery() {
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    } else {
        panic!("expected Retry");
    }
}

#[test]
fn sieve_nonexistent_is_a_missing_filter() {
    let account = sieve_error(
        Some(SieveResponseCode::NonExistent),
        AccountOperation::FilterDelete,
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::NotFound(ResourceKind::Filter)
    ));
    assert_eq!(account.message_key(), "notfound.filter");
}

#[test]
fn sieve_alreadyexists_is_a_concurrency_conflict() {
    let account = sieve_error(
        Some(SieveResponseCode::AlreadyExists),
        AccountOperation::FilterCreate,
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::ConcurrencyConflict
    ));
    assert!(account.recovery().is_retryable());
}

/// An unmodelled extension code, and a rejection with no code at all,
/// both keep the pre-existing behavior: the server refused and did not
/// say why, which is terminal. The table only overrides the codes where
/// that default is actively wrong.
#[test]
fn an_unmodelled_or_absent_sieve_code_stays_provider_refused() {
    for code in [None, Some(SieveResponseCode::Other("FROBNICATE".into()))] {
        let account = sieve_error(code, AccountOperation::FilterUpdate);
        assert!(matches!(
            account.kind(),
            AccountErrorKind::Server(ServerErrorKind::Error { status: None })
        ));
        assert!(account.recovery().is_terminal());
    }
}

/// A too-weak mechanism is a policy block, not a credential problem -
/// re-entering a password cannot fix it, so it must not derive the
/// reauthorization path.
#[test]
fn sieve_auth_too_weak_is_a_policy_block() {
    let account = sieve_error(
        Some(SieveResponseCode::AuthTooWeak),
        AccountOperation::Discover,
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
    ));
}

#[test]
fn auth_with_authentication_failed_maps_to_reauth() {
    let err = Error::auth_with_code(
        "credentials rejected".into(),
        Some(ResponseCode::AuthenticationFailed),
    );
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
    ));
    assert!(account.chain().iter().any(|c| matches!(
        c,
        Cause::Wire(WireCause::Imap(ImapResponseCode::AuthenticationFailed))
    )));
}

#[test]
fn auth_with_expired_maps_to_expired() {
    let err = Error::auth_with_code("expired".into(), Some(ResponseCode::Expired));
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authentication(AuthErrorKind::Expired)
    ));
}

#[test]
fn a_server_auth_rejection_carries_the_mechanism_at_the_support_tier() {
    // `AuthPolicyFailure::Display` already names mechanisms on the LOCAL
    // policy path. This is the server-rejection path, where "which mechanism
    // was refused" used to be unavailable at every consent tier.
    let err = Error::auth_with_mechanism(
        "credentials rejected".into(),
        Some(ResponseCode::AuthenticationFailed),
        "SCRAM-SHA-256-PLUS",
    );
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));

    let consented = account.support_consented();
    assert!(
        consented
            .support_text
            .iter()
            .any(|t| t.contains("SCRAM-SHA-256-PLUS")),
        "support-consented export must name the refused mechanism: {:?}",
        consented.support_text
    );
    // The original server text is not displaced by the attribution.
    assert!(
        consented
            .support_text
            .iter()
            .any(|t| t.contains("credentials rejected")),
        "{:?}",
        consented.support_text
    );

    // Support-only, not user-safe: the mechanism is evidence for an operator,
    // and the telemetry tier carries no free-form text at all.
    assert!(consented.user_safe_text.is_empty());
    assert_eq!(account.user_safe_text().count(), 0);
}

#[test]
fn an_unattributed_auth_rejection_keeps_its_bare_text() {
    // Producers that genuinely do not know the rung must not grow a fake one.
    let err = Error::auth_with_code("credentials rejected".into(), None);
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));
    let consented = account.support_consented();
    assert_eq!(consented.support_text, ["credentials rejected"]);
}

#[test]
fn auth_policy_maps_to_policy_blocked() {
    let err = Error::AuthPolicy(crate::error::AuthPolicyFailure::new(
        vec!["PLAIN".into()],
        Vec::new(),
    ));
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
    ));
}

#[test]
fn no_with_noperm_maps_to_permission_denied_with_scope_resource() {
    let err = Error::no_with_code("no perm".into(), Some(ResponseCode::NoPerm));
    let ctx = ImapErrorContext::operation(AccountOperation::UpdateFlags).with_message_id("msg-1");
    let account = into_account_error(err, ctx);

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
    let resource = account.chain().iter().find_map(|c| match c {
        Cause::Access(bifrost_types::AccessCause::PermissionDenied { resource }) => Some(*resource),
        _ => None,
    });
    assert_eq!(resource, Some(Some(ResourceKind::Message)));
}

#[test]
fn shared_folder_permission_loss_derives_disable_scope() {
    let err = Error::no_with_code("no perm".into(), Some(ResponseCode::NoPerm));
    let folder = mailbox("Shared/alice/INBOX");
    let owner = bifrost_types::MailboxId("alice".to_owned());
    let account = shared_folder_error(
        err,
        &folder,
        Some(&owner),
        ImapErrorContext::operation(AccountOperation::SyncChanges).with_folder_scope(&folder),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::ScopeRevoked)
    ));
    assert_eq!(
        *account.recovery(),
        RecoveryClass::Engine(EngineDirective::DisableScope(folder_cursor(
            "Shared/alice/INBOX"
        )))
    );
    assert_eq!(
        account.scope(),
        Some(&ErrorScope::Cursor(folder_cursor("Shared/alice/INBOX")))
    );
}

#[test]
fn personal_folder_permission_loss_stays_terminal() {
    let err = Error::no_with_code("no perm".into(), Some(ResponseCode::NoPerm));
    let folder = mailbox("INBOX");
    // A personal folder is untagged (`shared_owner == None`): the same
    // response must still derive terminal `NoPermission`, not quarantine.
    let account = shared_folder_error(
        err,
        &folder,
        None,
        ImapErrorContext::operation(AccountOperation::SyncChanges).with_folder_scope(&folder),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PermissionDenied)
    ));
    assert!(account.recovery().is_terminal());
    assert!(matches!(
        account.recovery(),
        RecoveryClass::NoPermission { .. }
    ));
}

#[test]
fn bad_without_code_maps_to_request_malformed() {
    let err = Error::bad_with_code("syntax".into(), None);
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
}

#[test]
fn incomplete_uid_expansion_is_an_acknowledged_local_request_limit() {
    let account = into_account_error(
        Error::SearchResultTruncated {
            returned: 1_000_000,
            omitted: Some(2_000_000),
        },
        ImapErrorContext::operation(AccountOperation::SearchMessages),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
    assert!(account.chain().iter().any(|cause| matches!(
        cause,
        Cause::Attempt(attempt) if attempt.transmission_state == TransmissionState::Acknowledged
    )));
    assert!(account.recovery().is_terminal());
}

#[test]
fn bad_with_serverbug_maps_to_contract_violation() {
    let err = Error::bad_with_code("bug".into(), Some(ResponseCode::ServerBug));
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    ));
}

#[test]
fn bye_with_unavailable_inflight_is_unavailable() {
    let err = Error::Bye {
        text: "going away".into(),
        code: Some(ResponseCode::Unavailable),
        attempt: None,
    }
    .with_attempt(TransmissionState::InFlight);
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::BulkMove));

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Server(ServerErrorKind::Unavailable)
    ));
    // Non-idempotent operation + InFlight => reconcile.
    assert!(account.recovery().requires_reconciliation());
}

#[test]
fn parse_maps_to_protocol_parse_failed() {
    let err = Error::Parse("garbled".into());
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed)
    ));
}

#[test]
fn invalid_input_maps_to_request_malformed() {
    let err = Error::InvalidInput("bad mailbox name".into());
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::ContainerCreate),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
}

#[test]
fn missing_capability_with_push_subscribe_maps_to_unsupported() {
    let err = Error::MissingCapability("IDLE".into());
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::PushSubscribe),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Unsupported(AccountOperation::PushSubscribe)
    ));
}

/// A client-side failure is never the server's fault: every internal
/// variant classifies `Internal(_)`, points at the implementation
/// (`ReportBug`), and none derives `ProviderContractViolation`.
#[test]
fn a_client_side_failure_is_internal_not_a_provider_fault() {
    use bifrost_types::{InternalErrorKind, RemediationAction};
    for (error, kind) in [
        (
            Error::Internal("missing pipeline result".into()),
            InternalErrorKind::InvariantViolated,
        ),
        (
            Error::internal_mid_exchange(
                "SCRAM server signature missing from client state",
                TransmissionState::Acknowledged,
            ),
            InternalErrorKind::InvariantViolated,
        ),
        (
            Error::LocalRuntime("failed to generate SCRAM nonce".into()),
            InternalErrorKind::RuntimeFailure,
        ),
        (
            Error::DriverPanicked {
                message: "boom".into(),
                attempt: Some(crate::error::ImapAttempt::new(TransmissionState::Unsent)),
            },
            InternalErrorKind::Panicked,
        ),
    ] {
        let account = into_account_error(
            error,
            ImapErrorContext::operation(AccountOperation::SyncChanges),
        );
        assert_eq!(account.kind(), &AccountErrorKind::Internal(kind));
        assert_eq!(account.recovery(), &RecoveryClass::InternalFailure);
        assert_eq!(
            account.suggested_remediation(),
            Some(&RemediationAction::ReportBug)
        );
    }
}

/// An omission by the server is `Protocol(MissingField)`, a wrong response is
/// `Protocol(ContractViolation)`, and the two share their recovery class and
/// their cause shape. They differ in connection fatality: an omission is
/// judged after the tagged OK, with the framing intact, so it does not retire
/// the connection, while a wrong response may have desynchronised it.
#[test]
fn a_missing_mandatory_response_is_missing_field_with_the_same_recovery() {
    let missing = into_account_error(
        Error::ProtocolMissing("SELECT missing UIDVALIDITY".into()),
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );
    let wrong = into_account_error(
        Error::Protocol("unexpected tag".into()),
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );
    assert_eq!(
        missing.kind(),
        &AccountErrorKind::Protocol(ProtocolErrorKind::MissingField)
    );
    assert_eq!(
        wrong.kind(),
        &AccountErrorKind::Protocol(ProtocolErrorKind::ContractViolation)
    );
    assert_eq!(
        missing.recovery(),
        &RecoveryClass::ProviderContractViolation
    );
    assert_eq!(missing.recovery(), wrong.recovery());
    assert!(
        missing
            .chain()
            .iter()
            .any(|cause| matches!(cause, Cause::Wire(WireCause::MalformedResponse { .. })))
    );
    assert!(!Error::ProtocolMissing("x".into()).is_connection_fatal());
}

/// A mid-exchange invariant failure retires the connection; the ordinary
/// internal failure, raised with the framing intact, does not.
#[test]
fn only_the_mid_exchange_internal_failure_is_connection_fatal() {
    assert!(Error::internal_mid_exchange("x", TransmissionState::InFlight).is_connection_fatal());
    assert!(!Error::Internal("x".into()).is_connection_fatal());
    assert!(!Error::LocalRuntime("x".into()).is_connection_fatal());
}

/// The central safety property of the driver-death phase: a non-idempotent
/// command the driver OWNED when it died may have been written, so it reads
/// the target back (`Reconcile`); one that never reached the driver is
/// plainly terminal. The same holds for a clean driver exit (`DriverGone`),
/// which used to lose the phase and so skipped reconciliation too.
#[test]
fn only_a_command_the_driver_owned_reconciles_after_driver_death() {
    let panicked = |phase| Error::DriverPanicked {
        message: "boom".into(),
        attempt: Some(crate::error::ImapAttempt::new(phase)),
    };
    let classify = |error| {
        into_account_error(error, ImapErrorContext::operation(AccountOperation::Send))
            .recovery()
            .clone()
    };

    assert_eq!(
        classify(panicked(TransmissionState::Unsent)),
        RecoveryClass::InternalFailure
    );
    let RecoveryClass::Reconcile(advice) = classify(panicked(TransmissionState::InFlight)) else {
        panic!("a panic with the command in flight must read the target back");
    };
    assert_eq!(
        advice.reason,
        bifrost_types::ReconcileReason::InternalFailureAfterSend
    );

    assert!(classify(Error::driver_gone_at(TransmissionState::Unsent)).is_retryable());
    assert!(classify(Error::driver_gone_at(TransmissionState::InFlight)).requires_reconciliation());
}

/// A local refusal made before any byte is never a provider fault. These pin
/// the recovery each local variant derives to at the account boundary, which
/// is what a consumer acts on: none may be `ProviderContractViolation`.
#[test]
fn a_wrong_session_state_is_a_client_bug() {
    let account = into_account_error(
        Error::InvalidState("command not valid in Authenticated state".into()),
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Request(RequestErrorKind::Malformed)
    ));
    assert!(matches!(
        account.recovery(),
        bifrost_types::RecoveryClass::ClientBug
    ));
}

#[test]
fn state_moved_under_a_queued_command_is_a_retry_after_refresh() {
    let account = into_account_error(
        Error::StateChangedBeforeSend("session moved to NotAuthenticated".into()),
        ImapErrorContext::operation(AccountOperation::Send),
    );
    assert_eq!(*account.kind(), AccountErrorKind::ConcurrencyConflict);
    let bifrost_types::RecoveryClass::Retry(advice) = account.recovery() else {
        panic!("expected a retry, got {:?}", account.recovery());
    };
    assert_eq!(
        advice.disposition,
        bifrost_types::RetryDisposition::AfterStateRefresh
    );
    let attempt = account.chain().iter().find_map(|c| match c {
        Cause::Attempt(a) => Some(a.transmission_state),
        _ => None,
    });
    assert_eq!(
        attempt,
        Some(TransmissionState::Unsent),
        "nothing was written, even for a non-idempotent operation"
    );
}

#[test]
fn an_operation_this_crate_does_not_implement_is_unsupported() {
    let account = into_account_error(
        Error::UnsupportedOperation("flag operation not implemented".into()),
        ImapErrorContext::operation(AccountOperation::UpdateFlags),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::Unsupported(AccountOperation::UpdateFlags)
    ));
}

#[test]
fn response_code_modified_maps_to_concurrency_conflict() {
    let err = Error::no_with_code("modified".into(), Some(ResponseCode::Modified(Vec::new())));
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::UpdateFlags),
    );
    assert_eq!(*account.kind(), AccountErrorKind::ConcurrencyConflict);
}

#[test]
fn response_code_nomodseq_maps_to_strategy_failure_condstore_to_basic() {
    let err = Error::no_with_code("nomodseq".into(), Some(ResponseCode::NoModSeq));
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::StrategyFailure)
    ));
    let downgrade = account.chain().iter().find_map(|c| match c {
        Cause::State(bifrost_types::StateCause::StrategyFailure { downgrade }) => Some(*downgrade),
        _ => None,
    });
    assert_eq!(downgrade, Some(StrategyDowngrade::CondstoreToBasic));
}

#[test]
fn response_code_overquota_maps_to_quota_with_account_throttle() {
    let err = Error::no_with_code("over".into(), Some(ResponseCode::OverQuota));
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Server(ServerErrorKind::QuotaExhausted)
    ));
    if let RecoveryClass::Retry(advice) = account.recovery() {
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Account));
    } else {
        panic!("expected Retry");
    }
}

#[test]
fn response_code_limit_in_mailbox_scope_throttles_mailbox() {
    let err = Error::no_with_code("limit".into(), Some(ResponseCode::Limit));
    let ctx =
        ImapErrorContext::operation(AccountOperation::SyncChanges).with_mailbox(&mailbox("INBOX"));
    let account = into_account_error(err, ctx);

    assert!(matches!(
        account.kind(),
        AccountErrorKind::Server(ServerErrorKind::RateLimited)
    ));
    if let RecoveryClass::Retry(advice) = account.recovery() {
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Mailbox));
    } else {
        panic!("expected Retry");
    }
}

/// The shape production actually builds. Every folder-scoped producer
/// uses `with_folder_scope`, so the scope is `Cursor(Folder(_))` and not
/// `Mailbox { id }` - a throttle reader matching only the latter would
/// widen every real per-mailbox `[LIMIT]` to an account-wide pause while
/// the `with_mailbox` test above still passed.
#[test]
fn response_code_limit_in_folder_scope_throttles_mailbox() {
    let err = Error::no_with_code("limit".into(), Some(ResponseCode::Limit));
    let ctx = ImapErrorContext::operation(AccountOperation::SyncChanges)
        .with_folder_scope(&mailbox("INBOX"));
    let account = into_account_error(err, ctx);

    if let RecoveryClass::Retry(advice) = account.recovery() {
        assert_eq!(advice.throttle_scope, Some(ThrottleScope::Mailbox));
    } else {
        panic!("expected Retry");
    }
}

/// Same migration gap on the id reader: the folder id is present in the
/// scope, so it must reach `RequestCause::NotFound { id }` rather than
/// being dropped because the scope is a `Cursor` rather than a `Mailbox`.
#[test]
fn response_code_nonexistent_in_folder_scope_keeps_the_folder_id() {
    let err = Error::no_with_code("nope".into(), Some(ResponseCode::NonExistent));
    let ctx = ImapErrorContext::operation(AccountOperation::ContainerDelete)
        .with_folder_scope(&mailbox("INBOX/old"));
    let account = into_account_error(err, ctx);

    assert!(matches!(
        account.kind(),
        AccountErrorKind::NotFound(ResourceKind::Mailbox)
    ));
    let found = account.chain().iter().any(|cause| {
        matches!(
            cause,
            Cause::Request(RequestCause::NotFound { id: Some(id), .. }) if id == "INBOX/old"
        )
    });
    assert!(found, "the folder id must survive into the cause chain");
}

#[test]
fn response_code_nonexistent_in_mailbox_scope_maps_to_notfound_mailbox() {
    let err = Error::no_with_code("nope".into(), Some(ResponseCode::NonExistent));
    let ctx = ImapErrorContext::operation(AccountOperation::ContainerDelete)
        .with_mailbox(&mailbox("INBOX/old"));
    let account = into_account_error(err, ctx);

    assert!(matches!(
        account.kind(),
        AccountErrorKind::NotFound(ResourceKind::Mailbox)
    ));
}

#[test]
fn response_code_notification_overflow_maps_to_cursor_invalid() {
    let err = Error::no_with_code(
        "overflow".into(),
        Some(ResponseCode::NotificationOverflow(None)),
    );
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::PushStream)
            .with_cursor_scope(bifrost_types::CursorScope::Account),
    );
    assert!(matches!(
        account.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
    ));
}

#[test]
fn every_response_code_variant_maps_to_imap_response_code() {
    // Exercise the wire-code mapping for every well-known variant.
    // Compilation is the meaningful assertion: if a `ResponseCode`
    // variant is added without extending `imap_response_code`, the
    // exhaustive match in `account/error.rs` won't compile.
    let codes: Vec<ResponseCode> = vec![
        ResponseCode::Alert,
        ResponseCode::BadCharset(Vec::new()),
        ResponseCode::Parse,
        ResponseCode::ReadOnly,
        ResponseCode::ReadWrite,
        ResponseCode::TryCreate,
        ResponseCode::UidNext(1),
        ResponseCode::UidValidity(1),
        ResponseCode::Unseen(1),
        ResponseCode::HighestModSeq(1),
        ResponseCode::Modified(Vec::new()),
        ResponseCode::NoModSeq,
        ResponseCode::Closed,
        ResponseCode::MailboxId("x".into()),
        ResponseCode::Unavailable,
        ResponseCode::AuthenticationFailed,
        ResponseCode::AuthorizationFailed,
        ResponseCode::Expired,
        ResponseCode::PrivacyRequired,
        ResponseCode::ContactAdmin,
        ResponseCode::NoPerm,
        ResponseCode::InUse,
        ResponseCode::ExpungeIssued,
        ResponseCode::Corruption,
        ResponseCode::ServerBug,
        ResponseCode::ClientBug,
        ResponseCode::Cannot,
        ResponseCode::Limit,
        ResponseCode::OverQuota,
        ResponseCode::AlreadyExists,
        ResponseCode::NonExistent,
        ResponseCode::NewName(None),
        ResponseCode::Referral(None),
        ResponseCode::UrlMech(None),
        ResponseCode::BadUrl(None),
        ResponseCode::BadComparator(None),
        ResponseCode::Annotate(None),
        ResponseCode::Annotations(None),
        ResponseCode::TempFail(None),
        ResponseCode::MaxConvertMessages(None),
        ResponseCode::MaxConvertParts(None),
        ResponseCode::NoUpdate(None),
        ResponseCode::NotificationOverflow(None),
        ResponseCode::BadEvent(None),
        ResponseCode::UndefinedFilter(None),
        ResponseCode::UidNotSticky,
        ResponseCode::NotSaved,
        ResponseCode::HasChildren,
        ResponseCode::UnknownCte,
        ResponseCode::TooBig,
        ResponseCode::CompressionActive,
        ResponseCode::UseAttr,
        ResponseCode::MetadataLongEntries(0),
        ResponseCode::MetadataMaxSize(0),
        ResponseCode::MetadataTooMany,
        ResponseCode::MetadataNoPrivate,
        ResponseCode::Other {
            name: "X-FOO".into(),
            value: Some("bar".into()),
        },
    ];
    for code in codes {
        let err = Error::no_with_code("t".into(), Some(code.clone()));
        // Thread an account-wide cursor scope so codes that classify
        // as `SyncState(CursorInvalid)` (e.g. NotificationOverflow,
        // ExpungeIssued, Closed) build cleanly. The test only cares
        // that the mapping does not panic and the wire enum survives
        // round-trip.
        let _account = into_account_error(
            err,
            ImapErrorContext::operation(AccountOperation::Discover)
                .with_cursor_scope(bifrost_types::CursorScope::Account),
        );
    }
}

#[test]
fn other_response_code_preserves_payload_in_wire_unknown() {
    let err = Error::no_with_code(
        "x".into(),
        Some(ResponseCode::Other {
            name: "X-CUSTOM".into(),
            value: Some("payload".into()),
        }),
    );
    let account = into_account_error(err, ImapErrorContext::operation(AccountOperation::Discover));
    let has_unknown = account.chain().iter().any(|c| {
        matches!(
            c,
            Cause::Wire(WireCause::Imap(ImapResponseCode::Unknown { code, value }))
                if code == "X-CUSTOM" && value.as_ref().map(|v| v.value.as_str()) == Some("payload")
        )
    });
    assert!(has_unknown);
}

#[test]
fn uidvalidity_changed_derives_restart_scope() {
    let account = super::uidvalidity_changed(&mailbox("INBOX"), 1, 2);
    assert!(matches!(
        account.recovery(),
        RecoveryClass::Engine(EngineDirective::RestartScope(CursorScope::Folder(_)))
    ));
    // imap-N2: SyncState(CursorInvalid) must carry a cursor scope or
    // builder rejects via `CursorInvalidWithoutScope`. This succeeds
    // structurally because `uidvalidity_changed` threads it.
    assert!(matches!(account.scope(), Some(ErrorScope::Cursor(_))));
}

#[test]
fn modseq_reset_derives_restart_scope() {
    let account = super::modseq_reset(&mailbox("INBOX"), 10, Some(5));
    assert!(matches!(
        account.recovery(),
        RecoveryClass::Engine(EngineDirective::RestartScope(CursorScope::Folder(_)))
    ));
}

#[test]
fn qresync_to_condstore_strategy_failure_derives_downgrade() {
    let account = strategy_failure(&mailbox("INBOX"), StrategyDowngrade::QResyncToCondstore);
    match account.recovery() {
        RecoveryClass::Engine(EngineDirective::DowngradeStrategy(
            StrategyDowngrade::QResyncToCondstore,
        )) => {}
        other => panic!("expected DowngradeStrategy(QResyncToCondstore), got {other:?}"),
    }
}

#[test]
fn condstore_to_basic_strategy_failure_derives_downgrade() {
    let account = strategy_failure(&mailbox("INBOX"), StrategyDowngrade::CondstoreToBasic);
    match account.recovery() {
        RecoveryClass::Engine(EngineDirective::DowngradeStrategy(
            StrategyDowngrade::CondstoreToBasic,
        )) => {}
        other => panic!("expected DowngradeStrategy(CondstoreToBasic), got {other:?}"),
    }
}

#[test]
fn tagged_no_carries_acknowledged_attempt_in_chain() {
    // imap-D1: tagged NO/BAD is server-acknowledged by definition.
    // Recovery rows that key on `Acknowledged` (e.g. tag-less
    // `ServerCause::Error { status: None }` -> `ProviderRefused`)
    // depend on the `Attempt` cause being present and set to
    // `Acknowledged` on the chain.
    let err = Error::no_with_code("rejected".into(), None);
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );
    let attempt = account.chain().iter().find_map(|c| match c {
        Cause::Attempt(a) => Some(a.transmission_state),
        _ => None,
    });
    assert_eq!(attempt, Some(TransmissionState::Acknowledged));
}

#[test]
fn tagged_bad_carries_acknowledged_attempt_in_chain() {
    let err = Error::bad_with_code("syntax".into(), None);
    let account = into_account_error(
        err,
        ImapErrorContext::operation(AccountOperation::SyncChanges),
    );
    let attempt = account.chain().iter().find_map(|c| match c {
        Cause::Attempt(a) => Some(a.transmission_state),
        _ => None,
    });
    assert_eq!(attempt, Some(TransmissionState::Acknowledged));
}

#[test]
fn ctx_with_cursor_scope_helper_round_trip() {
    let ctx = ImapErrorContext::operation(AccountOperation::SyncChanges)
        .with_cursor_scope(folder_cursor("INBOX"));
    let err = Error::no_with_code("nope".into(), Some(ResponseCode::ExpungeIssued));
    let account = into_account_error(err, ctx);
    assert!(matches!(
        account.kind(),
        AccountErrorKind::SyncState(SyncStateErrorKind::CursorInvalid)
    ));
}

#[test]
fn auth_attempt_evidence_survives_account_translation_and_override() {
    for state in [
        TransmissionState::Unsent,
        TransmissionState::InFlight,
        TransmissionState::Acknowledged,
    ] {
        let error = Error::auth_with_mechanism("SASL failure".into(), None, "PLAIN")
            .with_attempt(TransmissionState::InFlight)
            .with_attempt(state);
        assert_eq!(error.attempt(), Some(state));
        let account = into_account_error(
            error,
            ImapErrorContext::operation(AccountOperation::SyncChanges),
        );
        assert_eq!(
            account.chain().iter().find_map(|cause| match cause {
                Cause::Attempt(attempt) => Some(attempt.transmission_state),
                _ => None,
            }),
            Some(state)
        );
    }
}

#[test]
fn starttls_refusal_does_not_claim_the_capability_was_absent() {
    let error = into_account_error(
        Error::StartTlsUnavailable,
        ImapErrorContext::operation(AccountOperation::Discover),
    );
    assert_eq!(
        error.support_consented().support_text,
        ["required STARTTLS upgrade is unavailable in this session"]
    );
}
