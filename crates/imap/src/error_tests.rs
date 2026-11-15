//! Tests for the internal crate `Error` enum.
//!
//! These tests cover construction, `Display`, `Debug`, equality, and
//! the new attempt-state evidence helpers. The end-to-end classification
//! tests (Error -> `AccountError`) live in
//! `crates/imap/src/account/error_tests.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use bifrost_types::TransmissionState;

use super::*;

fn auth_policy_failure() -> AuthPolicyFailure {
    AuthPolicyFailure::new(
        vec!["AUTH=PLAIN".to_owned()],
        vec![AuthMechanismRejection::new(
            crate::types::AuthMechanism::Plain,
            AuthMechanismRejectionReason::CleartextWithoutTls,
        )],
    )
}

#[test]
fn channel_binding_unavailable_display() {
    assert_eq!(
        AuthMechanismRejectionReason::ChannelBindingUnavailable.to_string(),
        "channel binding unavailable"
    );

    // The reason renders with its mechanism inside an AuthPolicyFailure.
    let failure = AuthPolicyFailure::new(
        vec!["AUTH=SCRAM-SHA-256-PLUS".to_owned()],
        vec![AuthMechanismRejection::new(
            crate::types::AuthMechanism::ScramSha256Plus,
            AuthMechanismRejectionReason::ChannelBindingUnavailable,
        )],
    );
    let rendered = failure.to_string();
    assert!(
        rendered.contains("SCRAM-SHA-256-PLUS (channel binding unavailable)"),
        "got: {rendered}"
    );
}

// --- Display formatting smoke tests ---

#[test]
fn display_io() {
    let err = Error::io(std::io::Error::other("pipe broken"));
    let msg = err.to_string();
    assert!(msg.contains("I/O error"));
    assert!(msg.contains("pipe broken"));
}

#[test]
fn display_auth() {
    let err = Error::auth_with_code("LOGIN denied".into(), None);
    assert_eq!(err.to_string(), "authentication failed: LOGIN denied");
}

#[test]
fn display_no() {
    let err = Error::no_with_code("[NOPERM] SELECT not allowed".into(), None);
    assert_eq!(
        err.to_string(),
        "server rejected command: [NOPERM] SELECT not allowed"
    );
}

#[test]
fn display_bad() {
    let err = Error::bad_with_code("syntax error in FETCH".into(), None);
    assert_eq!(
        err.to_string(),
        "server reported bad command: syntax error in FETCH"
    );
}

#[test]
fn display_bye() {
    let err = Error::bye_with_code("Too many connections".into(), None);
    assert_eq!(
        err.to_string(),
        "server closing connection: Too many connections"
    );
}

#[test]
fn display_protocol() {
    let err = Error::Provider(crate::error::ProviderFailure::Protocol(
        "missing CRLF".into(),
    ));
    assert_eq!(err.to_string(), "protocol error: missing CRLF");
}

#[test]
fn display_parse() {
    let err = Error::Provider(crate::error::ProviderFailure::Parse(
        "unexpected NIL".into(),
    ));
    assert_eq!(err.to_string(), "parse error: unexpected NIL");
}

#[test]
fn display_invalid_input() {
    let err = Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(
        "bad mailbox name".into(),
    ));
    assert_eq!(err.to_string(), "invalid input: bad mailbox name");
}

#[test]
fn display_timeout() {
    assert_eq!(Error::timeout().to_string(), "operation timed out");
}

#[test]
fn display_closed() {
    assert_eq!(Error::closed().to_string(), "connection closed");
}

#[test]
fn display_starttls_unavailable() {
    assert_eq!(
        Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable).to_string(),
        "STARTTLS unavailable in current session",
    );
}

#[test]
fn starttls_unavailable_is_classified_as_a_policy_block() {
    use bifrost_types::{AccessErrorKind, AccountErrorKind, AccountOperation};

    let account_error = crate::account::error::into_account_error(
        Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable),
        crate::account::error::ImapErrorContext::operation(AccountOperation::Discover),
    );
    assert!(matches!(
        account_error.kind(),
        AccountErrorKind::Authorization(AccessErrorKind::PolicyBlocked)
    ));
}

#[test]
fn display_missing_capability() {
    let err = Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(
        "COMPRESS=DEFLATE".into(),
    ));
    assert_eq!(
        err.to_string(),
        "missing required capability: COMPRESS=DEFLATE",
    );
}

#[test]
fn display_append_limit() {
    let err = Error::LocalRefusal(crate::error::LocalRefusal::AppendLimit {
        size: 10_000,
        limit: 5_000,
    });
    assert_eq!(
        err.to_string(),
        "message size 10000 exceeds server APPENDLIMIT of 5000",
    );
}

#[test]
fn display_invalid_append_date() {
    let err = Error::LocalRefusal(crate::error::LocalRefusal::InvalidAppendDate(
        "32-Jan-2025 00:00:00 +0000".into(),
    ));
    assert_eq!(
        err.to_string(),
        "invalid APPEND date-time: 32-Jan-2025 00:00:00 +0000",
    );
}

#[test]
fn display_driver_gone() {
    assert_eq!(Error::driver_gone().to_string(), "driver task gone");
}

// --- Constructors + attempt evidence ---

#[test]
fn io_constructor_starts_with_no_attempt() {
    let err = Error::io(std::io::Error::other("eof"));
    assert!(err.attempt().is_none());
}

#[test]
fn with_attempt_sets_state_on_io() {
    let err = Error::io(std::io::Error::other("x")).with_attempt(TransmissionState::InFlight);
    assert_eq!(err.attempt(), Some(TransmissionState::InFlight));
}

#[test]
fn with_attempt_sets_state_on_timeout() {
    let err = Error::timeout().with_attempt(TransmissionState::Unsent);
    assert_eq!(err.attempt(), Some(TransmissionState::Unsent));
}

#[test]
fn with_attempt_sets_state_on_closed() {
    let err = Error::closed().with_attempt(TransmissionState::InFlight);
    assert_eq!(err.attempt(), Some(TransmissionState::InFlight));
}

#[test]
fn with_attempt_sets_state_on_driver_gone() {
    let err = Error::driver_gone().with_attempt(TransmissionState::InFlight);
    assert_eq!(err.attempt(), Some(TransmissionState::InFlight));
}

#[test]
fn with_attempt_sets_state_on_bye() {
    let err = Error::bye_with_code("bye".into(), None).with_attempt(TransmissionState::InFlight);
    assert_eq!(err.attempt(), Some(TransmissionState::InFlight));
}

#[test]
fn with_attempt_noop_on_pure_local_errors() {
    let err = Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput("x".into()))
        .with_attempt(TransmissionState::Acknowledged);
    assert!(err.attempt().is_none());
}

#[test]
fn no_with_code_defaults_to_acknowledged_attempt() {
    // A tagged NO is server-acknowledged by definition. Without this,
    // the recovery row `Server(Error { status: None }) + Acknowledged
    // -> ProviderRefused` collapses to the Unsent arm.
    let err = Error::no_with_code("nope".into(), None);
    assert_eq!(err.attempt(), Some(TransmissionState::Acknowledged));
}

#[test]
fn bad_with_code_defaults_to_acknowledged_attempt() {
    let err = Error::bad_with_code("syntax".into(), None);
    assert_eq!(err.attempt(), Some(TransmissionState::Acknowledged));
}

#[test]
fn with_attempt_preserves_server_acknowledgement() {
    // A tagged refusal remains acknowledged even inside a loop with an
    // earlier exchange phase.
    for err in [
        Error::no_with_code("nope".into(), None),
        Error::bad_with_code("syntax".into(), None),
        Error::auth_with_code("denied".into(), None),
    ] {
        assert_eq!(
            err.with_attempt(TransmissionState::InFlight).attempt(),
            Some(TransmissionState::Acknowledged)
        );
    }
}

#[test]
fn from_io_error_via_into_yields_io_variant_without_attempt() {
    let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "connect");
    let err: Error = io_err.into();
    assert!(matches!(
        err,
        Error::Transport {
            kind: crate::error::TransportFailure::Io(crate::error::IoFailure(_)),
            ..
        }
    ));
    assert!(err.attempt().is_none());
}

#[test]
fn from_validation_error_maps_to_invalid_input() {
    // ValidationError is constructed by failed mailbox-name parsing.
    let validation =
        crate::types::MailboxName::new("INBOX\r").expect_err("control chars should fail");
    let err: Error = validation.into();
    assert!(matches!(
        err,
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(_))
    ));
}

// --- Response code preservation on status variants ---

#[test]
fn no_preserves_response_code() {
    let err = Error::no_with_code("over".into(), Some(ResponseCode::OverQuota));
    assert_eq!(err.response_code(), Some(&ResponseCode::OverQuota));
}

#[test]
fn bad_preserves_response_code() {
    let err = Error::bad_with_code("bug".into(), Some(ResponseCode::ServerBug));
    assert_eq!(err.response_code(), Some(&ResponseCode::ServerBug));
}

#[test]
fn auth_preserves_response_code() {
    let err = Error::auth_with_code("e".into(), Some(ResponseCode::Expired));
    assert_eq!(err.response_code(), Some(&ResponseCode::Expired));
}

#[test]
fn bye_preserves_response_code() {
    let err = Error::bye_with_code("u".into(), Some(ResponseCode::Unavailable));
    assert_eq!(err.response_code(), Some(&ResponseCode::Unavailable));
}

#[test]
fn non_status_variants_have_no_response_code() {
    assert!(Error::timeout().response_code().is_none());
    assert!(
        Error::Provider(crate::error::ProviderFailure::Protocol("x".into()))
            .response_code()
            .is_none()
    );
    assert!(
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput("x".into()))
            .response_code()
            .is_none()
    );
}

// --- Equality ---

#[test]
fn equal_unit_constructed_variants() {
    assert_eq!(Error::timeout(), Error::timeout());
    assert_eq!(Error::closed(), Error::closed());
    assert_eq!(Error::driver_gone(), Error::driver_gone());
    assert_eq!(
        Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable),
        Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable)
    );
}

#[test]
fn equal_string_variants() {
    assert_eq!(
        Error::Provider(crate::error::ProviderFailure::Protocol("x".into())),
        Error::Provider(crate::error::ProviderFailure::Protocol("x".into()))
    );
    assert_eq!(
        Error::Provider(crate::error::ProviderFailure::Parse("y".into())),
        Error::Provider(crate::error::ProviderFailure::Parse("y".into()))
    );
    assert_eq!(
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput("a".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput("a".into())),
    );
}

#[test]
fn equal_authpolicy() {
    assert_eq!(
        Error::LocalRefusal(crate::error::LocalRefusal::AuthPolicy(auth_policy_failure())),
        Error::LocalRefusal(crate::error::LocalRefusal::AuthPolicy(auth_policy_failure())),
    );
}

#[test]
fn io_equal_by_kind_ignoring_message() {
    let a = Error::io(std::io::Error::new(std::io::ErrorKind::NotFound, "A"));
    let b = Error::io(std::io::Error::new(std::io::ErrorKind::NotFound, "B"));
    assert_eq!(a, b);
}

#[test]
fn io_not_equal_when_attempt_differs() {
    let a = Error::io(std::io::Error::new(std::io::ErrorKind::NotFound, "x"));
    let b = Error::io(std::io::Error::new(std::io::ErrorKind::NotFound, "x"))
        .with_attempt(TransmissionState::InFlight);
    assert_ne!(a, b);
}

#[test]
fn different_variants_are_not_equal() {
    assert_ne!(Error::timeout(), Error::closed());
    assert_ne!(
        Error::Provider(crate::error::ProviderFailure::Protocol("x".into())),
        Error::Provider(crate::error::ProviderFailure::Parse("x".into()))
    );
}

// --- Variant discrimination ---

#[test]
fn all_variants_are_distinguishable() {
    let errors: Vec<Error> = vec![
        Error::io(std::io::Error::other("test")),
        Error::auth_with_code("a".into(), None),
        Error::no_with_code("n".into(), None),
        Error::bad_with_code("b".into(), None),
        Error::bye_with_code("y".into(), None),
        Error::Provider(crate::error::ProviderFailure::Protocol("p".into())),
        Error::Provider(crate::error::ProviderFailure::Missing("pm".into())),
        Error::Provider(crate::error::ProviderFailure::Parse("r".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput("i".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidState("s".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::StateChangedBeforeSend(
            "m".into(),
        )),
        Error::LocalRefusal(crate::error::LocalRefusal::UnsupportedOperation("o".into())),
        Error::timeout(),
        Error::closed(),
        Error::LocalRefusal(crate::error::LocalRefusal::AuthPolicy(auth_policy_failure())),
        Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable),
        Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability("c".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::AppendLimit { size: 1, limit: 0 }),
        Error::ResultLimit(crate::error::ResultLimit::Fetch {
            estimated: 2,
            limit: 1,
            seq: Some(crate::types::Seq::new(3).expect("valid protocol test value")),
            uid: Some(crate::types::Uid::new(4).expect("valid protocol test value")),
        }),
        Error::ResultLimit(crate::error::ResultLimit::FetchLiteral {
            estimated: 2,
            limit: 1,
        }),
        Error::ResultLimit(crate::error::ResultLimit::SearchTruncated {
            returned: 1,
            omitted: Some(2),
        }),
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidAppendDate(
            "bad date".into(),
        )),
        Error::Internal {
            kind: crate::error::InternalFailure::Invariant("internal err".into()),
            mid_exchange: None,
        },
        Error::internal_mid_exchange("mid", TransmissionState::InFlight),
        Error::Internal {
            kind: crate::error::InternalFailure::Runtime("rng".into()),
            mid_exchange: None,
        },
        Error::Internal {
            kind: crate::error::InternalFailure::Panicked("p".into()),
            mid_exchange: None,
        },
        Error::driver_gone(),
    ];

    for err in &errors {
        let label = match err {
            Error::OAuthToken(_) => "oauthtoken",
            Error::OperationNotSent(_) => "operationnotsent",
            Error::MoveFallbackIncomplete { .. } => "movefallback",
            Error::Transport {
                kind: crate::error::TransportFailure::Io(crate::error::IoFailure(_)),
                ..
            } => "io",
            Error::ServerStatus(crate::error::ServerStatus::Auth { .. }) => "auth",
            Error::ServerStatus(crate::error::ServerStatus::No { .. }) => "no",
            Error::ServerStatus(crate::error::ServerStatus::Bad { .. }) => "bad",
            Error::Bye { .. } => "bye",
            Error::Provider(crate::error::ProviderFailure::Protocol(_)) => "protocol",
            Error::Provider(crate::error::ProviderFailure::Missing(_)) => "protocolmissing",
            Error::Provider(crate::error::ProviderFailure::Parse(_)) => "parse",
            Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(_)) => "invalidinput",
            Error::LocalRefusal(crate::error::LocalRefusal::InvalidState(_)) => "invalidstate",
            Error::LocalRefusal(crate::error::LocalRefusal::StateChangedBeforeSend(_)) => {
                "statechangedbeforesend"
            }
            Error::LocalRefusal(crate::error::LocalRefusal::UnsupportedOperation(_)) => {
                "unsupportedoperation"
            }
            Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                ..
            } => "timeout",
            Error::Transport {
                kind: crate::error::TransportFailure::Closed,
                ..
            } => "closed",
            Error::LocalRefusal(crate::error::LocalRefusal::AuthPolicy(_)) => "authpolicy",
            Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable) => "starttls",
            Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(_)) => "capability",
            Error::LocalRefusal(crate::error::LocalRefusal::AppendLimit { .. }) => "appendlimit",
            Error::ResultLimit(crate::error::ResultLimit::Fetch { .. }) => "fetchlimit",
            Error::ResultLimit(crate::error::ResultLimit::FetchLiteral { .. }) => {
                "fetchliterallimit"
            }
            Error::ResultLimit(crate::error::ResultLimit::SearchTruncated { .. }) => {
                "searchresulttruncated"
            }
            Error::LocalRefusal(crate::error::LocalRefusal::InvalidAppendDate(_)) => {
                "invalidappenddate"
            }
            Error::Internal {
                kind: crate::error::InternalFailure::Invariant(_),
                mid_exchange: None,
            } => "internal",
            Error::Internal {
                kind: crate::error::InternalFailure::Invariant(_),
                ..
            } => "internalmidexchange",
            Error::Internal {
                kind: crate::error::InternalFailure::Runtime(_),
                ..
            } => "localruntime",
            Error::Internal {
                kind: crate::error::InternalFailure::Panicked(_),
                ..
            } => "driverpanicked",
            Error::Transport {
                kind: crate::error::TransportFailure::DriverGone,
                ..
            } => "drivergone",
        };
        assert!(!label.is_empty());
    }
}

// --- std::error::Error source ---

#[test]
fn io_variant_has_source() {
    use std::error::Error as StdError;
    let err = Error::io(std::io::Error::other("disk full"));
    assert!(err.source().is_some());
}

#[test]
fn other_variants_have_no_source() {
    use std::error::Error as StdError;
    let variants: Vec<Error> = vec![
        Error::auth_with_code("fail".into(), None),
        Error::no_with_code("no".into(), None),
        Error::bad_with_code("bad".into(), None),
        Error::bye_with_code("bye".into(), None),
        Error::Provider(crate::error::ProviderFailure::Protocol("proto".into())),
        Error::Provider(crate::error::ProviderFailure::Missing("missing".into())),
        Error::Provider(crate::error::ProviderFailure::Parse("parse".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput("x".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidState("x".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::StateChangedBeforeSend(
            "x".into(),
        )),
        Error::LocalRefusal(crate::error::LocalRefusal::UnsupportedOperation("x".into())),
        Error::timeout(),
        Error::closed(),
        Error::LocalRefusal(crate::error::LocalRefusal::StartTlsUnavailable),
        Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability("CAP".into())),
        Error::LocalRefusal(crate::error::LocalRefusal::AppendLimit { size: 1, limit: 0 }),
        Error::ResultLimit(crate::error::ResultLimit::SearchTruncated {
            returned: 1,
            omitted: None,
        }),
        Error::LocalRefusal(crate::error::LocalRefusal::InvalidAppendDate("bad".into())),
        Error::Internal {
            kind: crate::error::InternalFailure::Invariant("test".into()),
            mid_exchange: None,
        },
        Error::internal_mid_exchange("mid", TransmissionState::Acknowledged),
        Error::Internal {
            kind: crate::error::InternalFailure::Runtime("rng".into()),
            mid_exchange: None,
        },
        Error::Internal {
            kind: crate::error::InternalFailure::Panicked("panic".into()),
            mid_exchange: None,
        },
        Error::driver_gone(),
    ];
    for variant in &variants {
        assert!(variant.source().is_none(), "no source for: {variant:?}");
    }
    // Avoid unused import warning when constructed errors live only in
    // the vec.
    let _ = Arc::new(0_u8);
}

/// An omission is raised only after the exchange completed, so the framing
/// is intact: `ProtocolMissing` must not retire the connection, while a
/// server that sent something WRONG (`Protocol`) still does.
#[test]
fn a_missing_mandatory_response_is_not_connection_fatal() {
    assert!(
        !Error::Provider(crate::error::ProviderFailure::Missing("x".into())).is_connection_fatal()
    );
    assert!(
        Error::Provider(crate::error::ProviderFailure::Protocol("x".into())).is_connection_fatal()
    );
}

#[test]
fn login_rejections_distinguish_sasl_from_command() {
    let failure = AuthPolicyFailure::new(
        vec!["AUTH=LOGIN".into(), "LOGIN".into()],
        [
            crate::types::AuthMechanism::Login,
            crate::types::AuthMechanism::ImapLogin,
        ]
        .into_iter()
        .map(|mechanism| {
            AuthMechanismRejection::new(mechanism, AuthMechanismRejectionReason::DisabledByPolicy)
        })
        .collect(),
    );
    let rendered = failure.to_string();
    assert!(rendered.contains("AUTH=LOGIN ("));
    assert!(rendered.contains(", LOGIN ("));
}

#[test]
fn prerequisite_refusal_retains_wire_acknowledgement_but_not_operation_transmission() {
    let error =
        Error::no_with_code("busy".into(), Some(ResponseCode::InUse)).for_unsent_operation();
    assert_eq!(error.attempt(), Some(TransmissionState::Unsent));
    assert_eq!(error.response_code(), Some(&ResponseCode::InUse));
    assert!(!error.is_connection_fatal());
    let Error::OperationNotSent(source) = error else {
        panic!("expected prerequisite evidence")
    };
    assert_eq!(source.attempt(), Some(TransmissionState::Acknowledged));
}
