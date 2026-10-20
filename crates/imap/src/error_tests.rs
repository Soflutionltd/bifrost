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
    let err = Error::Protocol("missing CRLF".into());
    assert_eq!(err.to_string(), "protocol error: missing CRLF");
}

#[test]
fn display_parse() {
    let err = Error::Parse("unexpected NIL".into());
    assert_eq!(err.to_string(), "parse error: unexpected NIL");
}

#[test]
fn display_invalid_input() {
    let err = Error::InvalidInput("bad mailbox name".into());
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
        Error::StartTlsUnavailable.to_string(),
        "STARTTLS not supported by server",
    );
}

#[test]
fn display_missing_capability() {
    let err = Error::MissingCapability("COMPRESS=DEFLATE".into());
    assert_eq!(
        err.to_string(),
        "missing required capability: COMPRESS=DEFLATE",
    );
}

#[test]
fn display_append_limit() {
    let err = Error::AppendLimit {
        size: 10_000,
        limit: 5_000,
    };
    assert_eq!(
        err.to_string(),
        "message size 10000 exceeds server APPENDLIMIT of 5000",
    );
}

#[test]
fn display_invalid_append_date() {
    let err = Error::InvalidAppendDate("32-Jan-2025 00:00:00 +0000".into());
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
    let err = Error::InvalidInput("x".into()).with_attempt(TransmissionState::Acknowledged);
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
fn with_attempt_overrides_no_attempt() {
    // The driver and test helpers can re-stamp the attempt state if a
    // wire-level observation says otherwise.
    let err = Error::no_with_code("nope".into(), None).with_attempt(TransmissionState::InFlight);
    assert_eq!(err.attempt(), Some(TransmissionState::InFlight));
}

#[test]
fn from_io_error_via_into_yields_io_variant_without_attempt() {
    let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "connect");
    let err: Error = io_err.into();
    assert!(matches!(err, Error::Io { .. }));
    assert!(err.attempt().is_none());
}

#[test]
fn from_validation_error_maps_to_invalid_input() {
    // ValidationError is constructed by failed mailbox-name parsing.
    let validation =
        crate::types::MailboxName::new("INBOX\r").expect_err("control chars should fail");
    let err: Error = validation.into();
    assert!(matches!(err, Error::InvalidInput(_)));
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
    assert!(Error::Protocol("x".into()).response_code().is_none());
    assert!(Error::InvalidInput("x".into()).response_code().is_none());
}

// --- Equality ---

#[test]
fn equal_unit_constructed_variants() {
    assert_eq!(Error::timeout(), Error::timeout());
    assert_eq!(Error::closed(), Error::closed());
    assert_eq!(Error::driver_gone(), Error::driver_gone());
    assert_eq!(Error::StartTlsUnavailable, Error::StartTlsUnavailable);
}

#[test]
fn equal_string_variants() {
    assert_eq!(Error::Protocol("x".into()), Error::Protocol("x".into()));
    assert_eq!(Error::Parse("y".into()), Error::Parse("y".into()));
    assert_eq!(
        Error::InvalidInput("a".into()),
        Error::InvalidInput("a".into()),
    );
}

#[test]
fn equal_authpolicy() {
    assert_eq!(
        Error::AuthPolicy(auth_policy_failure()),
        Error::AuthPolicy(auth_policy_failure()),
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
    assert_ne!(Error::Protocol("x".into()), Error::Parse("x".into()));
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
        Error::Protocol("p".into()),
        Error::ProtocolMissing("pm".into()),
        Error::Parse("r".into()),
        Error::InvalidInput("i".into()),
        Error::InvalidState("s".into()),
        Error::StateChangedBeforeSend("m".into()),
        Error::UnsupportedOperation("o".into()),
        Error::timeout(),
        Error::closed(),
        Error::AuthPolicy(auth_policy_failure()),
        Error::StartTlsUnavailable,
        Error::MissingCapability("c".into()),
        Error::AppendLimit { size: 1, limit: 0 },
        Error::FetchLimit {
            estimated: 2,
            limit: 1,
            seq: Some(3),
            uid: Some(4),
        },
        Error::FetchLiteralLimit {
            estimated: 2,
            limit: 1,
        },
        Error::SearchResultTruncated {
            returned: 1,
            omitted: Some(2),
        },
        Error::InvalidAppendDate("bad date".into()),
        Error::Internal("internal err".into()),
        Error::internal_mid_exchange("mid", TransmissionState::InFlight),
        Error::LocalRuntime("rng".into()),
        Error::DriverPanicked {
            message: "p".into(),
            attempt: None,
        },
        Error::driver_gone(),
    ];

    for err in &errors {
        let label = match err {
            Error::Io { .. } => "io",
            Error::Auth { .. } => "auth",
            Error::No { .. } => "no",
            Error::Sieve { .. } => "sieve",
            Error::Bad { .. } => "bad",
            Error::Bye { .. } => "bye",
            Error::Protocol(_) => "protocol",
            Error::ProtocolMissing(_) => "protocolmissing",
            Error::Parse(_) => "parse",
            Error::InvalidInput(_) => "invalidinput",
            Error::InvalidState(_) => "invalidstate",
            Error::StateChangedBeforeSend(_) => "statechangedbeforesend",
            Error::UnsupportedOperation(_) => "unsupportedoperation",
            Error::Timeout { .. } => "timeout",
            Error::Closed { .. } => "closed",
            Error::AuthPolicy(_) => "authpolicy",
            Error::StartTlsUnavailable => "starttls",
            Error::MissingCapability(_) => "capability",
            Error::AppendLimit { .. } => "appendlimit",
            Error::FetchLimit { .. } => "fetchlimit",
            Error::FetchLiteralLimit { .. } => "fetchliterallimit",
            Error::SearchResultTruncated { .. } => "searchresulttruncated",
            Error::InvalidAppendDate(_) => "invalidappenddate",
            Error::Internal(_) => "internal",
            Error::InternalMidExchange { .. } => "internalmidexchange",
            Error::LocalRuntime(_) => "localruntime",
            Error::DriverPanicked { .. } => "driverpanicked",
            Error::DriverGone { .. } => "drivergone",
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
        Error::Protocol("proto".into()),
        Error::ProtocolMissing("missing".into()),
        Error::Parse("parse".into()),
        Error::InvalidInput("x".into()),
        Error::InvalidState("x".into()),
        Error::StateChangedBeforeSend("x".into()),
        Error::UnsupportedOperation("x".into()),
        Error::timeout(),
        Error::closed(),
        Error::StartTlsUnavailable,
        Error::MissingCapability("CAP".into()),
        Error::AppendLimit { size: 1, limit: 0 },
        Error::SearchResultTruncated {
            returned: 1,
            omitted: None,
        },
        Error::InvalidAppendDate("bad".into()),
        Error::Internal("test".into()),
        Error::internal_mid_exchange("mid", TransmissionState::Acknowledged),
        Error::LocalRuntime("rng".into()),
        Error::DriverPanicked {
            message: "panic".into(),
            attempt: None,
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
    assert!(!Error::ProtocolMissing("x".into()).is_connection_fatal());
    assert!(Error::Protocol("x".into()).is_connection_fatal());
}
