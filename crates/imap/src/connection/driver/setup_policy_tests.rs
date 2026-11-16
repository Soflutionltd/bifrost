#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::types::Capability;
use crate::types::response::{GreetingResponse, GreetingStatus, ResponseCode};

#[test]
fn preauth_with_starttls_capability_is_a_tls_policy_refusal() {
    let mut state = state::ProtocolState::new();
    state
        .apply_greeting(&GreetingResponse {
            status: GreetingStatus::PreAuth,
            code: Some(ResponseCode::Capability(vec![Capability::StartTls])),
            text: "already authenticated".into(),
        })
        .unwrap();

    assert!(matches!(
        ensure_starttls_available(&state),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::InvalidState(_)
        ))
    ));
}

#[test]
fn preauth_without_greeting_capabilities_is_rejected_before_capability_fallback() {
    let mut state = state::ProtocolState::new();
    state
        .apply_greeting(&GreetingResponse {
            status: GreetingStatus::PreAuth,
            code: None,
            text: "already authenticated".into(),
        })
        .unwrap();

    assert_eq!(
        ensure_starttls_session(&state),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::StartTlsUnavailable
        ))
    );
}

#[test]
fn unauthenticated_session_requires_starttls_advertisement() {
    let mut state = state::ProtocolState::new();
    state
        .apply_greeting(&GreetingResponse {
            status: GreetingStatus::Ok,
            code: Some(ResponseCode::Capability(vec![Capability::StartTls])),
            text: "ready".into(),
        })
        .unwrap();

    assert_eq!(ensure_starttls_available(&state), Ok(()));
    state.apply_capability_fetch(Vec::new());
    assert_eq!(
        ensure_starttls_available(&state),
        Err(Error::LocalRefusal(
            crate::error::LocalRefusal::StartTlsUnavailable
        ))
    );
}
