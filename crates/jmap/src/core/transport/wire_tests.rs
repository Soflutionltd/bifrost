use crate::Error;
use serde_json::json;

/// Every net error shape that can carry a JMAP problem document has to
/// hand it to the problem-details conversion. A terminal 4xx arrives as
/// `Status`; a 429 or 5xx is retried first and arrives as `RateLimited`
/// / `RetryBudgetExhausted` with the final response preserved on the
/// error, so the body has to be lifted back out of that evidence.
#[test]
fn problem_details_from_every_body_bearing_transport_error() {
    use crate::core::transport::TransportError;

    let problem = bytes::Bytes::from(
        serde_json::to_vec(&json!({
            "type": "urn:ietf:params:jmap:error:limit",
            "title": "Too many requests"
        }))
        .unwrap(),
    );
    let final_response = |status| bifrost_net::FinalResponse {
        status,
        headers: reqwest::header::HeaderMap::new(),
        body: problem.clone(),
    };

    let shapes = [
        bifrost_net::Error::Status {
            code: reqwest::StatusCode::BAD_REQUEST,
            body: problem.clone(),
            headers: reqwest::header::HeaderMap::new(),
        },
        bifrost_net::Error::RateLimited {
            retry_after: None,
            final_response: final_response(reqwest::StatusCode::TOO_MANY_REQUESTS),
        },
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response(reqwest::StatusCode::SERVICE_UNAVAILABLE)),
            retry_after_history: Vec::new(),
            stop_reason: bifrost_net::error::RetryStopReason::AttemptBudget,
        },
    ];

    for shape in shapes {
        let described = shape.to_string();
        let error: Error = TransportError::from_net(shape).into();
        assert!(
            matches!(error, Error::Problem { .. }),
            "{described} must reach the problem-details mapping"
        );
    }
}

/// `AuthLost` is the deliberate exception: its 401 classification and
/// transmission evidence belong to bifrost-net, so its body is not
/// lifted into a status-derived problem guess.
///
/// Runs in EVERY configuration, which took a one-line change in bifrost-net to
/// become true. This used to be gated on `sync`, not because the behaviour
/// varied - bifrost-net depends on bifrost-types unconditionally, so it is
/// identical in every build - but because `sync` was what pulled in the
/// optional dependency needed to SPELL `TransmissionState`. A test whose
/// coverage depends on which features a sibling happens to enable is a
/// feature-graph accident; bifrost-net now re-exports the type that its own
/// `Error::AuthLost` carries, so the spelling is always available.
#[test]
fn auth_lost_stays_a_transport_error() {
    use crate::core::transport::TransportError;

    let error: Error = TransportError::from_net(bifrost_net::Error::AuthLost {
        transmission_state: Some(bifrost_net::TransmissionState::Acknowledged),
        final_response: Some(bifrost_net::FinalResponse {
            status: reqwest::StatusCode::UNAUTHORIZED,
            headers: reqwest::header::HeaderMap::new(),
            body: bytes::Bytes::from_static(br#"{"type":"urn:ietf:params:jmap:error:limit"}"#),
        }),
    })
    .into();

    assert!(matches!(error, Error::Transport(_)));
}

#[test]
fn transport_error_without_body_stays_transport() {
    use crate::core::transport::TransportError;

    let err = TransportError::new("connection refused");
    let error: Error = err.into();
    assert!(matches!(error, Error::Transport(_)));
}
