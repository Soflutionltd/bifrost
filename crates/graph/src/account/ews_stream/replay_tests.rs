use super::*;
use bifrost_net::test_support::{Canned, ScriptedDispatch, canned};
fn fixture() -> (EwsClient, std::sync::Arc<ScriptedDispatch>) {
    let script = ScriptedDispatch::new([
        Canned::Error(bifrost_net::Error::Network {
            message: "lost".into(),
            transmission_state: bifrost_types::TransmissionState::InFlight,
            source: None,
        }),
        canned(reqwest::StatusCode::OK, b"<root/>"),
    ]);
    let mut policy = bifrost_net::RetryPolicy::default();
    policy.max_attempts = 2;
    policy.initial_backoff = Duration::ZERO;
    policy.max_backoff = Duration::ZERO;
    let net = bifrost_net::test_support::scripted_account(
        &script,
        bifrost_net::NetConfig::default(),
        vec![],
        std::sync::Arc::new(bifrost_net::StaticTokenSource::new("token", None)),
        policy,
    );
    let client = crate::client::GraphClient::new("graph-token").with_outlook_source(
        std::sync::Arc::new(bifrost_net::StaticTokenSource::new("token", None)),
    );
    (
        EwsClient::new(net, client.ews_url(), client.outlook_auth()),
        script,
    )
}
#[tokio::test]
async fn ews_subscribe_is_not_replayed_and_reads_are() {
    let (client, script) = fixture();
    assert!(subscribe(&client, &[]).await.is_err());
    assert_eq!(script.requests().len(), 1);
    assert_eq!(
        script.requests()[0]
            .headers
            .get(reqwest::header::AUTHORIZATION)
            .expect("Exchange bearer"),
        "Bearer token",
    );
    let (client, script) = fixture();
    // The minimal XML can be rejected by the parser; the two wire attempts
    // pin the policy before parsing the response.
    let _ = client.find_folder("inbox", &EwsHeaders::default()).await;
    assert_eq!(script.requests().len(), 2);
}
#[tokio::test]
async fn ews_streaming_request_is_not_replayed_by_net() {
    let (client, script) = fixture();
    assert!(
        client
            .execute_streaming("<GetStreamingEvents/>", &EwsHeaders::default())
            .await
            .is_err()
    );
    assert_eq!(script.requests().len(), 1);
    assert_eq!(
        script.requests()[0]
            .headers
            .get(reqwest::header::AUTHORIZATION)
            .expect("Exchange bearer"),
        "Bearer token",
    );
}
