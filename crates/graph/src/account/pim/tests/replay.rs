use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, Resource};
use bifrost_net::test_support::{Canned, canned};
use bifrost_types::{AccountOperation, ContainerId};

fn dropped() -> Canned {
    Canned::Error(bifrost_net::Error::Network {
        message: "lost".into(),
        transmission_state: bifrost_types::TransmissionState::InFlight,
        source: None,
    })
}
fn policy() -> bifrost_net::RetryPolicy {
    let mut policy = bifrost_net::RetryPolicy::default();
    policy.max_attempts = 2;
    policy.initial_backoff = std::time::Duration::ZERO;
    policy.max_backoff = std::time::Duration::ZERO;
    policy
}
#[tokio::test]
async fn graph_unconditional_absolute_patch_replays_after_a_drop() {
    let client = GraphClient::new("token");
    client.script_wire(
        [dropped(), canned(reqwest::StatusCode::NO_CONTENT, b"")],
        policy(),
    );
    let account =
        GraphAccount::new_for_tests_with_shared(client.clone(), PushMode::GraphSubscriptions, &[]);
    crate::account::pim::containers::container_rename(
        account,
        ContainerId("f1".into()),
        "new".into(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(client.rest_requests().len(), 2);
}
#[tokio::test]
async fn graph_unabsorbed_delete_is_not_replayed() {
    let client = GraphClient::new("token");
    client.script_wire(
        [dropped(), canned(reqwest::StatusCode::NO_CONTENT, b"")],
        policy(),
    );
    let account =
        GraphAccount::new_for_tests_with_shared(client.clone(), PushMode::GraphSubscriptions, &[]);
    let error =
        crate::account::pim::containers::container_delete(account, ContainerId("f1".into()))
            .await
            .unwrap_err();
    assert!(error.recovery().requires_reconciliation());
    assert_eq!(client.rest_requests().len(), 1);
}
#[tokio::test]
async fn graph_read_batch_replays_and_mutation_batch_does_not() {
    for replay in [
        bifrost_net::ReplayPolicy::Idempotent,
        bifrost_net::ReplayPolicy::Never,
    ] {
        let client = GraphClient::new("token");
        client.script_wire(
            [
                dropped(),
                canned(reqwest::StatusCode::OK, br#"{"responses":[]}"#),
            ],
            policy(),
        );
        let result = client
            .post_batch(
                Resource::Outlook,
                &crate::types::BatchRequest { requests: vec![] },
                replay,
            )
            .await;
        assert_eq!(
            result.is_ok(),
            replay == bifrost_net::ReplayPolicy::Idempotent
        );
        assert_eq!(
            client.rest_requests().len(),
            if replay == bifrost_net::ReplayPolicy::Idempotent {
                2
            } else {
                1
            }
        );
    }
}
async fn event_patch(conditional: bool) {
    let client = GraphClient::new("token");
    client.script_wire(
        [
            canned(reqwest::StatusCode::OK, br#"{"id":"e1","subject":"old"}"#),
            dropped(),
            canned(reqwest::StatusCode::NO_CONTENT, b""),
        ],
        policy(),
    );
    let account =
        GraphAccount::new_for_tests_with_shared(client.clone(), PushMode::GraphSubscriptions, &[]);
    let patch = bifrost_types::EventPatch {
        title: Some(Some("new".into())),
        expected_version: conditional.then(|| bifrost_types::ServerVersion::ETag("\"v1\"".into())),
        ..Default::default()
    };
    let error = crate::account::calendar::update(
        account,
        bifrost_types::EventId("calendar::e1".into()),
        patch,
    )
    .await
    .unwrap_err();
    assert_eq!(error.operation(), Some(AccountOperation::EventUpdate));
    assert!(
        matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget]),
        "{error:?}"
    );
    assert_eq!(
        client
            .rest_requests()
            .iter()
            .filter(|r| r.method == reqwest::Method::PATCH)
            .count(),
        1
    );
}
#[tokio::test]
async fn graph_if_match_patch_reconciles_after_a_drop() {
    event_patch(true).await;
}
#[tokio::test]
async fn graph_unconditional_event_patch_is_not_replayed() {
    event_patch(false).await;
}
