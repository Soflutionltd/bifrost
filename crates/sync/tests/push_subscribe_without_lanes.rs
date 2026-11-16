//! Initial subscription applies the same effective-lane gate as reattach.
mod common;

use std::sync::Arc;

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{AccountId, AccountOperation, CursorScope, PushCapability};
use common::{StubAccount, StubFactory};

#[tokio::test(start_paused = true)]
async fn initial_subscribe_refuses_accounts_without_effective_push_lanes() {
    for capability in [
        PushCapability::None,
        PushCapability::InProcess,
        PushCapability::OutOfProcessPubsub,
        PushCapability::Webhook,
    ] {
        let mut account = StubAccount::new(vec![CursorScope::Account]);
        account.caps.push = capability;
        // None gates even declared lanes; other capabilities need a lane.
        if capability == PushCapability::None {
            account.push_lane_specs = StubAccount::registration_lanes();
        }
        let account = Arc::new(account);
        let engine = SyncEngine::builder().build().unwrap();
        let id = AccountId("initial-no-push-lanes".into());
        engine
            .attach(
                id.clone(),
                Arc::new(StubFactory::queue(vec![Arc::clone(&account)])),
            )
            .await
            .unwrap();

        let error = engine
            .subscribe_push(&id, &[CursorScope::Account])
            .await
            .expect_err("no effective lanes means push subscription is unavailable");
        let Error::Account(error) = error else {
            panic!("expected a classified account refusal");
        };
        assert_eq!(
            error.kind(),
            common::unsupported(AccountOperation::PushSubscribe).kind()
        );
        assert!(account.push_log.subscribed().is_empty());
        engine.unsubscribe_push(&id).await.unwrap();
        assert!(account.push_log.unsubscribed().is_empty());
        engine.shutdown().await.unwrap();
    }
}
