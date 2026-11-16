//! Builders shared by more than one half of the push suite.
//!
//! `state` / `expiring` are read by the webhook teardown tests and by every
//! renewal test; `email_scope` / `pending` / `ledger` / `converted` are read by
//! the EWS translation tests and by the dispatcher tests that drive a request
//! through the EWS arm. Keeping them in one module is what lets the split
//! avoid duplicating a fixture whose shape is itself an assertion.

use std::collections::HashMap;

use bifrost_types::{CursorScope, FolderId, ObjectType};

use crate::account::push::common::PushEndpoint;
use crate::account::push::ews::{
    PendingEwsScope, TranslatedExchangeId, ews_subscribable_folder_id,
};
use crate::account::push::webhook::GraphSubscriptionState;
use crate::account::{GraphAccount, PushMode};
use crate::client::{GraphClient, ScriptedRestResponse};

pub(super) fn state(server_id: &str, resource: &str) -> GraphSubscriptionState {
    GraphSubscriptionState {
        server_id: server_id.to_string(),
        expires_at: "2099-01-01T00:00:00Z".to_string(),
        resource: resource.to_string(),
        scopes: vec![CursorScope::FolderType {
            folder: FolderId(resource.to_string()),
            ty: ObjectType::Email,
        }],
        warned_unparseable_expiry: false,
        terminated: false,
    }
}

/// A subscription whose expiry is already past, so it is unconditionally
/// inside the renewal threshold.
pub(super) fn expiring(server_id: &str, resource: &str) -> GraphSubscriptionState {
    GraphSubscriptionState {
        server_id: server_id.to_string(),
        expires_at: "2000-01-01T00:00:00Z".to_string(),
        resource: resource.to_string(),
        scopes: vec![CursorScope::FolderType {
            folder: FolderId(resource.to_string()),
            ty: ObjectType::Email,
        }],
        warned_unparseable_expiry: false,
        terminated: false,
    }
}

pub(super) fn email_scope(folder: &str) -> CursorScope {
    CursorScope::FolderType {
        folder: FolderId(folder.to_string()),
        ty: ObjectType::Email,
    }
}

/// What `subscribe_ews` hands the translation phase: every scope already
/// validated and paired with the native id it contributes.
pub(super) fn pending(folders: &[&str]) -> Vec<PendingEwsScope> {
    folders
        .iter()
        .enumerate()
        .map(|(index, folder)| {
            let scope = email_scope(folder);
            let source_id =
                ews_subscribable_folder_id(&scope).expect("a primary folder scope is pending");
            (bifrost_types::SubmissionIndex(index), scope, source_id)
        })
        .collect()
}

/// A fresh per-request ledger for a call made below `push_subscribe`
/// (`reconcile_translated_ews_scopes`, or `subscribe_eligible` driven with a
/// lane list of the test's choosing).
pub(super) fn ledger()
-> bifrost_types::BatchOutcomeBuilder<CursorScope, bifrost_types::SubmissionIndex> {
    bifrost_types::BatchOutcomeBuilder::new()
}

/// No translation chunk's POST failed, for the reconciliation tests that
/// script an answered request.
pub(super) fn no_chunk_failures() -> HashMap<String, bifrost_types::AccountError> {
    HashMap::new()
}

pub(super) fn converted(source_id: &str, target_id: &str) -> TranslatedExchangeId {
    TranslatedExchangeId {
        source_id: source_id.to_string(),
        target_id: Some(target_id.to_string()),
        error_details: None,
    }
}

// ---- webhook accounts on a scripted wire ---------------------------------
//
// The scripted wire is one ordered script shared by `script_rest` and
// `script_aux_pending`, so a request can be parked forever (`Pending`)
// between answered ones. Tests that use these run on the current-thread test
// runtime: spawned work only advances when the test yields, which makes "drop
// the future NOW" deterministic.

pub(super) fn webhook_account(client: &GraphClient) -> GraphAccount {
    let mut account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
    account.push_config = crate::account::PushConfig::Webhook(PushEndpoint {
        webhook_url: "https://example.test/hook".to_string(),
        client_state: "secret".to_string(),
    });
    account
}

pub(super) fn created(id: &str) -> ScriptedRestResponse {
    ScriptedRestResponse::json(
        reqwest::StatusCode::CREATED,
        serde_json::json!({"id": id, "expirationDateTime": "2099-01-01T00:00:00Z"}),
    )
}

pub(super) fn deleted() -> ScriptedRestResponse {
    ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT)
}

/// Yield to the spawned work until `done` holds, bounded so a regression
/// fails the assertion instead of hanging. Yields, never sleeps.
pub(super) async fn settle(mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..2000 {
        if done() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    done()
}

pub(super) fn groups_empty(account: &GraphAccount) -> bool {
    account
        .webhooks
        .groups_for_tests()
        .try_read()
        .is_ok_and(|groups| groups.is_empty())
}
