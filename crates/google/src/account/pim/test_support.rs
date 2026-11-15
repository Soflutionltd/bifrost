use super::super::scopes::{ScopeCache, ScopeCacheState, ScopeSnapshot};
use crate::client::GmailClient;
use crate::types::{GmailLabel, GmailMessage};
use bifrost_net::test_support::{Canned, ScriptedDispatch};
use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
use bytes::Bytes;
use reqwest::StatusCode;
use std::sync::Arc;
use std::time::Instant;

pub(super) fn cache_with_label() -> ScopeCache {
    Arc::new(ScopeCacheState::new(ScopeSnapshot {
        labels: vec![GmailLabel {
            id: "Label_1".to_string(),
            name: "Work".to_string(),
            label_type: Some(crate::types::GmailLabelType::User),
            color: None,
        }],
        fetched_at: Some(Instant::now()),
    }))
}

pub(super) fn scripted_client(steps: Vec<Canned>) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
    let script = ScriptedDispatch::new(steps);
    let net = bifrost_net::test_support::scripted_account(
        &script,
        NetConfig::default(),
        Vec::new(),
        Arc::new(StaticTokenSource::new("token", None)),
        RetryPolicy::disabled(),
    );
    (
        Arc::new(GmailClient::with_account_net("https://gmail.test", net)),
        script,
    )
}

pub(super) fn ok_json(body: &'static [u8]) -> Canned {
    Canned::Response {
        status: StatusCode::OK,
        headers: reqwest::header::HeaderMap::new(),
        body: Bytes::from_static(body),
    }
}

pub(super) fn gmail_message(value: serde_json::Value) -> GmailMessage {
    serde_json::from_value(value).expect("message fixture deserializes")
}
