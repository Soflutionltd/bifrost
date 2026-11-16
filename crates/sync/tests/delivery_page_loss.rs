//! Delivery page loss contract.

mod common;
#[path = "common/recovery.rs"]
mod recovery;

use bifrost_sync::SyncEngine;
use bifrost_types::{AccountFactory, AccountId, CursorScope, SyncEvent};
use recovery::*;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};

/// Both page loss lanes ride out in the returned `Page`, so this
/// warning reveals no new DATA - it closes an asymmetry. An open-time
/// skip is announced (`open_skipped_scopes` plus a log line) while a
/// page-time skip was entirely silent, so a consumer had to already know
/// to look at the lanes. The warning gives them the reason to look; the
/// `Page` in hand stays the actionable copy.
#[tokio::test]
async fn a_forwarded_page_with_a_loss_lane_warns_on_the_change_stream() {
    let account_id = AccountId("page-loss".to_owned());
    let scope = CursorScope::Account;
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::with_degraded_contacts(
        vec![scope],
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
    ));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let page = engine
        .contacts_list(&account_id, None, None)
        .await
        .expect("a degraded page is still a successful call");

    // The lanes reach the caller untouched - the warning is additive.
    assert_eq!(page.failed_ids.len(), 1);
    assert_eq!(page.skipped_scopes.len(), 1);

    let warning = loop {
        let event = events.recv().await.expect("the warning is broadcast");
        if let SyncEvent::Warning(warning) = event.event.as_ref() {
            break warning.clone();
        }
    };
    assert_eq!(
        warning.kind,
        bifrost_types::WarningKind::Engine(bifrost_types::EngineNotice::IncompletePage {
            skipped_scopes: 1,
            failed_ids: 1,
        })
    );
    let text = warning.message.value.clone();
    assert!(text.contains("contacts_list"), "{text}");
    assert!(text.contains('1'), "the counts ride in the message: {text}");
    // `failed_ids` holds native provider identifiers, which are not
    // user-safe text. The message counts them and never names them.
    assert!(!text.contains("urn:uuid"), "{text}");
    assert!(
        warning.next_action.is_some(),
        "the warning must point at the lanes that carry the detail"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The complement: a clean page must stay silent, or the warning is
/// noise on every successful call and consumers learn to ignore it.
#[tokio::test]
async fn a_clean_forwarded_page_emits_no_warning() {
    let account_id = AccountId("page-clean".to_owned());
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(HealFactory::new(
        vec![CursorScope::Account],
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
    ));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let page = engine
        .contacts_list(&account_id, None, None)
        .await
        .expect("the clean page succeeds");
    assert!(page.failed_ids.is_empty() && page.skipped_scopes.is_empty());

    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "a call with no page-lane loss must not warn"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}
