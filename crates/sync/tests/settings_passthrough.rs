//! Account settings passthrough tests.
//!
//! Attaches a recording `Account` double to a `SyncEngine` and drives
//! the five `SyncEngine` settings passthroughs (`identities_list`,
//! `identity_update`, `vacation_get`, `vacation_set`, `quota_get`),
//! asserting that (1) each call forwards its arguments 1:1 to the
//! matching `Account` method and relays the account's return value, (2)
//! an `AccountError` folds into `Error::Account` through the `?` funnel,
//! and (3) an unattached account short-circuits to
//! `Error::AccountNotAttached` before any forwarding happens.

use std::sync::{Arc, Mutex};

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountId, Identity,
    IdentityId, IdentityPatch, QuotaInfo, VacationConfig,
};

mod common;

use common::{caps, unsupported};

/// What the recording account observed on its last settings call.
#[derive(Default)]
struct Recorded {
    identities_called: bool,
    identity_updated: Option<(IdentityId, IdentityPatch)>,
    vacation_get_called: bool,
    vacation_set: Option<VacationConfig>,
    quota_called: bool,
}

/// Account double: records the arguments each settings primitive is
/// called with (or, when `fail` is set, returns an `AccountError` so the
/// engine's `?` folding can be exercised). The shared stub discovers zero
/// scopes, so `attach` establishes zero
/// scopes and completes without touching any of them.
struct SettingsAccount {
    caps: AccountCapabilities,
    rec: Arc<Mutex<Recorded>>,
    fail: bool,
}

struct SettingsFactory {
    rec: Arc<Mutex<Recorded>>,
    fail: bool,
}

impl AccountFactory for SettingsFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let account = SettingsAccount {
            caps: caps(),
            rec: Arc::clone(&self.rec),
            fail: self.fail,
        }
        .into_stub();
        Box::pin(async move {
            Ok(bifrost_types::OpenedAccount::complete(
                Arc::new(account) as Arc<dyn Account>
            ))
        })
    }
}

/// A representative identity used as the `identities_list` return value.
fn sample_identity() -> Identity {
    Identity {
        id: IdentityId("listed-identity".to_owned()),
        name: "Listed User".to_owned(),
        address: "listed@example.test".to_owned(),
        signature_text: Some("-- listed".to_owned()),
        signature_html: None,
        reply_to: None,
        is_default: true,
    }
}

impl SettingsAccount {
    fn into_stub(self) -> common::StubAccount {
        let mut stub = common::StubAccount::new(Vec::new());
        stub.protocol = bifrost_types::Protocol::Unknown;
        stub.caps = self.caps.clone();
        let config = Arc::new(self);
        let config_for_hook = Arc::clone(&config);
        stub.lanes.settings.identities_list = Some(Arc::new(move || {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::IdentitiesList));
                }
                rec.lock().expect("recorder lock").identities_called = true;
                Ok(vec![sample_identity()])
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.settings.identity_update = Some(Arc::new(move |identity, patch| {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::IdentityUpdate));
                }
                rec.lock().expect("recorder lock").identity_updated = Some((identity, patch));
                Ok(())
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.settings.vacation_get = Some(Arc::new(move || {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::VacationGet));
                }
                rec.lock().expect("recorder lock").vacation_get_called = true;
                Ok(Some(VacationConfig {
                    is_enabled: true,
                    subject: Some("Away".to_owned()),
                    body_text: Some("Out of office".to_owned()),
                    body_html: None,
                    starts_at: None,
                    ends_at: None,
                }))
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.settings.vacation_set = Some(Arc::new(move |config| {
            let state = &config_for_hook;
            let fail = state.fail;
            let rec = Arc::clone(&state.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::VacationSet));
                }
                rec.lock().expect("recorder lock").vacation_set = Some(config);
                Ok(())
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.settings.quota_get = Some(Arc::new(move || {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::QuotaGet));
                }
                rec.lock().expect("recorder lock").quota_called = true;
                Ok(Some(QuotaInfo {
                    used_bytes: 512,
                    total_bytes: Some(4096),
                }))
            })
        }));
        stub
    }
}

/// Attach a `SettingsAccount` and return the engine plus its shared
/// recorder. `fail` selects the error-returning mode.
async fn attach_settings_account(
    account_id: &AccountId,
    fail: bool,
) -> (SyncEngine, Arc<Mutex<Recorded>>) {
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let rec = Arc::new(Mutex::new(Recorded::default()));
    let factory: Arc<dyn AccountFactory> = Arc::new(SettingsFactory {
        rec: Arc::clone(&rec),
        fail,
    });
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds with empty discovery streams");
    (engine, rec)
}

#[tokio::test]
async fn settings_passthroughs_forward_arguments_and_relay_returns() {
    let account_id = AccountId("settings-forward".to_owned());
    let (engine, rec) = attach_settings_account(&account_id, false).await;

    // identities_list: relays the account's returned vec.
    let listed = engine
        .identities_list(&account_id)
        .await
        .expect("identities_list forwards");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, IdentityId("listed-identity".to_owned()));
    assert_eq!(listed[0].address, "listed@example.test");
    assert!(rec.lock().expect("recorder lock").identities_called);

    // identity_update: forwards both the id and the patch.
    let mut patch = IdentityPatch::default();
    patch.name = Some("Renamed".to_owned());
    patch.signature_text = Some(Some("-- new sig".to_owned()));
    patch.is_default = Some(true);
    engine
        .identity_update(&account_id, IdentityId("edit-me".to_owned()), patch)
        .await
        .expect("identity_update forwards");
    match rec.lock().expect("recorder lock").identity_updated.clone() {
        Some((id, patch)) => {
            assert_eq!(id, IdentityId("edit-me".to_owned()));
            assert_eq!(patch.name.as_deref(), Some("Renamed"));
            assert_eq!(patch.signature_text, Some(Some("-- new sig".to_owned())));
            assert_eq!(patch.is_default, Some(true));
        }
        None => panic!("expected forwarded identity patch"),
    }

    // vacation_get: relays the account's returned config.
    let vacation = engine
        .vacation_get(&account_id)
        .await
        .expect("vacation_get forwards")
        .expect("responder configured");
    assert!(vacation.is_enabled);
    assert_eq!(vacation.subject.as_deref(), Some("Away"));
    assert!(rec.lock().expect("recorder lock").vacation_get_called);

    // vacation_set: forwards the config payload.
    let config = VacationConfig {
        is_enabled: false,
        subject: Some("Back soon".to_owned()),
        body_text: Some("Returning Monday".to_owned()),
        body_html: Some("<p>Returning Monday</p>".to_owned()),
        starts_at: None,
        ends_at: None,
    };
    engine
        .vacation_set(&account_id, config)
        .await
        .expect("vacation_set forwards");
    match rec.lock().expect("recorder lock").vacation_set.clone() {
        Some(config) => {
            assert!(!config.is_enabled);
            assert_eq!(config.subject.as_deref(), Some("Back soon"));
            assert_eq!(config.body_text.as_deref(), Some("Returning Monday"));
            assert_eq!(config.body_html.as_deref(), Some("<p>Returning Monday</p>"));
        }
        None => panic!("expected forwarded vacation config"),
    }

    // quota_get: relays the account's returned readout.
    let quota = engine
        .quota_get(&account_id)
        .await
        .expect("quota_get forwards")
        .expect("quota reported");
    assert_eq!(quota.used_bytes, 512);
    assert_eq!(quota.total_bytes, Some(4096));
    assert!(rec.lock().expect("recorder lock").quota_called);
}

#[tokio::test]
async fn settings_passthroughs_fold_account_error_into_error_account() {
    let account_id = AccountId("settings-error".to_owned());
    let (engine, _rec) = attach_settings_account(&account_id, true).await;

    assert!(matches!(
        engine.identities_list(&account_id).await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .identity_update(
                &account_id,
                IdentityId("x".to_owned()),
                IdentityPatch::default(),
            )
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine.vacation_get(&account_id).await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .vacation_set(
                &account_id,
                VacationConfig {
                    is_enabled: false,
                    subject: None,
                    body_text: None,
                    body_html: None,
                    starts_at: None,
                    ends_at: None,
                },
            )
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine.quota_get(&account_id).await,
        Err(Error::Account(_))
    ));
}

#[tokio::test]
async fn settings_passthroughs_reject_unattached_account() {
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let missing = AccountId("never-attached".to_owned());

    assert!(matches!(
        engine.identities_list(&missing).await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .identity_update(
                &missing,
                IdentityId("x".to_owned()),
                IdentityPatch::default()
            )
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine.vacation_get(&missing).await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .vacation_set(
                &missing,
                VacationConfig {
                    is_enabled: false,
                    subject: None,
                    body_text: None,
                    body_html: None,
                    starts_at: None,
                    ends_at: None,
                },
            )
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine.quota_get(&missing).await,
        Err(Error::AccountNotAttached(_))
    ));
}
