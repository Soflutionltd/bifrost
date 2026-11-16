//! Server-side filter passthrough tests.
//!
//! Attaches a recording `Account` double to a `SyncEngine` and drives
//! the five `SyncEngine::filter_*` passthroughs, asserting that (1) each
//! call forwards its arguments 1:1 to the matching `Account` method and
//! relays the account's return value, (2) an `AccountError` folds into
//! `Error::Account` through the `?` funnel, and (3) an unattached
//! account short-circuits to `Error::AccountNotAttached` before any
//! forwarding happens.

use std::sync::{Arc, Mutex};

use bifrost_sync::{Error, SyncEngine};
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountId,
    FilterDiagnostic, FilterDiagnosticSeverity, FilterRuleShape, FilterScriptCreate,
    FilterScriptPatch, FilterValidation, ScriptLanguage, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch,
};

mod common;

use common::unsupported;

fn caps() -> AccountCapabilities {
    AccountCapabilities {
        filter_rule_shape: FilterRuleShape::Scripts,
        ..common::caps()
    }
}

/// What the recording account observed on its last filter call.
#[derive(Default)]
struct Recorded {
    list_called: bool,
    created: Option<ServerFilterCreate>,
    updated: Option<(ServerFilterId, ServerFilterPatch)>,
    deleted: Option<ServerFilterId>,
    validated: Option<ServerFilterCreate>,
}

/// Account double: records the arguments each filter primitive is
/// called with (or, when `fail` is set, returns an `AccountError` so the
/// engine's `?` folding can be exercised). The shared stub discovers zero
/// scopes, so `attach` establishes zero
/// scopes and completes without touching any of them.
struct FilterAccount {
    caps: AccountCapabilities,
    rec: Arc<Mutex<Recorded>>,
    fail: bool,
}

struct FilterFactory {
    rec: Arc<Mutex<Recorded>>,
    fail: bool,
}

impl AccountFactory for FilterFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let account = FilterAccount {
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

/// A representative script filter used both as the `filters_list` return
/// value and to seed create/validate payloads.
fn sample_script_create(body: &str) -> ServerFilterCreate {
    ServerFilterCreate::Script(FilterScriptCreate {
        name: Some("test-script".to_owned()),
        language: ScriptLanguage::Sieve,
        body: body.to_owned(),
        is_active: true,
    })
}

impl FilterAccount {
    fn into_stub(self) -> common::StubAccount {
        let mut stub = common::StubAccount::new(Vec::new());
        stub.protocol = bifrost_types::Protocol::Unknown;
        stub.caps = self.caps.clone();
        let config = Arc::new(self);
        let config_for_hook = Arc::clone(&config);
        stub.lanes.filters.filters_list = Some(Arc::new(move || {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::FiltersList));
                }
                rec.lock().expect("recorder lock").list_called = true;
                Ok(vec![ServerFilter::Script(bifrost_types::FilterScript {
                    id: ServerFilterId("listed-script".to_owned()),
                    name: Some("listed".to_owned()),
                    language: ScriptLanguage::Sieve,
                    body: "keep;".to_owned(),
                    is_active: true,
                })])
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.filters.filter_create = Some(Arc::new(move |filter| {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::FilterCreate));
                }
                rec.lock().expect("recorder lock").created = Some(filter);
                Ok(ServerFilterId("created-id".to_owned()))
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.filters.filter_update = Some(Arc::new(move |filter, patch| {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::FilterUpdate));
                }
                rec.lock().expect("recorder lock").updated = Some((filter, patch));
                Ok(())
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.filters.filter_delete = Some(Arc::new(move |filter| {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::FilterDelete));
                }
                rec.lock().expect("recorder lock").deleted = Some(filter);
                Ok(())
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.filters.filter_validate = Some(Arc::new(move |filter| {
            let config = &config_for_hook;
            let fail = config.fail;
            let rec = Arc::clone(&config.rec);
            Box::pin(async move {
                if fail {
                    return Err(unsupported(bifrost_types::AccountOperation::FilterValidate));
                }
                rec.lock().expect("recorder lock").validated = Some(filter);
                Ok(FilterValidation {
                    diagnostics: vec![FilterDiagnostic {
                        severity: FilterDiagnosticSeverity::Warning,
                        message: "accepted with warning".to_owned(),
                        line: Some(1),
                        column: None,
                    }],
                })
            })
        }));
        stub
    }
}

/// Attach a `FilterAccount` and return the engine plus its shared
/// recorder. `fail` selects the error-returning mode.
async fn attach_filter_account(
    account_id: &AccountId,
    fail: bool,
) -> (SyncEngine, Arc<Mutex<Recorded>>) {
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let rec = Arc::new(Mutex::new(Recorded::default()));
    let factory: Arc<dyn AccountFactory> = Arc::new(FilterFactory {
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
async fn filter_passthroughs_forward_arguments_and_relay_returns() {
    let account_id = AccountId("filter-forward".to_owned());
    let (engine, rec) = attach_filter_account(&account_id, false).await;

    // filters_list: relays the account's returned vec.
    let listed = engine
        .filters_list(&account_id)
        .await
        .expect("filters_list forwards");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id(), &ServerFilterId("listed-script".to_owned()));
    assert!(rec.lock().expect("recorder lock").list_called);

    // filter_create: forwards the create payload, relays the new id.
    let create = sample_script_create("fileinto \"INBOX\";");
    let new_id = engine
        .filter_create(&account_id, create)
        .await
        .expect("filter_create forwards");
    assert_eq!(new_id, ServerFilterId("created-id".to_owned()));
    match rec.lock().expect("recorder lock").created.clone() {
        Some(ServerFilterCreate::Script(script)) => {
            assert_eq!(script.body, "fileinto \"INBOX\";");
            assert_eq!(script.name.as_deref(), Some("test-script"));
        }
        other => panic!("expected forwarded script create, got {other:?}"),
    }

    // filter_update: forwards both the id and the patch.
    let patch = ServerFilterPatch::Script(FilterScriptPatch {
        name: None,
        body: Some("discard;".to_owned()),
        is_active: Some(false),
    });
    engine
        .filter_update(&account_id, ServerFilterId("edit-me".to_owned()), patch)
        .await
        .expect("filter_update forwards");
    match rec.lock().expect("recorder lock").updated.clone() {
        Some((id, ServerFilterPatch::Script(script))) => {
            assert_eq!(id, ServerFilterId("edit-me".to_owned()));
            assert_eq!(script.body.as_deref(), Some("discard;"));
            assert_eq!(script.is_active, Some(false));
        }
        other => panic!("expected forwarded script patch, got {other:?}"),
    }

    // filter_delete: forwards the id.
    engine
        .filter_delete(&account_id, ServerFilterId("drop-me".to_owned()))
        .await
        .expect("filter_delete forwards");
    assert_eq!(
        rec.lock().expect("recorder lock").deleted.clone(),
        Some(ServerFilterId("drop-me".to_owned()))
    );

    // filter_validate: forwards the payload, relays the diagnostics.
    let validation = engine
        .filter_validate(&account_id, sample_script_create("keep;"))
        .await
        .expect("filter_validate forwards");
    assert!(validation.is_valid());
    assert_eq!(validation.diagnostics.len(), 1);
    match rec.lock().expect("recorder lock").validated.clone() {
        Some(ServerFilterCreate::Script(script)) => assert_eq!(script.body, "keep;"),
        other => panic!("expected forwarded validate payload, got {other:?}"),
    }
}

#[tokio::test]
async fn filter_passthroughs_fold_account_error_into_error_account() {
    let account_id = AccountId("filter-error".to_owned());
    let (engine, _rec) = attach_filter_account(&account_id, true).await;

    assert!(matches!(
        engine.filters_list(&account_id).await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .filter_create(&account_id, sample_script_create("keep;"))
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .filter_update(
                &account_id,
                ServerFilterId("x".to_owned()),
                ServerFilterPatch::Script(FilterScriptPatch::default()),
            )
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .filter_delete(&account_id, ServerFilterId("x".to_owned()))
            .await,
        Err(Error::Account(_))
    ));
    assert!(matches!(
        engine
            .filter_validate(&account_id, sample_script_create("keep;"))
            .await,
        Err(Error::Account(_))
    ));
}

#[tokio::test]
async fn filter_passthroughs_reject_unattached_account() {
    let engine = SyncEngine::builder()
        .build()
        .expect("default engine config is valid");
    let missing = AccountId("never-attached".to_owned());

    assert!(matches!(
        engine.filters_list(&missing).await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .filter_create(&missing, sample_script_create("keep;"))
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .filter_update(
                &missing,
                ServerFilterId("x".to_owned()),
                ServerFilterPatch::Script(FilterScriptPatch::default()),
            )
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .filter_delete(&missing, ServerFilterId("x".to_owned()))
            .await,
        Err(Error::AccountNotAttached(_))
    ));
    assert!(matches!(
        engine
            .filter_validate(&missing, sample_script_create("keep;"))
            .await,
        Err(Error::AccountNotAttached(_))
    ));
}
