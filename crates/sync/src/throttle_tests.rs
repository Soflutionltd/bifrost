use super::*;
use crate::recovery::reconcile_delay;
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountId, AccountOperation, AttemptCause, Cause,
    CursorScope, ErrorScope, PrimaryCause, PrincipalId, RecoveryClass, RetryHint, ServerCause,
    ThrottleKey, ThrottleScope, TransmissionState,
};
use std::time::Duration;
use std::time::SystemTime;
use tokio::time::Instant;

/// Removing the slot fence extends the successor's account hold to 120 seconds.
/// Clearing the bucket on retirement instead loses the original 30-second hold.
#[tokio::test(start_paused = true)]
async fn retirement_preserves_known_holds_but_fences_late_account_observations() {
    let shared = ThrottleBucket::new();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let retired = shared.for_slot(shutdown.clone());
    let successor = shared.for_slot(tokio_util::sync::CancellationToken::new());
    let account = AccountId("retiring".into());
    let refusal = |seconds| {
        AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
            throttle: Default::default(),
            retry_hint: Some(RetryHint::After(Duration::from_secs(seconds))),
        }))
        .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
        .operation(AccountOperation::SyncChanges)
        .with_throttle(|throttle| throttle.scope = Some(ThrottleScope::Account))
        .try_build()
        .expect("provider refusal")
    };
    retired.observe(&account, &refusal(30));
    shutdown.cancel();
    let late = refusal(120);
    assert_eq!(
        retired.observe(&account, &late),
        Some(Instant::now() + Duration::from_secs(120)),
        "late cleanup still receives its own local pacing deadline"
    );
    assert_eq!(
        successor.wait_for_account(&account, Instant::now()),
        Some(Duration::from_secs(30))
    );
    let budget = bifrost_types::ThrottleBudget::new("provider-limit");
    let late = late
        .into_builder()
        .with_throttle(|throttle| throttle.add_budget(budget.clone()))
        .try_build()
        .expect("budget refusal");
    retired.observe(&account, &late);
    assert_eq!(
        successor.wait_for(&ThrottleKey::Budget(budget), Instant::now()),
        Some(Duration::from_secs(120)),
        "an explicitly named server budget survives even a late response"
    );
    successor.observe(&account, &refusal(60));
    assert_eq!(
        shared.wait_for_account(&account, Instant::now()),
        Some(Duration::from_secs(60)),
        "the successor has its own live recording fence"
    );
}

// A hintless rate limit: `resolve_throttle_key` stamps a throttle scope on
// it, and the builder admits a throttle scope only on rate-limit or quota
// kinds, so a transport fixture would fail to build.
fn throttled_error(scope: Option<ErrorScope>) -> AccountError {
    let mut builder = AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
        throttle: Default::default(),
        retry_hint: None,
    }))
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(AccountOperation::SyncChanges);
    if let Some(scope) = scope {
        builder = builder.scope(scope);
    }
    builder.try_build().expect("valid")
}

#[tokio::test(start_paused = true)]
async fn net_local_hold_paces_without_recording_an_engine_hold() {
    use bifrost_net::{HoldDeadline, HoldKind, HttpErrorPolicy, NetErrorContext};
    use bifrost_types::{Application, PartialCommitBuilder, PrimaryLeg, StepReplay, StepTargets};

    let account = AccountId("held".into());
    let budget = bifrost_types::ThrottleBudget::new("gmail-project|123");
    let now = Instant::now();
    let wall = SystemTime::now();
    let net = bifrost_net::Net::new(bifrost_net::NetConfig::default()).unwrap();
    let hold = net.shared_hold(budget.as_str());
    for kind in [HoldKind::RateLimited, HoldKind::QuotaExhausted] {
        hold.record(
            HoldDeadline::new(wall + Duration::from_secs(10), wall, now),
            kind,
        );
        for commit in [
            bifrost_net::CommitEvidence::Unsent,
            bifrost_net::CommitEvidence::MayHaveCommitted,
        ] {
            for stamped in [false, true] {
                let local = bifrost_net::into_account_error_with_throttle_scope(
                    bifrost_net::Error::LocalHold {
                        key: budget.as_str().to_owned(),
                        held: hold.current().unwrap(),
                        evidence: bifrost_net::RequestEvidence::new(Vec::new(), commit),
                    },
                    NetErrorContext {
                        provider: None,
                        protocol: bifrost_types::Protocol::Gmail,
                        operation: AccountOperation::Send,
                        scope: None,
                    },
                    HttpErrorPolicy {
                        throttle_scope: Some(ThrottleScope::Account),
                        throttle_budget: stamped.then(|| budget.clone()),
                        ..HttpErrorPolicy::default()
                    },
                );
                // Exercise shared-Arc decoration, as provider boundaries do.
                let decorated = local.clone().into_builder().try_build().unwrap();
                assert_eq!(decorated.recovery(), local.recovery());
                let bucket = ThrottleBucket::new();
                assert_eq!(
                    bucket.observe(&account, &decorated),
                    Some(now + Duration::from_secs(10))
                );
                assert_eq!(bucket.wait_for_account(&account, now), None);
                assert_eq!(
                    bucket.wait_for(&ThrottleKey::Budget(budget.clone()), now),
                    None
                );

                // Local provenance belongs to each leg. A real sibling throttle
                // still enters the bucket, without re-recording the primary.
                let server =
                    AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
                        throttle: Default::default(),
                        retry_hint: Some(RetryHint::After(Duration::from_secs(3))),
                    }))
                    .with_throttle(|throttle| throttle.add_budget(budget.clone()))
                    .try_build()
                    .unwrap();
                let mut record =
                    PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
                record.failed_sibling(
                    None,
                    StepTargets::unknown(),
                    StepReplay::Repeatable,
                    Application::NotApplicable,
                    server.clone(),
                );
                let compound = decorated
                    .into_builder()
                    .partial_commit(record)
                    .try_build()
                    .unwrap();
                assert_eq!(
                    bucket.observe(&account, &compound),
                    Some(now + Duration::from_secs(10))
                );
                assert_eq!(
                    bucket.wait_for(&ThrottleKey::Budget(budget.clone()), now),
                    Some(Duration::from_secs(3))
                );
                assert_eq!(bucket.wait_for_account(&account, now), None);

                // The reverse shape also matters: a local sibling or cleanup
                // refusal must not extend the real primary's shared hold.
                let mut record =
                    PartialCommitBuilder::new(PrimaryLeg::effectless(StepTargets::unknown()));
                record.failed_sibling(
                    None,
                    StepTargets::unknown(),
                    StepReplay::Repeatable,
                    Application::NotApplicable,
                    local.clone(),
                );
                let committed = record.committed(
                    bifrost_types::StepEffect::Created,
                    StepTargets::unknown(),
                    StepReplay::Repeatable,
                );
                record.compensation(
                    bifrost_types::StepEffect::Deleted,
                    StepTargets::unknown(),
                    vec![committed],
                    bifrost_types::CompensationOutcome::from_error(local.clone()),
                );
                let compound = server
                    .into_builder()
                    .partial_commit(record)
                    .try_build()
                    .unwrap();
                let bucket = ThrottleBucket::new();
                assert_eq!(
                    bucket.observe(&account, &compound),
                    Some(now + Duration::from_secs(10))
                );
                assert_eq!(
                    bucket.wait_for(&ThrottleKey::Budget(budget.clone()), now),
                    Some(Duration::from_secs(3))
                );
                assert_eq!(bucket.wait_for_account(&account, now), None);
            }
        }
    }
}

#[test]
fn throttle_bucket_records_and_expires() {
    let bucket = ThrottleBucket::new();
    // A bucket instant is monotonic now; anchor the test at a fixed
    // point on that clock rather than on the wall clock.
    let now = Instant::now();
    let key = ThrottleKey::Account(AccountId("a".into()));
    bucket.record(key.clone(), now + Duration::from_secs(5));

    assert_eq!(bucket.wait_for(&key, now), Some(Duration::from_secs(5)));
    // After the deadline.
    assert_eq!(bucket.wait_for(&key, now + Duration::from_secs(10)), None);
}

#[test]
fn throttle_bucket_take_max_of_overlapping_records() {
    let bucket = ThrottleBucket::new();
    // A bucket instant is monotonic now; anchor the test at a fixed
    // point on that clock rather than on the wall clock.
    let now = Instant::now();
    let key = ThrottleKey::Account(AccountId("a".into()));
    bucket.record(key.clone(), now + Duration::from_secs(5));
    bucket.record(key.clone(), now + Duration::from_secs(2));
    assert_eq!(bucket.wait_for(&key, now), Some(Duration::from_secs(5)));
}

#[test]
fn throttle_bucket_cleanup_drops_expired() {
    let bucket = ThrottleBucket::new();
    // A bucket instant is monotonic now; anchor the test at a fixed
    // point on that clock rather than on the wall clock.
    let now = Instant::now();
    let a = ThrottleKey::Account(AccountId("a".into()));
    let b = ThrottleKey::Account(AccountId("b".into()));
    bucket.record(a.clone(), now + Duration::from_secs(1));
    bucket.record(b.clone(), now + Duration::from_secs(60));
    bucket.cleanup_expired(now + Duration::from_secs(30));
    assert!(bucket.wait_for(&a, now + Duration::from_secs(30)).is_none());
    assert!(bucket.wait_for(&b, now + Duration::from_secs(30)).is_some());
}

#[test]
fn wait_for_account_reads_only_the_account_key() {
    // A budget the account recorded holds the work that declares it,
    // not the whole account.
    let bucket = ThrottleBucket::new();
    // A bucket instant is monotonic now; anchor the test at a fixed
    // point on that clock rather than on the wall clock.
    let now = Instant::now();
    let a = AccountId("acc-1".into());
    bucket.record_for(
        ThrottleKey::Account(a.clone()),
        now + Duration::from_secs(3),
    );
    bucket.record_for(
        ThrottleKey::Budget(bifrost_types::ThrottleBudget::new("test|mailbox:x")),
        now + Duration::from_secs(8),
    );
    assert_eq!(
        bucket.wait_for_account(&a, now),
        Some(Duration::from_secs(3))
    );
}

#[test]
fn wait_for_account_excludes_mailbox_keys() {
    // A per-mailbox throttle must not pause the whole account.
    let bucket = ThrottleBucket::new();
    // A bucket instant is monotonic now; anchor the test at a fixed
    // point on that clock rather than on the wall clock.
    let now = Instant::now();
    let a = AccountId("acc-1".into());
    bucket.record_for(
        ThrottleKey::Owner {
            account: a.clone(),
            owner: PrincipalId("shared@example.com".into()),
        },
        now + Duration::from_secs(30),
    );
    assert_eq!(bucket.wait_for_account(&a, now), None);
}

#[test]
fn reconcile_throttle_is_recorded_for_the_account() {
    let account = AccountId("a".into());
    let error = AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
        throttle: Default::default(),
        retry_hint: Some(RetryHint::After(Duration::from_secs(30))),
    }))
    .operation(AccountOperation::Send)
    .push_cause(Cause::Attempt(AttemptCause::new(
        TransmissionState::InFlight,
    )))
    .with_throttle(|throttle| throttle.scope = Some(ThrottleScope::Account))
    .try_build()
    .expect("valid");
    let RecoveryClass::Reconcile(advice) = error.recovery() else {
        panic!("in-flight send must reconcile");
    };
    let bucket = ThrottleBucket::new();

    record_reconcile_throttle(&bucket, &account, advice, &error);

    assert!(account_throttle_wait(&bucket, &account).is_some());

    // The drive loop and the push reconciler sleep this before
    // probing; the provider hint must win over the 1s fallback or
    // the read-back walks straight back into the rate limit.
    let delay = reconcile_delay(advice, SystemTime::now(), Duration::from_secs(1));
    assert!(
        delay > Duration::from_secs(25),
        "throttled reconcile must honor the provider hint, got {delay:?}"
    );
}

/// A stamped budget is recorded ALONE, and holds even without a hint.
///
/// The error also carries an `Account` throttle scope, as every Graph
/// throttle does. Lifting that into the account key as well would pause
/// every sibling mailbox of the account; and a hintless stamp that
/// recorded nothing would leave a sibling declaring the budget to walk
/// straight into the same 429 the observer is pacing itself against.
#[test]
fn a_stamped_budget_is_recorded_alone_and_holds_without_a_hint() {
    let account = AccountId("a".into());
    let budget = bifrost_types::ThrottleBudget::new("test|mailbox:x");
    let error = AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
        throttle: Default::default(),
        retry_hint: None,
    }))
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(AccountOperation::SyncChanges)
    .with_throttle(|throttle| throttle.scope = Some(ThrottleScope::Account))
    .with_throttle(|throttle| throttle.add_budget(budget.clone()))
    .try_build()
    .expect("valid");
    let RecoveryClass::Retry(advice) = error.recovery() else {
        panic!("an unsent rate limit retries");
    };
    let bucket = ThrottleBucket::new();

    record_throttle(&bucket, &account, advice, &error);

    assert_eq!(
        account_throttle_wait(&bucket, &account),
        None,
        "the lifted account key must not be recorded beside a stamped budget"
    );
    let wait = throttle_wait(&bucket, &account, &[ThrottleKey::Budget(budget)]);
    assert!(
        wait.is_some_and(|wait| wait <= STAMPED_BUDGET_FALLBACK),
        "a hintless stamped budget holds for the local fallback, got {wait:?}"
    );
}

/// A hintless folder-scoped mailbox throttle (IMAP's `[LIMIT]`) is held
/// for the fallback on its `Mailbox` key, while a hintless account-scoped
/// one still records nothing.
#[test]
fn a_hintless_mailbox_throttle_holds_its_mailbox_for_the_fallback() {
    let account = AccountId("a".into());
    let build = |scope: ThrottleScope, error_scope: Option<ErrorScope>| {
        let mut builder =
            AccountErrorBuilder::new(PrimaryCause::Server(ServerCause::RateLimited {
                throttle: Default::default(),
                retry_hint: None,
            }))
            .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
            .operation(AccountOperation::SyncChanges)
            .with_throttle(|throttle| throttle.scope = Some(scope));
        if let Some(error_scope) = error_scope {
            builder = builder.scope(error_scope);
        }
        builder.try_build().expect("valid")
    };
    let record = |bucket: &ThrottleBucket, error: &AccountError| {
        let RecoveryClass::Retry(advice) = error.recovery() else {
            panic!("an unsent rate limit retries");
        };
        record_throttle(bucket, &account, advice, error);
    };

    let bucket = ThrottleBucket::new();
    record(
        &bucket,
        &build(
            ThrottleScope::ScopedResource,
            Some(ErrorScope::Cursor(CursorScope::Folder(
                bifrost_types::FolderId("INBOX".into()),
            ))),
        ),
    );
    let inbox = ThrottleKey::Folder {
        account: account.clone(),
        folder: bifrost_types::FolderId("INBOX".into()),
    };
    assert!(
        throttle_wait(&bucket, &account, &[inbox])
            .is_some_and(|wait| wait <= STAMPED_BUDGET_FALLBACK)
    );

    let bucket = ThrottleBucket::new();
    record(&bucket, &build(ThrottleScope::Account, None));
    assert_eq!(account_throttle_wait(&bucket, &account), None);
}

/// A declared shared key is consulted directly: an account that never
/// recorded anything, so was never enrolled, still sees a deadline
/// another account recorded under a key it declares.
#[test]
fn a_declared_key_reaches_an_account_that_was_never_enrolled() {
    let bystander = AccountId("bystander".into());
    let key = ThrottleKey::Budget(bifrost_types::ThrottleBudget::new("test|mailbox:x"));
    let bucket = ThrottleBucket::new();
    bucket.record_for(key.clone(), Instant::now() + Duration::from_secs(30));

    assert_eq!(account_throttle_wait(&bucket, &bystander), None);
    assert!(throttle_wait(&bucket, &bystander, &[key]).is_some());
}

#[test]
fn resolve_throttle_key_current_operation_does_not_enter_bucket() {
    let account = AccountId("a".into());
    let error = throttled_error(None);
    assert!(resolve_throttle_key(ThrottleScope::CurrentOperation, &account, &error).is_none());
}

#[test]
fn resolve_throttle_key_reads_identities_from_the_error() {
    let account = AccountId("a".into());
    let mailbox_err = throttled_error(Some(ErrorScope::Mailbox {
        id: "shared@example.com".into(),
    }));
    assert_eq!(
        resolve_throttle_key(ThrottleScope::ScopedResource, &account, &mailbox_err),
        Some(ThrottleKey::Folder {
            account: account.clone(),
            folder: bifrost_types::FolderId("shared@example.com".into()),
        })
    );
}

#[test]
fn throttle_owner_and_container_ids_never_share_a_bucket() {
    let account = AccountId("a".into());
    let owner = throttled_error(Some(ErrorScope::Owner {
        id: bifrost_types::PrincipalId("same".into()),
    }));
    let folder = throttled_error(Some(ErrorScope::Cursor(CursorScope::FolderType {
        folder: bifrost_types::FolderId("same".into()),
        ty: bifrost_types::ObjectType::Email,
    })));
    let mailbox = throttled_error(Some(ErrorScope::Mailbox { id: "same".into() }));
    let owner_key = resolve_throttle_key(ThrottleScope::ScopedResource, &account, &owner).unwrap();
    let folder_key =
        resolve_throttle_key(ThrottleScope::ScopedResource, &account, &folder).unwrap();
    let mailbox_key =
        resolve_throttle_key(ThrottleScope::ScopedResource, &account, &mailbox).unwrap();
    assert_eq!(
        owner_key,
        ThrottleKey::Owner {
            account: account.clone(),
            owner: bifrost_types::PrincipalId("same".into())
        }
    );
    assert_eq!(
        folder_key,
        ThrottleKey::Folder {
            account,
            folder: bifrost_types::FolderId("same".into())
        }
    );
    assert_ne!(owner_key, folder_key);
    assert_ne!(owner_key, mailbox_key);
    assert_eq!(folder_key, mailbox_key);
}

/// The shape production actually produces.
///
/// The test above builds `ErrorScope::Mailbox { .. }`, which only tests
/// construct: IMAP's `with_mailbox` is annotated production-dead, and every
/// real folder producer goes through `with_folder_scope`, yielding
/// `Cursor(Folder(_))`. IMAP's `mailbox_throttle` is the only production
/// emitter of `ThrottleScope::ScopedResource` anywhere in the workspace and it
/// fires for both shapes, so a reader accepting only the synthetic one
/// degraded on every real per-folder throttle and on none of the tested
/// ones - the bucket entry was simply never recorded.
#[test]
fn a_folder_scoped_throttle_keys_the_folder_bucket() {
    let account = AccountId("a".into());
    let folder_err = throttled_error(Some(ErrorScope::Cursor(CursorScope::Folder(
        bifrost_types::FolderId("INBOX/shared".into()),
    ))));
    assert_eq!(
        resolve_throttle_key(ThrottleScope::ScopedResource, &account, &folder_err),
        Some(ThrottleKey::Folder {
            account,
            folder: bifrost_types::FolderId("INBOX/shared".into()),
        }),
        "a folder-scoped per-mailbox throttle must record a mailbox bucket \
         entry, not fall through to no entry at all"
    );
}

#[test]
fn a_mailbox_throttle_naming_no_mailbox_never_widens_to_the_account() {
    let account = AccountId("a".into());
    let bare = throttled_error(None);
    assert_eq!(
        resolve_throttle_key(ThrottleScope::ScopedResource, &account, &bare),
        None
    );
    // `throttled_error` carries no provider timing, and an account-wide
    // hold requires a hint (`AccountError::throttle_holds`): a guessed
    // deadline on the account key would pause every folder. The hinted
    // account case is pinned by `reconcile_throttle_is_recorded_for_the_account`.
    assert_eq!(
        resolve_throttle_key(ThrottleScope::Account, &account, &bare),
        None
    );
}
