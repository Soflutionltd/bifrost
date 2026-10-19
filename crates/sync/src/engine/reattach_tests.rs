//! Recovery pacing tests use the engine's own writer and in-process accounts.
use super::*;
use crate::test_common as common;
use bifrost_types::{
    AccountFuture, AttemptCause, Cause, ObjectType, ServerCause, ServerErrorKind, ThrottleBudget,
    ThrottleKey, ThrottleScope, TransmissionState,
};
use std::sync::atomic::{AtomicUsize, Ordering};

const SCOPE: CursorScope = CursorScope::Type(ObjectType::Email);
fn key() -> ThrottleKey {
    ThrottleKey::Budget(ThrottleBudget::new("repair-test"))
}
fn throttle(seconds: u64) -> AccountError {
    bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Server(ServerErrorKind::RateLimited),
        Cause::Server(ServerCause::RateLimited {
            retry_hint: Some(bifrost_types::RetryHint::After(Duration::from_secs(
                seconds,
            ))),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(bifrost_types::AccountOperation::EstablishCursor)
    .throttle_scope(ThrottleScope::Account)
    .throttle_budget(ThrottleBudget::new("repair-test"))
    .try_build()
    .expect("valid throttle")
}

struct Fixture {
    engine: SyncEngine,
    slot: Arc<AccountSlot>,
    id: AccountId,
}
impl Fixture {
    async fn new(factory: Arc<dyn AccountFactory>) -> Self {
        let engine = SyncEngine::builder().build().expect("engine");
        let id = AccountId("repair".into());
        engine.attach(id.clone(), factory).await.expect("attach");
        let slot = Arc::clone(engine.accounts.get(&id).expect("slot").value());
        Self { engine, slot, id }
    }
    fn writer(&self) -> WriterHandle {
        WriterHandle::new(
            self.engine
                .ack_senders
                .get(&self.id)
                .expect("writer")
                .value()
                .clone(),
        )
    }
    fn ctx<'a>(&'a self, writer: &'a WriterHandle) -> RecoveryContext<'a> {
        let slot = &self.slot;
        RecoveryContext {
            bandwidth_budget: slot.bandwidth_budget.as_ref(),
            factory: &slot.factory,
            current: &slot.current,
            cursors: &slot.cursors,
            changes_tx: &slot.multiplexer.changes_tx,
            account_id: &self.id,
            control: &slot.control,
            account_control_tx: &slot.account_control_tx,
            throttles: &slot.throttles,
            boundary_tx: &slot.boundary_tx,
            capabilities: &slot.capabilities,
            subscriptions: &self.engine.subscriptions,
            account_generation_tx: &slot.account_generation_tx,
            reopen_lock: &slot.reopen_lock,
            open_skips: &slot.open_skips,
            shutdown: &slot.shutdown,
            writer,
            coverage: &slot.coverage,
            delivery: &slot.delivery,
        }
    }
    fn hold(&self, seconds: u64) {
        self.slot.throttles.lock().expect("bucket").record(
            key(),
            tokio::time::Instant::now() + Duration::from_secs(seconds),
        );
    }
}
fn account() -> common::StubAccount {
    let mut account = common::StubAccount::new(vec![]);
    account.scope_keys = Some(Arc::new(|_| vec![key()]));
    account
}
async fn turns() {
    for _ in 0..30 {
        tokio::task::yield_now().await;
    }
}
async fn step(seconds: u64) {
    tokio::time::advance(Duration::from_secs(seconds)).await;
    turns().await;
}
fn calls(account: &common::StubAccount) -> usize {
    account.established.lock().expect("calls").len()
}

#[tokio::test(start_paused = true)]
async fn repair_declared_hold_blocks_first_attempt_and_clears() {
    let account = Arc::new(account());
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::<
        common::StubAccount,
    >::clone(
        &account
    )])))
    .await;
    fixture.hold(20);
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let repair = re_establish_scope_with_backoff(&ctx, SCOPE, ctx.control);
    tokio::pin!(repair);
    tokio::select! { biased; () = &mut repair => panic!("repair ignored hold"), () = turns() => {} }
    assert_eq!(calls(&account), 0);
    step(19).await;
    tokio::select! { biased; () = &mut repair => panic!("repair ran early"), () = turns() => {} }
    assert_eq!(calls(&account), 0);
    step(1).await;
    repair.await;
    assert_eq!(calls(&account), 1);
}

fn failing_inventory(seconds: u64, failures: usize) -> Arc<common::StubAccount> {
    let mut account = account();
    account.establishment = |_| CursorEstablishment::EstablishViaInventory;
    let count = AtomicUsize::new(0);
    account.inventory_hook = Some(Arc::new(move |scope| {
        if count.fetch_add(1, Ordering::SeqCst) < failures {
            vec![bifrost_types::InventoryEvent::Terminated(throttle(seconds))]
        } else {
            common::establishing_inventory()(scope)
        }
    }));
    Arc::new(account)
}

#[tokio::test(start_paused = true)]
async fn repair_429_is_recorded_for_another_lane() {
    let account = failing_inventory(20, 1);
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::<
        common::StubAccount,
    >::clone(
        &account
    )])))
    .await;
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let repair = re_establish_scope_with_backoff(&ctx, SCOPE, ctx.control);
    tokio::pin!(repair);
    tokio::select! { biased; () = &mut repair => panic!("repair finished"), () = turns() => {} }
    assert_eq!(calls(&account), 1);
    assert_eq!(
        crate::recovery::throttle_wait(ctx.throttles, &AccountId("peer".into()), &[key()]),
        Some(Duration::from_secs(20))
    );
}

#[tokio::test(start_paused = true)]
async fn repair_throttle_wait_replaces_jitter_without_spending_attempts() {
    let account = failing_inventory(20, 2);
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::<
        common::StubAccount,
    >::clone(
        &account
    )])))
    .await;
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let repair = re_establish_scope_with_backoff(&ctx, SCOPE, ctx.control);
    tokio::pin!(repair);
    for expected in 1..=2 {
        tokio::select! { biased; () = &mut repair => panic!("repair finished early"), () = turns() => {} }
        assert_eq!(calls(&account), expected);
        step(19).await;
        tokio::select! { biased; () = &mut repair => panic!("repair ignored hold"), () = turns() => {} }
        assert_eq!(calls(&account), expected);
        step(1).await;
    }
    // Exactly the two holds, with no jitter delay appended to either.
    tokio::select! { biased; () = &mut repair => {}, () = turns() => panic!("extra retry delay") }
    assert_eq!(calls(&account), 3);
}

#[tokio::test(start_paused = true)]
async fn repair_wait_allowance_expires_without_provider_call() {
    let account = Arc::new(account());
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::<
        common::StubAccount,
    >::clone(
        &account
    )])))
    .await;
    fixture.hold(600);
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let mut events = ctx.changes_tx.subscribe();
    let repair = re_establish_scope_with_backoff(&ctx, SCOPE, ctx.control);
    tokio::pin!(repair);
    tokio::select! { biased; () = &mut repair => panic!("ignored hold"), () = turns() => {} }
    // Poll only after BOTH timers are ready: expiry must still win.
    step(600).await;
    repair.await;
    assert_eq!(calls(&account), 0);
    let event = events.try_recv().expect("wait exhaustion warning");
    assert!(format!("{:?}", event.event).contains("recovery wait budget exhausted"));
}

struct RetryFactory {
    initial: Arc<common::StubAccount>,
    replacement: Arc<common::StubAccount>,
    opens: AtomicUsize,
}
impl AccountFactory for RetryFactory {
    fn open(
        &self,
        _: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let count = self.opens.fetch_add(1, Ordering::SeqCst);
        let result = match count {
            0 => Ok(bifrost_types::OpenedAccount {
                account: Arc::<common::StubAccount>::clone(&self.initial),
                skipped_scopes: vec![],
            }),
            1 => Err(throttle(20)),
            _ => Ok(bifrost_types::OpenedAccount {
                account: Arc::<common::StubAccount>::clone(&self.replacement),
                skipped_scopes: vec![],
            }),
        };
        Box::pin(async move { result })
    }
}
#[tokio::test(start_paused = true)]
async fn repair_restart_waits_before_opening_next_replacement() {
    let factory = Arc::new(RetryFactory {
        initial: Arc::new(account()),
        replacement: Arc::new(account()),
        opens: AtomicUsize::new(0),
    });
    let fixture = Fixture::new(Arc::<RetryFactory>::clone(&factory)).await;
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let restart = restart_account(&ctx);
    tokio::pin!(restart);
    tokio::select! { biased; _ = &mut restart => panic!("restart finished"), () = turns() => {} }
    step(19).await;
    tokio::select! { biased; _ = &mut restart => panic!("restart ignored hold"), () = turns() => {} }
    assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
    step(1).await;
    assert!(restart.await);
    assert_eq!(factory.opens.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn repair_replacement_uses_its_own_declared_keys() {
    let initial = Arc::new(common::StubAccount::new(vec![]));
    let mut replacement = account();
    replacement.scopes = vec![SCOPE];
    let replacement = Arc::new(replacement);
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![
        initial,
        Arc::<common::StubAccount>::clone(&replacement),
    ])))
    .await;
    fixture.hold(20);
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let restart = restart_account(&ctx);
    tokio::pin!(restart);
    tokio::select! { biased; _ = &mut restart => panic!("replacement ignored declaration"), () = turns() => {} }
    assert_eq!(calls(&replacement), 0);
    step(20).await;
    assert!(restart.await);
    assert_eq!(calls(&replacement), 1);
}

#[tokio::test(start_paused = true)]
async fn repair_first_attempt_honours_hold_recorded_by_another_lane() {
    let account = Arc::new(account());
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::<
        common::StubAccount,
    >::clone(
        &account
    )])))
    .await;
    let error = throttle(20);
    let crate::recovery::RecoveryPlan::Retry(advice) =
        crate::recovery::plan_recovery(error.clone())
    else {
        panic!("retry")
    };
    crate::recovery::record_throttle(
        &fixture.slot.throttles,
        &AccountId("other-lane".into()),
        &advice,
        &error,
    );
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let repair = re_establish_scope_with_backoff(&ctx, SCOPE, ctx.control);
    tokio::pin!(repair);
    tokio::select! { biased; () = &mut repair => panic!("first attempt ignored existing hold"), () = turns() => {} }
    assert_eq!(calls(&account), 0);
    step(20).await;
    repair.await;
    assert_eq!(calls(&account), 1);
}

#[tokio::test(start_paused = true)]
async fn repair_wait_allowance_is_cumulative_across_hold_extensions() {
    let account = Arc::new(account());
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::<
        common::StubAccount,
    >::clone(
        &account
    )])))
    .await;
    fixture.hold(200);
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let repair = re_establish_scope_with_backoff(&ctx, SCOPE, ctx.control);
    tokio::pin!(repair);
    tokio::select! { biased; () = &mut repair => panic!("ignored hold"), () = turns() => {} }
    step(100).await;
    fixture.hold(300);
    step(100).await;
    tokio::select! { biased; () = &mut repair => panic!("repair finished before allowance"), () = turns() => {} }
    step(100).await;
    repair.await;
    assert_eq!(calls(&account), 0);
}

struct CountingQueue {
    inner: common::StubFactory,
    opens: AtomicUsize,
}
impl AccountFactory for CountingQueue {
    fn open(
        &self,
        id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.open(id)
    }
}

#[tokio::test(start_paused = true)]
async fn repair_restart_carries_failed_scope_keys_across_replacement_close() {
    let initial = Arc::new(common::StubAccount::new(vec![]));
    let mut failed = account();
    failed.scopes = vec![SCOPE];
    failed.establishment = |_| CursorEstablishment::EstablishViaInventory;
    failed.inventory_hook = Some(Arc::new(|_| {
        vec![bifrost_types::InventoryEvent::Terminated(throttle(20))]
    }));
    let failed = Arc::new(failed);
    let factory = Arc::new(CountingQueue {
        inner: common::StubFactory::queue(vec![
            initial,
            Arc::<common::StubAccount>::clone(&failed),
            Arc::new(account()),
        ]),
        opens: AtomicUsize::new(0),
    });
    let fixture = Fixture::new(Arc::<CountingQueue>::clone(&factory)).await;
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let restart = restart_account(&ctx);
    tokio::pin!(restart);
    tokio::select! { biased; _ = &mut restart => panic!("replacement unexpectedly succeeded"), () = turns() => {} }
    assert_eq!(calls(&failed), 1);
    assert_eq!(failed.closed.load(Ordering::SeqCst), 1);
    // This extends only the failed scope's declared key, beyond its local hint.
    fixture.hold(40);
    step(20).await;
    tokio::select! { biased; _ = &mut restart => panic!("lost failed replacement declaration"), () = turns() => {} }
    assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
    step(20).await;
    assert!(restart.await);
    assert_eq!(factory.opens.load(Ordering::SeqCst), 3);
}

async fn writer_boundary_case(stored_inventory: bool, exhaust: bool) {
    let mut account = account();
    account.resume_inventory = stored_inventory;
    let account = Arc::new(account);
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::clone(
        &account,
    )])))
    .await;
    let (writer_tx, mut writer_rx) = mpsc::channel(8);
    let writer = WriterHandle::new(writer_tx);
    let ctx = fixture.ctx(&writer);
    let mut pacing = RepairPacing::default();
    if exhaust {
        fixture.hold(100);
    }
    let establish = run_establish(
        &fixture.id,
        account.as_ref(),
        SCOPE,
        Arc::clone(ctx.cursors),
        &writer,
        Arc::clone(ctx.delivery),
        Some(ctx.control),
        Arc::clone(ctx.coverage),
        false,
        ctx.shutdown,
        None,
        &ctx,
        &mut pacing,
    );
    tokio::pin!(establish);
    if exhaust {
        tokio::select! { biased; _ = &mut establish => panic!("ignored initial hold"), () = turns() => {} }
        step(100).await;
    }
    let request = tokio::select! {
        biased;
        _ = &mut establish => panic!("establishment must await writer response"),
        request = writer_rx.recv() => request.expect("cursor read"),
    };
    let WriterRequest::GetChangeCursor { done, .. } = request else {
        panic!("cursor read")
    };
    fixture.hold(if exhaust { 250 } else { 20 });
    let stored = stored_inventory.then(|| common::cursor_for(&SCOPE, b"inventory"));
    done.send(Ok(stored)).expect("answer cursor read");
    tokio::select! { biased; _ = &mut establish => panic!("writer response bypassed new hold"), () = turns() => {} }
    assert_eq!(calls(&account), 0);
    assert_eq!(account.inventory_calls.load(Ordering::SeqCst), 0);
    if exhaust {
        step(200).await;
        let result = establish.await;
        assert!(
            matches!(result, Err(Error::EstablishCursorFailed(ref reason)) if reason == "recovery wait budget exhausted")
        );
        assert_eq!(calls(&account), 0);
        assert_eq!(account.inventory_calls.load(Ordering::SeqCst), 0);
    } else {
        step(19).await;
        tokio::select! { biased; _ = &mut establish => panic!("hold ended early"), () = turns() => {} }
        assert_eq!(calls(&account), 0);
        assert_eq!(account.inventory_calls.load(Ordering::SeqCst), 0);
        step(1).await;
        assert!(establish.await.is_ok());
        assert_eq!(calls(&account), usize::from(!stored_inventory));
        assert_eq!(
            account.inventory_calls.load(Ordering::SeqCst),
            usize::from(stored_inventory)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn round1_writer_response_rechecks_before_initial_establishment() {
    writer_boundary_case(false, false).await;
}
#[tokio::test(start_paused = true)]
async fn round1_writer_response_rechecks_before_inventory_resume() {
    writer_boundary_case(true, false).await;
}
#[tokio::test(start_paused = true)]
async fn round1_writer_response_keeps_cumulative_wait_allowance() {
    writer_boundary_case(false, true).await;
}

async fn lock_boundary_case(exhaust: bool) {
    use bifrost_types::Control;
    let factory = Arc::new(CountingQueue {
        inner: common::StubFactory::queue(vec![Arc::new(account()), Arc::new(account())]),
        opens: AtomicUsize::new(0),
    });
    let fixture = Fixture::new(Arc::<CountingQueue>::clone(&factory)).await;
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let lock = ctx.reopen_lock.lock().await;
    let restart = restart_account(&ctx);
    tokio::pin!(restart);
    tokio::select! { biased; _ = &mut restart => panic!("restart must queue on lock"), () = turns() => {} }
    assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
    ctx.throttles.lock().expect("bucket").record(
        ThrottleKey::Account(fixture.id.clone()),
        tokio::time::Instant::now() + Duration::from_secs(if exhaust { 600 } else { 20 }),
    );
    drop(lock);
    tokio::select! { biased; _ = &mut restart => panic!("lock acquisition bypassed hold"), () = turns() => {} }
    assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
    assert!(
        ctx.reopen_lock.try_lock().is_ok(),
        "throttle wait retained reopen lock"
    );
    // A pause must see quiescence while the restart waits, proving the
    // registration was released as well as the lock.
    tokio::select! {
        biased;
        result = ctx.control.pause() => { result.expect("pause reaches quiescence"); }
        () = turns() => panic!("throttle wait retained activity"),
    }
    ctx.control.resume();
    if exhaust {
        step(300).await;
        assert!(!restart.await);
        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
    } else {
        step(19).await;
        tokio::select! { biased; _ = &mut restart => panic!("opened before hold expired"), () = turns() => {} }
        assert_eq!(factory.opens.load(Ordering::SeqCst), 1);
        step(1).await;
        assert!(restart.await);
        assert_eq!(factory.opens.load(Ordering::SeqCst), 2);
    }
}
#[tokio::test(start_paused = true)]
async fn round1_restart_rechecks_after_lock_and_releases_admission() {
    lock_boundary_case(false).await;
}
#[tokio::test(start_paused = true)]
async fn round1_restart_lock_hold_obeys_wait_allowance() {
    lock_boundary_case(true).await;
}

fn teardown_throttle(seconds: u64, budget: Option<&str>) -> AccountError {
    let builder = bifrost_types::AccountErrorBuilder::new(
        bifrost_types::AccountErrorKind::Server(ServerErrorKind::RateLimited),
        Cause::Server(ServerCause::RateLimited {
            retry_hint: Some(bifrost_types::RetryHint::After(Duration::from_secs(
                seconds,
            ))),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .throttle_scope(ThrottleScope::CurrentOperation);
    let builder = if let Some(budget) = budget {
        builder.throttle_budget(ThrottleBudget::new(budget))
    } else {
        builder
    };
    builder.try_build().expect("teardown throttle")
}
fn subscription(handle: &str, unconfirmed: bool) -> RegisteredSubscription {
    RegisteredSubscription {
        handle: bifrost_types::SubscriptionHandle(handle.into()),
        scopes: vec![],
        teardown_unconfirmed: unconfirmed,
        desired: !unconfirmed,
        torn_down: false,
    }
}

#[tokio::test(start_paused = true)]
async fn round1_teardown_records_every_refusal_before_next_call() {
    let bucket = Arc::new(std::sync::Mutex::new(
        None::<Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>>,
    ));
    let observed_bucket = Arc::clone(&bucket);
    let mut initial = account();
    initial.unsubscribe_error_hook = Some(Arc::new(move |handle| {
        if handle.0 == "third" || handle.0 == "fourth" {
            let preceding = if handle.0 == "third" {
                "second"
            } else {
                "third"
            };
            let bucket = observed_bucket.lock().expect("bucket handle");
            let wait = crate::recovery::throttle_wait(
                bucket.as_ref().expect("installed bucket"),
                &AccountId("peer".into()),
                &[ThrottleKey::Budget(ThrottleBudget::new(preceding))],
            );
            assert!(
                wait.is_some(),
                "preceding teardown failure was not recorded immediately"
            );
        }
        Some(match handle.0.as_str() {
            "first" => common::push_teardown_refused(),
            "second" => teardown_throttle(20, Some("second")),
            "third" => teardown_throttle(40, Some("third")),
            _ => teardown_throttle(5, None),
        })
    }));
    let factory = Arc::new(common::StubFactory::queue(vec![
        Arc::new(initial),
        Arc::new(account()),
    ]));
    let fixture = Fixture::new(factory).await;
    *bucket.lock().expect("install bucket") = Some(Arc::clone(&fixture.slot.throttles));
    fixture
        .engine
        .subscriptions
        .replace(
            fixture.id.clone(),
            vec![
                subscription("first", false),
                subscription("second", false),
                subscription("third", true),
                subscription("fourth", true),
            ],
            &fixture.slot.shutdown,
        )
        .expect("install subscriptions");
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let (_lock, activity, next) = open_replacement(&ctx)
        .await
        .unwrap_or_else(|_| panic!("open"));
    let mut pacing = RepairPacing::default();
    let result = reattach_account_paced(&ctx, activity, next, &mut pacing).await;
    assert!(
        matches!(result, Err(Error::Account(ref error)) if error.kind() == common::push_teardown_refused().kind()),
        "the first refusal must remain the returned error"
    );
    assert_eq!(
        pacing.local_deadline,
        Some(tokio::time::Instant::now() + Duration::from_secs(40))
    );
}

#[tokio::test(start_paused = true)]
async fn round1_teardown_records_unconfirmed_refusal_even_when_swap_succeeds() {
    let mut initial = account();
    initial.unsubscribe_error_hook =
        Some(Arc::new(|_| Some(teardown_throttle(40, Some("orphan")))));
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![
        Arc::new(initial),
        Arc::new(account()),
    ])))
    .await;
    fixture
        .engine
        .subscriptions
        .replace(
            fixture.id.clone(),
            vec![subscription("orphan", true)],
            &fixture.slot.shutdown,
        )
        .expect("subscriptions");
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let (_lock, activity, next) = open_replacement(&ctx)
        .await
        .unwrap_or_else(|_| panic!("open"));
    let mut pacing = RepairPacing::default();
    assert!(
        reattach_account_paced(&ctx, activity, next, &mut pacing)
            .await
            .is_ok()
    );
    assert_eq!(
        crate::recovery::throttle_wait(
            ctx.throttles,
            &AccountId("peer".into()),
            &[ThrottleKey::Budget(ThrottleBudget::new("orphan"))]
        ),
        Some(Duration::from_secs(40))
    );
}

#[tokio::test(start_paused = true)]
async fn round1_local_deadline_keeps_longest_across_shorter_and_unthrottled_failures() {
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::new(
        account(),
    )])))
    .await;
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let mut pacing = RepairPacing::default();
    pacing.observe(&ctx, &teardown_throttle(60, None));
    let longest = pacing.local_deadline;
    pacing.observe(&ctx, &teardown_throttle(10, None));
    assert_eq!(
        pacing.local_deadline, longest,
        "shorter refusal shortened local hold"
    );
    pacing.observe(&ctx, &common::push_teardown_refused());
    assert_eq!(
        pacing.local_deadline, longest,
        "unthrottled refusal cleared local hold"
    );
}

fn peer_hold(ctx: &RecoveryContext<'_>, budget: &str) -> Option<Duration> {
    crate::recovery::throttle_wait(
        ctx.throttles,
        &AccountId("peer".into()),
        &[ThrottleKey::Budget(ThrottleBudget::new(budget))],
    )
}

#[tokio::test(start_paused = true)]
async fn round2_unwind_records_every_refusal_and_preserves_original_error() {
    type Bucket = Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>;
    let bucket = Arc::new(std::sync::OnceLock::<Bucket>::new());
    let observed_bucket = Arc::clone(&bucket);
    let immediate = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_immediate = Arc::clone(&immediate);
    let mut initial = account();
    initial.unsubscribe_error_hook = Some(Arc::new(|_| Some(common::push_teardown_refused())));
    let mut next = account();
    next.scopes = vec![SCOPE];
    next.caps.push = bifrost_types::PushCapability::InProcess;
    next.push_label = "replacement".into();
    next.unsubscribe_error_hook = Some(Arc::new(move |handle| {
        if handle.0 == "replacement-2" {
            let wait = crate::recovery::throttle_wait(
                observed_bucket.get().expect("bucket installed"),
                &AccountId("peer".into()),
                &[ThrottleKey::Budget(ThrottleBudget::new("cleanup-first"))],
            );
            observed_immediate.store(wait.is_some(), Ordering::SeqCst);
        }
        Some(match handle.0.as_str() {
            "replacement" => teardown_throttle(20, Some("cleanup-first")),
            "replacement-2" => teardown_throttle(60, None),
            _ => teardown_throttle(10, Some("cleanup-last")),
        })
    }));
    let next = Arc::new(next);
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![
        Arc::new(initial),
        Arc::clone(&next),
    ])))
    .await;
    bucket
        .set(Arc::clone(&fixture.slot.throttles))
        .expect("bucket installed once");
    let records = ["old-first", "old-second", "old-third"]
        .into_iter()
        .map(|name| {
            let mut record = subscription(name, false);
            record.scopes = vec![SCOPE];
            record
        })
        .collect();
    fixture
        .engine
        .subscriptions
        .replace(fixture.id.clone(), records, &fixture.slot.shutdown)
        .expect("subscriptions");
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let (_lock, activity, replacement) = open_replacement(&ctx)
        .await
        .unwrap_or_else(|_| panic!("open"));
    let mut pacing = RepairPacing::default();
    let result = reattach_account_paced(&ctx, activity, replacement, &mut pacing).await;
    assert!(
        matches!(result, Err(Error::Account(ref error)) if error.kind() == common::push_teardown_refused().kind()),
        "cleanup must preserve the original returned error"
    );
    assert_eq!(next.push_log.unsubscribed().len(), 3);
    assert!(
        immediate.load(Ordering::SeqCst),
        "cleanup hold must be recorded before the next unsubscribe"
    );
    assert_eq!(
        peer_hold(&ctx, "cleanup-first"),
        Some(Duration::from_secs(20))
    );
    assert_eq!(
        peer_hold(&ctx, "cleanup-last"),
        Some(Duration::from_secs(10))
    );
    assert_eq!(
        pacing.local_deadline,
        Some(tokio::time::Instant::now() + Duration::from_secs(60))
    );
}

#[tokio::test(start_paused = true)]
async fn round2_aborted_replacement_close_records_hold_without_replacing_error() {
    let original = teardown_throttle(120, Some("original-failure"));
    let mut next = account();
    *next.memberships_error.get_mut().expect("membership error") = Some(original.clone());
    next.close_error = Some(teardown_throttle(80, Some("replacement-close")));
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![
        Arc::new(account()),
        Arc::new(next),
    ])))
    .await;
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let (_lock, activity, replacement) = open_replacement(&ctx)
        .await
        .unwrap_or_else(|_| panic!("open"));
    let mut pacing = RepairPacing::default();
    let result = reattach_account_paced(&ctx, activity, replacement, &mut pacing).await;
    let Err(Error::Account(error)) = result else {
        panic!("original account error must be preserved")
    };
    assert_eq!(error.throttle_budgets(), original.throttle_budgets());
    assert_eq!(
        peer_hold(&ctx, "replacement-close"),
        Some(Duration::from_secs(80))
    );
    assert_eq!(
        pacing.local_deadline,
        Some(tokio::time::Instant::now() + Duration::from_secs(120))
    );
}

#[tokio::test(start_paused = true)]
async fn round2_old_account_close_records_hold_without_failing_committed_swap() {
    let mut initial = account();
    initial.close_error = Some(teardown_throttle(70, Some("old-close")));
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![
        Arc::new(initial),
        Arc::new(account()),
    ])))
    .await;
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let (_lock, activity, replacement) = open_replacement(&ctx)
        .await
        .unwrap_or_else(|_| panic!("open"));
    let mut pacing = RepairPacing::default();
    assert!(
        reattach_account_paced(&ctx, activity, replacement, &mut pacing)
            .await
            .is_ok()
    );
    assert_eq!(peer_hold(&ctx, "old-close"), Some(Duration::from_secs(70)));
    assert_eq!(
        pacing.local_deadline,
        Some(tokio::time::Instant::now() + Duration::from_secs(70))
    );
}

#[tokio::test(start_paused = true)]
async fn round2_scope_membership_refresh_records_hold_and_remains_best_effort() {
    let account = Arc::new(account());
    let fixture = Fixture::new(Arc::new(common::StubFactory::queue(vec![Arc::clone(
        &account,
    )])))
    .await;
    *account.memberships_error.lock().expect("membership error") =
        Some(teardown_throttle(30, Some("memberships")));
    let writer = fixture.writer();
    let ctx = fixture.ctx(&writer);
    let mut events = ctx.changes_tx.subscribe();
    re_establish_scope_with_backoff(&ctx, SCOPE, ctx.control).await;
    assert_eq!(
        calls(&account),
        1,
        "membership refresh must not retry a successful establishment"
    );
    assert!(
        ctx.cursors.snapshot(&SCOPE).is_some(),
        "successful repair must remain installed"
    );
    assert_eq!(
        peer_hold(&ctx, "memberships"),
        Some(Duration::from_secs(30))
    );
    assert!(
        events.try_recv().is_err(),
        "best-effort refresh must not terminate the scope"
    );
}
