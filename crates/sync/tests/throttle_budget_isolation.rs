//! A stamped throttle budget holds exactly the work that declares it.
//!
//! The provider case this models is Microsoft Graph, whose Outlook limits are
//! per app per MAILBOX: one mailbox's 429 must pause every scope routed to
//! that mailbox - on this account and on any other attached account that
//! reaches the same mailbox - and nothing else. Three facts are pinned at
//! once, each by a scope that can only be explained by one mechanism:
//!
//! - ISOLATION. A sibling scope on another budget keeps polling. Recording
//!   precedence is what this proves: the stamped error also carries an
//!   `Account` throttle scope, and if the engine lifted that into the account
//!   key as well, this scope would stop too.
//! - SHARING. A scope declaring the stamped budget stops polling, though it
//!   never failed and holds no retry advice of its own.
//! - FIRST-DEADLINE REACH. A second attached account declaring the same
//!   budget stops polling on the FIRST deadline the other account records.
//!   It has never failed, so it was never enrolled in anything; only a
//!   declared key looked up literally can reach it.
//!
//! As in `throttle_defers_changes.rs`, every assertion is a poll COUNT held
//! flat over a window many cadences wide and then resuming, never an elapsed
//! time another timer could satisfy.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_sync::{EngineConfig, MultiplexerConfig, SyncEngine};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountId,
    AccountOperation, AttemptCause, Cause, Change, ChangeCursor, CursorScope, ObjectType,
    ServerCause, ServerErrorKind, SyncEvent, ThrottleBudget, ThrottleKey, ThrottleScope,
    TransmissionState,
};

/// Fails once the others have polled; declares the held budget.
const NOISY: ObjectType = ObjectType::Email;
/// Never fails; declares the held budget.
const SAME_MAILBOX: ObjectType = ObjectType::Mailbox;
/// Never fails; declares a different budget.
const OTHER_MAILBOX: ObjectType = ObjectType::Contact;

const POLL: Duration = Duration::from_secs(1);
const RETRY_AFTER: Duration = Duration::from_secs(300);

fn held() -> ThrottleBudget {
    ThrottleBudget::new("test|mailbox:held@example.com")
}

fn other() -> ThrottleBudget {
    ThrottleBudget::new("test|mailbox:other@example.com")
}

/// A 429 stamped with the held mailbox's budget. It ALSO carries an
/// `Account` throttle scope, as every Graph throttle does: the precedence
/// rule is what must keep that scope out of the bucket.
fn stamped_throttle() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Server(ServerErrorKind::RateLimited),
        Cause::Server(ServerCause::RateLimited {
            retry_hint: Some(bifrost_types::RetryHint::After(RETRY_AFTER)),
        }),
    )
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(AccountOperation::SyncChanges)
    .throttle_scope(ThrottleScope::Account)
    .throttle_budget(held())
    .try_build()
    .expect("valid account error")
}

#[derive(Default)]
struct Counts {
    noisy: AtomicUsize,
    same: AtomicUsize,
    other: AtomicUsize,
}

fn keys_for(scope: &CursorScope) -> Vec<ThrottleKey> {
    match scope {
        CursorScope::Type(t) if *t == OTHER_MAILBOX => vec![ThrottleKey::Budget(other())],
        _ => vec![ThrottleKey::Budget(held())],
    }
}

/// `throttles` is false for the sibling account: its noisy scope never
/// fails, so nothing it does can record a deadline.
fn stub(counts: &Arc<Counts>, throttles: bool) -> Arc<common::StubAccount> {
    let mut account = common::StubAccount::new(vec![
        CursorScope::Type(NOISY),
        CursorScope::Type(SAME_MAILBOX),
        CursorScope::Type(OTHER_MAILBOX),
    ]);
    account.scope_keys = Some(Arc::new(keys_for));
    let counts = Arc::clone(counts);
    account.changes_hook = Some(Arc::new(move |cursor: &ChangeCursor| match &cursor.scope {
        CursorScope::Type(t) if *t == NOISY => {
            // Throttle only once the observed scopes have polled, so the
            // "they were running before the deadline" precondition rests on
            // staging rather than on winning an admission race.
            let ready =
                counts.same.load(Ordering::SeqCst) > 0 && counts.other.load(Ordering::SeqCst) > 0;
            if !throttles || !ready {
                return Vec::<SyncEvent<Change>>::new();
            }
            counts.noisy.fetch_add(1, Ordering::SeqCst);
            vec![SyncEvent::Terminated(stamped_throttle())]
        }
        CursorScope::Type(t) if *t == SAME_MAILBOX => {
            counts.same.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        }
        _ => {
            counts.other.fetch_add(1, Ordering::SeqCst);
            Vec::new()
        }
    }));
    Arc::new(account)
}

fn engine() -> Arc<SyncEngine> {
    let config = EngineConfig {
        multiplexer: MultiplexerConfig {
            poll_initial: POLL,
            poll_min: POLL,
            poll_max: POLL,
            ..MultiplexerConfig::default()
        },
        ..EngineConfig::default()
    };
    Arc::new(
        SyncEngine::builder()
            .config(config)
            .build()
            .expect("engine config is valid"),
    )
}

/// Step virtual time until the held scope has been flat for three steps and
/// the noisy scope has recorded at least once. Returns the held scope's
/// count at that point.
async fn settle(counts: &Counts, throttler: &Counts) -> usize {
    let mut last = counts.same.load(Ordering::SeqCst);
    let mut flat = 0usize;
    for _ in 0..60 {
        tokio::time::sleep(POLL).await;
        let now = counts.same.load(Ordering::SeqCst);
        if now == last && throttler.noisy.load(Ordering::SeqCst) > 0 {
            flat += 1;
            if flat == 3 {
                return now;
            }
        } else {
            flat = 0;
        }
        last = now;
    }
    panic!(
        "the held scope never stopped polling: same={} noisy={}",
        counts.same.load(Ordering::SeqCst),
        throttler.noisy.load(Ordering::SeqCst)
    );
}

#[tokio::test(start_paused = true)]
async fn a_stamped_budget_holds_exactly_the_work_that_declares_it() {
    let engine = engine();
    let loud_id = AccountId("budget-loud".to_owned());
    let sibling_id = AccountId("budget-sibling".to_owned());

    let loud = Arc::new(Counts::default());
    let sibling = Arc::new(Counts::default());
    let sibling_factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![stub(&sibling, false)]));
    engine
        .attach(sibling_id.clone(), sibling_factory)
        .await
        .expect("sibling attach succeeds");
    let loud_factory: Arc<dyn AccountFactory> =
        Arc::new(common::StubFactory::queue(vec![stub(&loud, true)]));
    engine
        .attach(loud_id.clone(), loud_factory)
        .await
        .expect("loud attach succeeds");

    let loud_base = settle(&loud, &loud).await;
    let sibling_base = settle(&sibling, &loud).await;
    assert!(
        loud_base > 0 && sibling_base > 0,
        "both held scopes ran first"
    );
    let loud_other_base = loud.other.load(Ordering::SeqCst);
    let sibling_other_base = sibling.other.load(Ordering::SeqCst);

    tokio::time::sleep(POLL * 120).await;
    assert_eq!(
        loud.same.load(Ordering::SeqCst),
        loud_base,
        "a scope declaring the held budget polled through its deadline"
    );
    assert_eq!(
        sibling.same.load(Ordering::SeqCst),
        sibling_base,
        "the sibling account never failed and was never enrolled; only a declared \
         key looked up literally reaches it, and it polled through the deadline"
    );
    assert!(
        loud.other.load(Ordering::SeqCst) >= loud_other_base + 100,
        "a scope on another budget was held: the stamped error's account scope \
         must not be recorded alongside its budget"
    );
    assert!(
        sibling.other.load(Ordering::SeqCst) >= sibling_other_base + 100,
        "the sibling's scope on another budget was held"
    );

    tokio::time::sleep(RETRY_AFTER).await;
    assert!(
        sibling.same.load(Ordering::SeqCst) > sibling_base,
        "a shared budget deadline must expire, not stall"
    );

    engine.detach(&loud_id).await.expect("detach succeeds");
    engine.detach(&sibling_id).await.expect("detach succeeds");
}
