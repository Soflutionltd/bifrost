//! A recorded throttle deadline actually defers `changes_stream`.
//!
//! The throttle bucket's mechanics are unit-pinned in `recovery.rs` - key
//! resolution, max-of-overlapping records, membership, expiry. None of that
//! proves the engine OBSERVES a deadline: the recorder and the reader could
//! both be correct and still never meet, because the reader ran on a
//! different clock from the timer that retired the wait. That is exactly what
//! was wrong until `ThrottleBucket` moved to `tokio::time::Instant`; with
//! `SystemTime` deadlines and a `tokio::time::sleep` retiring them, a run
//! under paused time re-derived the full wait after every sleep and spun
//! forever.
//!
//! Both tests are staged so that ONLY the shared bucket can explain what they
//! observe:
//!
//! - The scope under observation NEVER fails. It has no retry advice of its
//!   own, so the drive loop's local `retry_delay` sleep - the other timer in
//!   this path, and the one that could otherwise fake the result - cannot
//!   apply to it. The scope that does fail is a different one.
//! - The assertion is not "total elapsed time looks about right". It is a
//!   poll COUNT over a window many cadences wide (flat), followed by the same
//!   count RESUMING once the deadline passes. Flat alone would also be
//!   satisfied by a hang; resumption at the deadline is what makes it a
//!   deferral.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bifrost_sync::{EngineConfig, MultiplexerConfig, SyncEngine};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, AccountFactory, AccountId,
    AccountOperation, AttemptCause, Cause, Change, ChangeCursor, CursorScope, ObjectType, Provider,
    ServerCause, ServerErrorKind, SyncEvent, ThrottleScope, TransmissionState,
};

/// The scope that fails. Its terminations carry the provider deadline.
const NOISY: ObjectType = ObjectType::Email;
/// The scope under observation. It never fails; nothing local to it can
/// explain a pause in its polling.
const QUIET: ObjectType = ObjectType::Mailbox;

/// One second per poll, fixed. The adaptive cadence is pinned flat so the
/// window arithmetic below is exact: N seconds of quiet means N polls did not
/// happen, not "the interval may have backed off".
const POLL: Duration = Duration::from_secs(1);
/// The provider's Retry-After. Far longer than any cadence in play.
const RETRY_AFTER: Duration = Duration::from_secs(300);

fn throttled(scope: ThrottleScope, after: Duration, provider: Option<Provider>) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Server(ServerErrorKind::RateLimited),
        Cause::Server(ServerCause::RateLimited {
            retry_hint: Some(bifrost_types::RetryHint::After(after)),
        }),
    )
    // Unsent, so the classification is `Retry` (the poll loop's throttle
    // recorder arm) rather than the mid-flight `Reconcile` a send would get.
    .push_cause(Cause::Attempt(AttemptCause::new(TransmissionState::Unsent)))
    .operation(AccountOperation::SyncChanges)
    .throttle_scope(scope);
    if let Some(provider) = provider {
        builder = builder.provider(provider);
    }
    builder.try_build().expect("valid account error")
}

/// Counters for the two scopes' `changes_stream` calls.
#[derive(Default)]
struct PollCounts {
    noisy: AtomicUsize,
    quiet: AtomicUsize,
}

/// Build a stub whose NOISY scope terminates every drive with `error` and
/// whose QUIET scope yields an empty (immediately complete) change stream.
///
/// The noisy scope starts failing only once the quiet one has polled. Every
/// wait site re-checks the bucket after scheduler admission, so a deadline
/// recorded while the quiet scope's FIRST poll was still being admitted now
/// (correctly) holds that poll too - and the precondition below, that the
/// quiet scope was running before the deadline landed, would rest on winning
/// that race instead of on staging.
fn stub(counts: &Arc<PollCounts>, error: AccountError) -> Arc<common::StubAccount> {
    let mut account =
        common::StubAccount::new(vec![CursorScope::Type(NOISY), CursorScope::Type(QUIET)]);
    let counts = Arc::clone(counts);
    account.changes_hook = Some(Arc::new(move |cursor: &ChangeCursor| match &cursor.scope {
        CursorScope::Type(t) if *t == NOISY => {
            if counts.quiet.load(Ordering::SeqCst) == 0 {
                return Vec::<SyncEvent<Change>>::new();
            }
            counts.noisy.fetch_add(1, Ordering::SeqCst);
            vec![SyncEvent::Terminated(error.clone())]
        }
        _ => {
            counts.quiet.fetch_add(1, Ordering::SeqCst);
            Vec::<SyncEvent<Change>>::new()
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

/// Advance virtual time in cadence-sized steps until the observed scope has
/// gone quiet for three consecutive steps AND the failing scope has been
/// driven at least once. Returns the quiet scope's poll count at that point.
///
/// Three steps rather than one because a poll already in flight when the
/// deadline lands still completes; the point of the wait is to reach a
/// settled state whose base count the assertions below can trust.
async fn settle(counts: &PollCounts) -> usize {
    let mut last = counts.quiet.load(Ordering::SeqCst);
    let mut flat = 0usize;
    for _ in 0..60 {
        tokio::time::sleep(POLL).await;
        let now = counts.quiet.load(Ordering::SeqCst);
        if now == last && counts.noisy.load(Ordering::SeqCst) > 0 {
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
        "the observed scope never stopped polling: quiet={} noisy={}",
        counts.quiet.load(Ordering::SeqCst),
        counts.noisy.load(Ordering::SeqCst)
    );
}

/// One scope's provider deadline defers a SIBLING scope's `changes_stream`.
///
/// The quiet scope has never failed, so it holds no retry advice and takes no
/// local `retry_delay` sleep. The only thing in the engine that can stop it
/// polling on its 1s cadence is the account-wide throttle wait it consults
/// before each drive - reading the deadline the noisy scope's `Retry-After`
/// recorded in the shared bucket.
#[tokio::test(start_paused = true)]
async fn a_recorded_deadline_defers_a_sibling_scopes_changes_stream() {
    let account_id = AccountId("throttle-defers-sibling".to_owned());
    let counts = Arc::new(PollCounts::default());
    let engine = engine();
    let factory: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub(
        &counts,
        throttled(ThrottleScope::Account, RETRY_AFTER, None),
    )]));
    engine
        .attach(account_id.clone(), Arc::clone(&factory))
        .await
        .expect("attach succeeds");

    let base = settle(&counts).await;
    assert!(
        base > 0,
        "the quiet scope must have polled at least once before the deadline landed, \
         or this test proves nothing about a scope that was ever running"
    );

    // A window 120 cadences wide. Without the deferral this is 120 polls.
    tokio::time::sleep(POLL * 120).await;
    assert_eq!(
        counts.quiet.load(Ordering::SeqCst),
        base,
        "a 120-cadence window produced polls, so the recorded deadline deferred nothing"
    );

    // Past the deadline the scope must come BACK. Flat forever would also be
    // a deadlock; only resumption proves this was a timed deferral.
    tokio::time::sleep(RETRY_AFTER).await;
    assert!(
        counts.quiet.load(Ordering::SeqCst) > base,
        "polling never resumed after the Retry-After elapsed: the wait is not a \
         deadline, it is a stall"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// Two attached slots share a `Provider` deadline.
///
/// The sibling account records only a 1ms deadline of its own - enrollment in
/// the provider key, and nothing more; it has expired before any assertion
/// below runs. So the 300s pause its quiet scope observes cannot be its own
/// record, cannot be a local retry sleep (that scope never failed), and can
/// only be the deadline the OTHER account recorded under the shared
/// `ThrottleKey::Provider`.
#[tokio::test(start_paused = true)]
async fn two_attached_slots_share_a_provider_deadline() {
    let throttled_id = AccountId("provider-throttled".to_owned());
    let sibling_id = AccountId("provider-sibling".to_owned());
    let engine = engine();

    let sibling_counts = Arc::new(PollCounts::default());
    let sibling: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub(
        &sibling_counts,
        // Enrollment is lazy: an account joins a provider key only when its
        // own error stream names the provider. One millisecond is enough to
        // enroll and short enough that it can never be mistaken for the wait
        // the assertions observe.
        throttled(
            ThrottleScope::Provider,
            Duration::from_millis(1),
            Some(Provider::Microsoft),
        ),
    )]));
    engine
        .attach(sibling_id.clone(), sibling)
        .await
        .expect("sibling attach succeeds");

    // Let the sibling enroll before the long deadline is recorded. (It would
    // also see a deadline recorded first - the membership index is consulted
    // at read time - but staging it this way keeps the two facts separate.)
    tokio::time::sleep(POLL * 3).await;
    assert!(
        sibling_counts.noisy.load(Ordering::SeqCst) > 0,
        "the sibling must have failed once, or it is not enrolled in the provider key"
    );

    let loud_counts = Arc::new(PollCounts::default());
    let loud: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub(
        &loud_counts,
        throttled(
            ThrottleScope::Provider,
            RETRY_AFTER,
            Some(Provider::Microsoft),
        ),
    )]));
    engine
        .attach(throttled_id.clone(), loud)
        .await
        .expect("throttled attach succeeds");

    let base = settle(&sibling_counts).await;
    assert!(
        base > 0,
        "the sibling's quiet scope must have been polling before the other \
         account's deadline landed"
    );

    tokio::time::sleep(POLL * 120).await;
    assert_eq!(
        sibling_counts.quiet.load(Ordering::SeqCst),
        base,
        "one account's provider Retry-After did not pause its sibling"
    );

    tokio::time::sleep(RETRY_AFTER).await;
    assert!(
        sibling_counts.quiet.load(Ordering::SeqCst) > base,
        "the sibling never resumed: a shared provider deadline must expire, not stall"
    );

    engine.detach(&throttled_id).await.expect("detach succeeds");
    engine.detach(&sibling_id).await.expect("detach succeeds");
}
