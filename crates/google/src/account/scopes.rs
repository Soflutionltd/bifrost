use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bifrost_types::{
    AccountStream, Batch, CursorScope, LabelId, MembershipScope, PageBoundary, ScopeLifecycle,
    ScopeLifecycleEvent, SyncEvent,
};
use futures::{StreamExt, stream};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;
use crate::types::GmailLabel;

use super::error;

const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const SCOPE_CACHE_STALE_AFTER: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub(crate) struct ScopeSnapshot {
    pub(crate) labels: Vec<GmailLabel>,
    /// When `list_labels` last succeeded, or `None` for a cache that has
    /// never been populated.
    ///
    /// This is an `Option` rather than a bare `Instant` so that the
    /// never-populated state is representable. Stamping `Instant::now()`
    /// on an empty snapshot made `is_stale()` report FRESH for the first
    /// five minutes after `open()`, so the first flag mutation of a
    /// session translated against an empty vocabulary and silently
    /// no-opped. Keying staleness on `labels.is_empty()` instead would
    /// fix that but re-fetch forever for any account whose label list is
    /// legitimately empty; "was it ever fetched" is the actual question.
    pub(crate) fetched_at: Option<Instant>,
}

impl ScopeSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            labels: Vec::new(),
            fetched_at: None,
        }
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.fetched_at
            .is_none_or(|fetched_at| fetched_at.elapsed() > SCOPE_CACHE_STALE_AFTER)
    }
}

pub(crate) struct ScopeCacheState {
    snapshot: RwLock<ScopeSnapshot>,
    refresh: Mutex<()>,
}

impl ScopeCacheState {
    pub(crate) fn new(snapshot: ScopeSnapshot) -> Self {
        Self {
            snapshot: RwLock::new(snapshot),
            refresh: Mutex::new(()),
        }
    }
}

pub(crate) type ScopeCache = Arc<ScopeCacheState>;

#[derive(Default)]
pub(crate) struct LifecycleBaselineState {
    last_emitted: Option<ScopeSnapshot>,
    pending: VecDeque<ScopeLifecycle>,
    // Order overlapping replacement streams' fetches as well as their commits.
    poll: Arc<Mutex<()>>,
}

pub(crate) type LifecycleBaseline = Arc<std::sync::Mutex<LifecycleBaselineState>>;

struct LifecycleState {
    client: Arc<GmailClient>,
    shutdown: CancellationToken,
    // This state is account-owned and independent of the mutable vocabulary cache.
    baseline: LifecycleBaseline,
    ended: bool,
    next_delay: Option<Duration>,
}

impl LifecycleState {
    /// Waits out the inter-poll delay, returning `false` if the stream
    /// was cancelled while waiting.
    async fn await_next_poll(&mut self) -> bool {
        let Some(delay) = self.next_delay else {
            // First attempt of the stream's life: poll immediately so a
            // consumer gets its baseline without a cold-start stall.
            self.next_delay = Some(LIFECYCLE_POLL_INTERVAL);
            return true;
        };
        tokio::select! {
            () = self.shutdown.cancelled() => false,
            () = tokio::time::sleep(delay) => true,
        }
    }

    fn note_poll_succeeded(&mut self) {
        self.next_delay = Some(LIFECYCLE_POLL_INTERVAL);
    }
}

pub(crate) fn discover_cursor_scopes() -> AccountStream<SyncEvent<CursorScope>> {
    let batch = Batch {
        items: vec![CursorScope::Account],
        page_boundary: PageBoundary::Final,
        server_latency: Duration::ZERO,
        // Synthetic: Gmail has exactly one cursor scope and it is a
        // constant, so this batch performs no request at all.
        bytes_in: 0,
        checkpoint: None,
    };
    Box::pin(stream::iter([
        SyncEvent::Batch(batch),
        SyncEvent::Done(None),
    ]))
}

pub(crate) fn discover_memberships(
    client: Arc<GmailClient>,
    cache: ScopeCache,
) -> AccountStream<SyncEvent<MembershipScope>> {
    // The Account adapter wraps this stream with its shutdown token, so close
    // drops an in-flight refresh as well as an idle stream.
    Box::pin(
        stream::once(async move {
            let started = Instant::now();
            // A fresh snapshot answers discovery without a request, so its
            // request-local byte tally is legitimately zero.
            let (client, tally) = client.metered();
            match scope_snapshot_or_refresh(&client, &cache).await {
                Ok(snapshot) => {
                    let items = snapshot
                        .labels
                        .iter()
                        .map(|label| MembershipScope::Label(LabelId(label.id.clone())))
                        .collect();
                    // Success: emit the batch then Done.
                    let batch = SyncEvent::Batch(Batch {
                        items,
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: tally.take(),
                        checkpoint: None,
                    });
                    vec![batch, SyncEvent::Done(None)]
                }
                Err(error) => {
                    let account_error = error::into_account_error(
                        error,
                        error::GmailErrorContext::containers_list(),
                    );
                    vec![SyncEvent::Terminated(account_error)]
                }
            }
        })
        .flat_map(stream::iter),
    )
}

#[cfg(test)]
pub(crate) fn scope_lifecycle_stream(
    client: Arc<GmailClient>,
    shutdown: CancellationToken,
) -> AccountStream<ScopeLifecycleEvent> {
    Box::pin(
        scope_lifecycle_stream_with_baseline(client, shutdown, Default::default())
            .filter(|event| futures::future::ready(!matches!(event, ScopeLifecycleEvent::Polled))),
    )
}

pub(crate) fn scope_lifecycle_stream_with_baseline(
    client: Arc<GmailClient>,
    shutdown: CancellationToken,
    baseline: LifecycleBaseline,
) -> AccountStream<ScopeLifecycleEvent> {
    let state = LifecycleState {
        client,
        shutdown,
        baseline,
        ended: false,
        next_delay: None,
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        // Do not drain a queued diff after account closure.
        if state.ended || state.shutdown.is_cancelled() {
            return None;
        }
        let event = state
            .baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .pop_front();
        if let Some(event) = event {
            return Some((ScopeLifecycleEvent::Lifecycle(event), state));
        }
        if !state.await_next_poll().await {
            return None;
        }

        let poll = Arc::clone(
            &state
                .baseline
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .poll,
        );
        let _poll = tokio::select! {
            biased;
            () = state.shutdown.cancelled() => return None,
            guard = poll.lock() => guard,
        };
        // A previous reader may have queued a diff while this one waited.
        let event = state
            .baseline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .pop_front();
        if let Some(event) = event {
            return Some((ScopeLifecycleEvent::Lifecycle(event), state));
        }
        let snapshot = tokio::select! {
            biased;
            () = state.shutdown.cancelled() => return None,
            result = fetch_scope_snapshot(&state.client) => result,
        };
        match snapshot {
            Ok(new) => {
                state.note_poll_succeeded();
                let mut baseline = state
                    .baseline
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                baseline.pending =
                    record_lifecycle_snapshot(&mut baseline.last_emitted, new).into();
                drop(baseline);
                Some((ScopeLifecycleEvent::Polled, state))
            }
            Err(error) => {
                let acct = super::error::into_account_error(
                    error,
                    super::error::GmailErrorContext::containers_list(),
                );
                // The engine owns retry, throttle holds and reconnect cadence.
                // Baseline AND undelivered events survive replacement streams.
                state.ended = true;
                Some((ScopeLifecycleEvent::Terminated(acct), state))
            }
        }
    }))
}

fn record_lifecycle_snapshot(
    last_emitted: &mut Option<ScopeSnapshot>,
    new: ScopeSnapshot,
) -> Vec<ScopeLifecycle> {
    last_emitted
        .replace(new.clone())
        .map_or_else(Vec::new, |old| diff_snapshots(&old, &new))
}

/// The label vocabulary every flag-canonicalizing call site must go
/// through.
///
/// This helper checks cache age, not whether a caller-supplied label id is
/// present. A translator that finds an unresolved `$gmail-label:` id in a
/// fresh snapshot must force one serialized refresh with
/// `refresh_scope_snapshot` and retry translation before declaring it
/// unsupported.
///
/// Never reach for `snapshot(cache)` directly for this: the cache starts
/// empty, and canonicalizing against an empty vocabulary is silently wrong
/// in both directions. Reading, `canonical_flags` falls back to the id in
/// the name slot and destabilises `Fingerprint.flags_hash`; writing, every
/// `$gmail-label:` flag becomes unsupported and the batch reports
/// `Skipped`. A refresh failure with nothing cached therefore propagates
/// as an error - a stale-but-populated vocabulary is degraded, an empty
/// one is unusable.
///
/// The refresh is single-flight under `ScopeCache::refresh`, with staleness
/// re-checked after the lock, and that is load-bearing rather than tidy:
/// hydration runs 32 concurrent `users.messages.get` calls, so without it a
/// single batch issued 32 `labels.list` refreshes against the same stale
/// snapshot. Do not replace the lock with a bare staleness test.
pub(crate) async fn labels_for_flags(
    client: &Arc<GmailClient>,
    cache: &ScopeCache,
) -> crate::Result<Vec<GmailLabel>> {
    let current = snapshot(cache);
    if !current.is_stale() {
        return Ok(current.labels);
    }
    let _refresh = cache.refresh.lock().await;
    let current = snapshot(cache);
    if !current.is_stale() {
        return Ok(current.labels);
    }
    match refresh_scope_snapshot_locked(client, cache).await {
        Ok(snapshot) => Ok(snapshot.labels),
        Err(error) if current.fetched_at.is_some() => {
            tracing::warn!("gmail label refresh failed during flag translation: {error}");
            Ok(current.labels)
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn snapshot(cache: &ScopeCache) -> ScopeSnapshot {
    cache
        .snapshot
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| {
            // A poisoned lock means a panic happened mid-write. Degrading to
            // an empty snapshot (which reads as "never fetched" and triggers
            // a refetch) is the right recovery, but it must not be silent.
            tracing::warn!("scope snapshot lock poisoned; treating cache as never fetched");
            ScopeSnapshot::empty()
        })
}

/// Return the current vocabulary, refreshing only when the shared snapshot is
/// stale. The second staleness check is inside the single-flight lock so
/// concurrent readers consume the result of one `labels.list` request.
pub(crate) async fn scope_snapshot_or_refresh(
    client: &GmailClient,
    cache: &ScopeCache,
) -> crate::Result<ScopeSnapshot> {
    let current = snapshot(cache);
    if !current.is_stale() {
        return Ok(current);
    }

    let _refresh = cache.refresh.lock().await;
    let current = snapshot(cache);
    if !current.is_stale() {
        return Ok(current);
    }

    refresh_scope_snapshot_locked(client, cache).await
}

/// Fetch an authoritative snapshot even when the cache is still fresh.
/// Containers-list uses this to report the latest server view. The refresh
/// lock serializes it with every other cache writer and prevents an older
/// fetch from landing after a newer one.
pub(crate) async fn refresh_scope_snapshot(
    client: &GmailClient,
    cache: &ScopeCache,
) -> crate::Result<ScopeSnapshot> {
    let _refresh = cache.refresh.lock().await;
    refresh_scope_snapshot_locked(client, cache).await
}

/// Mark the vocabulary stale after a label mutation. Keep the previous labels
/// for diagnostics and fallback decisions, but require the next vocabulary
/// consumer to refresh them. Sharing the refresh lock orders this invalidation
/// against an already-running snapshot fetch.
pub(crate) async fn invalidate_scope_snapshot(cache: &ScopeCache) {
    let _refresh = cache.refresh.lock().await;
    match cache.snapshot.write() {
        Ok(mut guard) => guard.fetched_at = None,
        Err(_) => tracing::warn!("scope snapshot lock poisoned; could not invalidate cache"),
    }
}

async fn refresh_scope_snapshot_locked(
    client: &GmailClient,
    cache: &ScopeCache,
) -> crate::Result<ScopeSnapshot> {
    let snapshot = fetch_scope_snapshot(client).await?;
    if let Ok(mut guard) = cache.snapshot.write() {
        *guard = snapshot.clone();
    } else {
        tracing::warn!("scope snapshot lock poisoned; refreshed labels were not cached");
    }
    Ok(snapshot)
}

async fn fetch_scope_snapshot(client: &GmailClient) -> crate::Result<ScopeSnapshot> {
    Ok(ScopeSnapshot {
        labels: client.list_labels().await?,
        fetched_at: Some(Instant::now()),
    })
}

fn diff_snapshots(old: &ScopeSnapshot, new: &ScopeSnapshot) -> Vec<ScopeLifecycle> {
    let old_by_id = old
        .labels
        .iter()
        .map(|label| (label.id.as_str(), label.name.as_str()))
        .collect::<HashMap<_, _>>();
    let new_by_id = new
        .labels
        .iter()
        .map(|label| (label.id.as_str(), label.name.as_str()))
        .collect::<HashMap<_, _>>();

    let mut events = Vec::new();
    for label in &new.labels {
        if !old_by_id.contains_key(label.id.as_str()) {
            events.push(ScopeLifecycle::Created(MembershipScope::Label(LabelId(
                label.id.clone(),
            ))));
        } else if old_by_id.get(label.id.as_str()).is_some_and(|old_name| {
            *old_name != label.name.as_str()
                && label
                    .label_type
                    .as_deref()
                    .is_none_or(|label_type| label_type != "system")
        }) {
            events.push(ScopeLifecycle::Renamed {
                old: MembershipScope::Label(LabelId(label.id.clone())),
                new: MembershipScope::Label(LabelId(label.id.clone())),
                old_name: old_by_id[label.id.as_str()].to_string(),
                new_name: label.name.clone(),
            });
        }
    }
    for label in &old.labels {
        if !new_by_id.contains_key(label.id.as_str()) {
            events.push(ScopeLifecycle::Deleted(MembershipScope::Label(LabelId(
                label.id.clone(),
            ))));
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use bytes::Bytes;
    use reqwest::StatusCode;
    use serde_json::json;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_successful_label_poll_yields_polled_after_its_commit() {
        let (client, _) = scripted_client(vec![ok_json(
            json!({"labels": [{"id":"one", "name":"One"}]}),
        )]);
        let baseline = LifecycleBaseline::default();
        let mut stream = scope_lifecycle_stream_with_baseline(
            client,
            CancellationToken::new(),
            Arc::clone(&baseline),
        );
        assert!(matches!(
            stream.next().await,
            Some(ScopeLifecycleEvent::Polled)
        ));
        assert!(baseline.lock().expect("baseline").last_emitted.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_label_poll_yields_no_polled() {
        let (client, _) = scripted_client(vec![unavailable()]);
        let mut stream = scope_lifecycle_stream_with_baseline(
            client,
            CancellationToken::new(),
            Default::default(),
        );
        assert!(matches!(
            stream.next().await,
            Some(ScopeLifecycleEvent::Terminated(_))
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn project_hold_label_lifecycle_makes_no_wire_calls() {
        let (client, script) = scripted_client(Vec::new());
        let client = Arc::new(client.with_quota_project("123".into()));
        let wall = std::time::SystemTime::UNIX_EPOCH;
        client
            .account_net()
            .shared_hold("gmail-project|123")
            .record(
                bifrost_net::HoldDeadline::new(
                    wall + Duration::from_secs(10_000),
                    wall,
                    tokio::time::Instant::now(),
                ),
                bifrost_net::HoldKind::QuotaExhausted,
            );
        let shutdown = CancellationToken::new();
        let mut lifecycle = scope_lifecycle_stream(client, shutdown.clone());
        assert!(matches!(
            lifecycle.next().await,
            Some(ScopeLifecycleEvent::Terminated(_))
        ));
        assert!(script.requests().is_empty());
        shutdown.cancel();
        assert!(lifecycle.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_during_label_lifecycle_request_drops_the_request() {
        let (client, script) = scripted_client(vec![Canned::Pending]);
        let shutdown = CancellationToken::new();
        let mut lifecycle = scope_lifecycle_stream(client, shutdown.clone());
        let next = tokio::spawn(async move { lifecycle.next().await });

        while script.requests().is_empty() {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();

        assert!(next.await.expect("lifecycle task joins").is_none());
        assert_eq!(script.requests().len(), 1);
    }

    fn ok_json(value: serde_json::Value) -> Canned {
        Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(serde_json::to_vec(&value).expect("fixture serializes")),
        }
    }

    fn scripted_client(steps: Vec<Canned>) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
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

    fn unavailable() -> Canned {
        Canned::Response {
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    fn label(id: &str, name: &str, label_type: &str) -> GmailLabel {
        GmailLabel {
            id: id.to_owned(),
            name: name.to_owned(),
            label_type: Some(label_type.to_owned()),
            color: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_stale_vocabulary_reads_share_one_refresh() {
        let (client, script) = scripted_client(vec![Canned::Pending, Canned::Pending]);
        let cache: ScopeCache = Arc::new(ScopeCacheState::new(ScopeSnapshot::empty()));
        let first_client = Arc::clone(&client);
        let first_cache = Arc::clone(&cache);
        let first =
            tokio::spawn(async move { labels_for_flags(&first_client, &first_cache).await });
        while script.requests().is_empty() {
            tokio::task::yield_now().await;
        }
        let second = tokio::spawn(async move { labels_for_flags(&client, &cache).await });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            script.requests().len(),
            1,
            "a waiter on the stale cache must not issue its own labels.list"
        );
        first.abort();
        second.abort();
    }

    #[test]
    fn first_successful_lifecycle_snapshot_seeds_without_created_events() {
        let mut last_emitted = None;
        let new = ScopeSnapshot {
            labels: vec![label("Label_1", "One", "user")],
            fetched_at: Some(Instant::now()),
        };
        assert!(record_lifecycle_snapshot(&mut last_emitted, new).is_empty());
    }

    #[test]
    fn populated_lifecycle_snapshot_still_emits_real_changes() {
        let mut last_emitted = Some(ScopeSnapshot {
            labels: vec![label("Label_1", "One", "user")],
            fetched_at: Some(Instant::now()),
        });
        let new = ScopeSnapshot {
            labels: vec![
                label("Label_1", "One", "user"),
                label("Label_2", "Two", "user"),
            ],
            fetched_at: Some(Instant::now()),
        };
        assert!(matches!(
            record_lifecycle_snapshot(&mut last_emitted, new).as_slice(),
            [ScopeLifecycle::Created(MembershipScope::Label(LabelId(id)))]
                if id == "Label_2"
        ));
    }

    /// Membership discovery shares the vocabulary cache with flag readers,
    /// while lifecycle diffing owns an independent baseline. A discovery
    /// between polls must not move that baseline forward and swallow a
    /// creation event.
    #[tokio::test(start_paused = true)]
    async fn a_shared_cache_refresh_between_polls_does_not_swallow_creation() {
        let (client, script) = scripted_client(vec![
            ok_json(json!({
                "labels": [{"id": "Label_1", "name": "One", "type": "user"}]
            })),
            ok_json(json!({
                "labels": [
                    {"id": "Label_1", "name": "One", "type": "user"},
                    {"id": "Label_2", "name": "Two", "type": "user"}
                ]
            })),
            ok_json(json!({
                "labels": [
                    {"id": "Label_1", "name": "One", "type": "user"},
                    {"id": "Label_2", "name": "Two", "type": "user"}
                ]
            })),
        ]);
        let cache: ScopeCache = Arc::new(ScopeCacheState::new(ScopeSnapshot::empty()));
        let shutdown = CancellationToken::new();
        let mut lifecycle = scope_lifecycle_stream(Arc::clone(&client), shutdown);
        let next_event = tokio::spawn(async move { lifecycle.next().await });
        while script.requests().is_empty() {
            tokio::task::yield_now().await;
        }

        let discovered = discover_memberships(Arc::clone(&client), Arc::clone(&cache))
            .collect::<Vec<_>>()
            .await;
        assert!(matches!(discovered.first(), Some(SyncEvent::Batch(_))));
        assert_eq!(
            snapshot(&cache).labels.len(),
            2,
            "the interfering writer must have advanced the shared cache",
        );

        tokio::time::advance(LIFECYCLE_POLL_INTERVAL).await;
        assert!(matches!(
            next_event.await.expect("lifecycle task joins"),
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Label(LabelId(id))
            ))) if id == "Label_2"
        ));
    }

    #[tokio::test]
    async fn fresh_membership_cache_hit_performs_no_request() {
        let (client, script) = scripted_client(Vec::new());
        let cache: ScopeCache = Arc::new(ScopeCacheState::new(snapshot_of(vec![label(
            "Label_1", "Work", "user",
        )])));

        let events = discover_memberships(client, cache)
            .collect::<Vec<_>>()
            .await;

        assert!(script.requests().is_empty());
        assert!(matches!(
            events.as_slice(),
            [
                SyncEvent::Batch(Batch {
                    items,
                    bytes_in: 0,
                    ..
                }),
                SyncEvent::Done(None)
            ] if matches!(items.as_slice(), [MembershipScope::Label(LabelId(id))] if id == "Label_1")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_snapshot_refreshes_share_the_refresh_lock() {
        let (client, script) = scripted_client(vec![Canned::Pending, Canned::Pending]);
        let cache: ScopeCache = Arc::new(ScopeCacheState::new(ScopeSnapshot::empty()));
        let first_client = Arc::clone(&client);
        let first_cache = Arc::clone(&cache);
        let first =
            tokio::spawn(async move { refresh_scope_snapshot(&first_client, &first_cache).await });
        while script.requests().is_empty() {
            tokio::task::yield_now().await;
        }

        let second_client = Arc::clone(&client);
        let second_cache = Arc::clone(&cache);
        let second =
            tokio::spawn(
                async move { refresh_scope_snapshot(&second_client, &second_cache).await },
            );
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            script.requests().len(),
            1,
            "a second writer must wait for the first snapshot to finish",
        );
        first.abort();
        second.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn initial_poll_failure_terminates_once_without_local_retry() {
        let (client, script) = scripted_client(vec![unavailable()]);
        let mut lifecycle = scope_lifecycle_stream(client, CancellationToken::new());
        assert!(matches!(
            lifecycle.next().await,
            Some(ScopeLifecycleEvent::Terminated(_))
        ));
        assert!(lifecycle.next().await.is_none());
        assert_eq!(script.requests().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_preserves_baseline_and_undelivered_events() {
        let (client, script) = scripted_client(vec![
            ok_json(json!({"labels": [{"id":"one", "name":"One"}]})),
            unavailable(),
            ok_json(json!({"labels": [
                {"id":"one", "name":"One"},
                {"id":"two", "name":"Two"},
                {"id":"three", "name":"Three"}
            ]})),
        ]);
        let baseline = LifecycleBaseline::default();
        let make_stream = || {
            Box::pin(
                scope_lifecycle_stream_with_baseline(
                    Arc::clone(&client),
                    CancellationToken::new(),
                    Arc::clone(&baseline),
                )
                .filter(|event| {
                    futures::future::ready(!matches!(event, ScopeLifecycleEvent::Polled))
                }),
            )
        };
        let mut first = make_stream();
        assert!(matches!(
            first.next().await,
            Some(ScopeLifecycleEvent::Terminated(_))
        ));
        assert!(first.next().await.is_none());
        drop(first);
        let mut second = make_stream();
        assert!(matches!(second.next().await,
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Label(LabelId(id))))) if id == "two"));
        drop(second);
        let mut third = make_stream();
        assert!(matches!(third.next().await,
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Label(LabelId(id))))) if id == "three"));
        assert_eq!(script.requests().len(), 3);
    }

    fn snapshot_of(labels: Vec<GmailLabel>) -> ScopeSnapshot {
        ScopeSnapshot {
            labels,
            fetched_at: Some(Instant::now()),
        }
    }

    fn label_id(scope: &MembershipScope) -> &str {
        match scope {
            MembershipScope::Label(LabelId(id)) => id.as_str(),
            _ => panic!("gmail lifecycle events are always label-scoped"),
        }
    }

    #[test]
    fn identical_snapshots_produce_no_events() {
        let snap = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let again = snapshot_of(vec![label("Label_1", "Work", "user")]);
        assert!(diff_snapshots(&snap, &again).is_empty());
    }

    #[test]
    fn a_new_label_is_created_and_a_vanished_one_is_deleted() {
        let old = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let new = snapshot_of(vec![label("Label_2", "Home", "user")]);
        let events = diff_snapshots(&old, &new);
        assert_eq!(events.len(), 2);
        match &events[0] {
            ScopeLifecycle::Created(scope) => assert_eq!(label_id(scope), "Label_2"),
            other => panic!("expected Created first, got {other:?}"),
        }
        match &events[1] {
            ScopeLifecycle::Deleted(scope) => assert_eq!(label_id(scope), "Label_1"),
            other => panic!("expected Deleted second, got {other:?}"),
        }
    }

    /// Gmail keeps the label id across a rename, so a name change on a
    /// user label surfaces as `Renamed`.
    #[test]
    fn a_user_label_name_change_is_a_rename() {
        let old = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let new = snapshot_of(vec![label("Label_1", "Work Stuff", "user")]);
        let events = diff_snapshots(&old, &new);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ScopeLifecycle::Renamed { .. }));
    }

    /// Gmail's stable label id makes Renamed an invalidation signal.
    /// The consumer re-reads container metadata to obtain the new name.
    #[test]
    fn rename_events_carry_the_same_scope_on_both_sides() {
        let old = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let new = snapshot_of(vec![label("Label_1", "Renamed", "user")]);
        match &diff_snapshots(&old, &new)[0] {
            ScopeLifecycle::Renamed {
                old,
                new,
                old_name,
                new_name,
            } => {
                assert_eq!(label_id(old), label_id(new));
                assert_eq!(label_id(old), "Label_1");
                assert_eq!(old_name, "Work");
                assert_eq!(new_name, "Renamed");
            }
            other => panic!("expected Renamed, got {other:?}"),
        }
    }

    /// System labels are localised by Gmail (the `INBOX` label's `name`
    /// follows the account language), so a locale flip must not be
    /// reported as a user-visible rename.
    #[test]
    fn system_label_name_changes_are_not_renames() {
        let old = snapshot_of(vec![label("INBOX", "INBOX", "system")]);
        let new = snapshot_of(vec![label("INBOX", "Innboks", "system")]);
        assert!(diff_snapshots(&old, &new).is_empty());
    }

    /// A label whose `type` Gmail omitted is treated as user-shaped, so
    /// its rename still surfaces. The alternative (swallowing it) would
    /// silently drop renames on any future label shape.
    #[test]
    fn a_label_with_no_type_is_treated_as_renameable() {
        let old = snapshot_of(vec![GmailLabel {
            id: "Label_3".to_owned(),
            name: "Old".to_owned(),
            label_type: None,
            color: None,
        }]);
        let new = snapshot_of(vec![GmailLabel {
            id: "Label_3".to_owned(),
            name: "New".to_owned(),
            label_type: None,
            color: None,
        }]);
        assert_eq!(diff_snapshots(&old, &new).len(), 1);
    }

    /// A color-only edit is not a lifecycle event; only presence and
    /// name participate in the diff.
    #[test]
    fn a_color_only_change_is_not_a_lifecycle_event() {
        let old = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let mut recolored = label("Label_1", "Work", "user");
        recolored.color = Some(crate::types::GmailLabelColor {
            background_color: Some("#000000".to_owned()),
            text_color: Some("#ffffff".to_owned()),
        });
        assert!(diff_snapshots(&old, &snapshot_of(vec![recolored])).is_empty());
    }

    #[test]
    fn diffing_against_an_empty_old_snapshot_creates_everything() {
        let new = snapshot_of(vec![
            label("Label_1", "Work", "user"),
            label("INBOX", "INBOX", "system"),
        ]);
        let events = diff_snapshots(&ScopeSnapshot::empty(), &new);
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|event| matches!(event, ScopeLifecycle::Created(_))),
        );
    }

    #[test]
    fn a_never_populated_scope_cache_is_born_stale() {
        let empty = ScopeSnapshot::empty();
        assert!(empty.labels.is_empty());
        assert!(
            empty.is_stale(),
            "the first label-dependent operation must populate the cache",
        );
    }

    #[test]
    fn a_snapshot_older_than_the_stale_window_is_stale() {
        // `Instant` is monotonic-since-boot, so back-dating can fail on
        // a machine that just booted. Skip rather than panic there.
        if let Some(back_dated) =
            Instant::now().checked_sub(SCOPE_CACHE_STALE_AFTER + Duration::from_secs(1))
        {
            let stale = ScopeSnapshot {
                labels: vec![label("Label_1", "Work", "user")],
                fetched_at: Some(back_dated),
            };
            assert!(stale.is_stale());
        }

        let fresh = ScopeSnapshot {
            labels: vec![label("Label_1", "Work", "user")],
            fetched_at: Some(Instant::now()),
        };
        assert!(!fresh.is_stale());
    }

    /// An account whose `labels.list` legitimately returns nothing must
    /// still cache that answer. Keying staleness on `labels.is_empty()`
    /// would make every flag-translating call re-fetch forever.
    #[test]
    fn a_successfully_fetched_empty_label_list_is_fresh() {
        let fetched_empty = ScopeSnapshot {
            labels: Vec::new(),
            fetched_at: Some(Instant::now()),
        };
        assert!(!fetched_empty.is_stale());
    }

    #[test]
    fn snapshot_reads_the_cache_under_the_lock() {
        let cache: ScopeCache = Arc::new(ScopeCacheState::new(snapshot_of(vec![label(
            "Label_1", "Work", "user",
        )])));
        assert_eq!(snapshot(&cache).labels.len(), 1);
    }
}
