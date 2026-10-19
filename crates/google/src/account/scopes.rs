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
/// Ceiling for the lifecycle poll's failure backoff.
///
/// A retryable `labels.list` failure doubles the wait rather than
/// re-polling on the ordinary cadence, so a multi-hour outage costs a
/// bounded trickle of requests and warnings instead of one every thirty
/// seconds forever.
const LIFECYCLE_MAX_BACKOFF: Duration = Duration::from_secs(480);
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

struct LifecycleState {
    client: Arc<GmailClient>,
    shutdown: CancellationToken,
    pending: VecDeque<ScopeLifecycle>,
    /// Diff baseline, private to this stream.
    ///
    /// Deliberately NOT the shared `ScopeCache`: that cache is also
    /// written by `labels_for_flags` and `discover_memberships` whenever
    /// it goes stale, so a refresh landing between two lifecycle polls
    /// used to move the baseline forward and make the next diff empty -
    /// the create, delete or rename was then never announced.
    last_emitted: Option<ScopeSnapshot>,
    /// How long to wait before the NEXT poll, or `None` when no poll has
    /// been attempted yet.
    ///
    /// This is the attempt counter, not a baseline check. Keying the
    /// delay off `last_emitted` instead conflates "a poll has happened"
    /// with "a poll has SUCCEEDED", so a failing first poll looped with
    /// no wait at all and turned an outage into a hot request loop.
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

    /// Backs the cadence off after a retryable failure.
    ///
    /// Reset happens only in `note_poll_succeeded`, on a labels.list that
    /// actually completed. `bifrost-graph` paid for the other design: its
    /// EWS reconnect backoff has to reset on a completed read rather than
    /// on a successful subscribe, because an appliance that accepts every
    /// attempt and then fails it spins forever otherwise.
    fn note_poll_failed(&mut self) {
        let previous = self.next_delay.unwrap_or(LIFECYCLE_POLL_INTERVAL);
        self.next_delay = Some(previous.saturating_mul(2).min(LIFECYCLE_MAX_BACKOFF));
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
    Box::pin(
        stream::once(async move {
            let started = Instant::now();
            // A cache hit performs no request, and then the tally is
            // legitimately zero - the batch cost no inbound bytes.
            let (client, tally) = client.metered();
            match refresh_scope_snapshot(&client, &cache).await {
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

pub(crate) fn scope_lifecycle_stream(
    client: Arc<GmailClient>,
    shutdown: CancellationToken,
) -> AccountStream<ScopeLifecycleEvent> {
    let state = LifecycleState {
        client,
        shutdown,
        pending: VecDeque::new(),
        last_emitted: None,
        next_delay: None,
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                return Some((ScopeLifecycleEvent::Lifecycle(event), state));
            }
            if state.shutdown.is_cancelled() {
                return None;
            }
            if !state.await_next_poll().await {
                return None;
            }

            match fetch_scope_snapshot(&state.client).await {
                Ok(new) => {
                    state.note_poll_succeeded();
                    state.pending = record_lifecycle_snapshot(&mut state.last_emitted, new).into();
                }
                Err(error) => {
                    // Classify: terminal / engine-action -> emit
                    // Terminated and end the stream so the engine
                    // escalates. Retry classes continue with the
                    // backoff so transient hiccups don't pollute the
                    // engine surface.
                    let acct = super::error::into_account_error(
                        error,
                        super::error::GmailErrorContext::containers_list(),
                    );
                    if acct.recovery().is_terminal() || acct.recovery().requires_engine_action() {
                        return Some((ScopeLifecycleEvent::Terminated(acct), state));
                    }
                    state.note_poll_failed();
                    tracing::warn!(
                        target: "bifrost.gmail.scope_lifecycle",
                        kind = ?acct.kind(),
                        message_key = acct.message_key(),
                        "gmail label lifecycle poll: transient failure"
                    );
                }
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
    match refresh_scope_snapshot(client, cache).await {
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

pub(crate) async fn refresh_scope_snapshot(
    client: &GmailClient,
    cache: &ScopeCache,
) -> crate::Result<ScopeSnapshot> {
    let snapshot = fetch_scope_snapshot(client).await?;
    if let Ok(mut guard) = cache.snapshot.write() {
        *guard = snapshot.clone();
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
    async fn project_hold_label_lifecycle_makes_no_wire_calls() {
        use futures::FutureExt;
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
        let mut next = Box::pin(lifecycle.next());
        assert!(next.as_mut().now_or_never().is_none());
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(500)).await;
            assert!(next.as_mut().now_or_never().is_none());
        }
        assert!(script.requests().is_empty());
        shutdown.cancel();
        assert!(next.await.is_none());
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

    /// The interfering writer is the whole bug. `discover_memberships`
    /// refreshes the SHARED `ScopeCache` unconditionally, so while the
    /// lifecycle diff read that cache, a membership discovery landing
    /// between two polls moved the baseline forward and the next diff
    /// came back empty - the label creation was never announced and the
    /// consumer's container list silently diverged.
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

    /// Before a baseline exists there is nothing to diff, but there is
    /// still something to WAIT for. Keying the inter-poll delay off the
    /// baseline snapshot made a failing first poll re-issue `labels.list`
    /// with no delay at all, so an outage became a hot request loop. Two
    /// failures must therefore cost strictly more wall time than two
    /// successes, and the wait must observe the shutdown token.
    #[tokio::test(start_paused = true)]
    async fn repeated_initial_failures_back_off_instead_of_spinning() {
        let (client, _script) = scripted_client(vec![
            unavailable(),
            unavailable(),
            ok_json(json!({
                "labels": [{"id": "Label_1", "name": "One", "type": "user"}]
            })),
            ok_json(json!({
                "labels": [
                    {"id": "Label_1", "name": "One", "type": "user"},
                    {"id": "Label_2", "name": "Two", "type": "user"}
                ]
            })),
        ]);
        let started = tokio::time::Instant::now();
        let mut lifecycle = scope_lifecycle_stream(client, CancellationToken::new());

        let event = lifecycle.next().await;

        assert!(matches!(
            event,
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Label(LabelId(id))
            ))) if id == "Label_2"
        ));
        // t=0 fail, t=30+30 fail, t=+120 seed, t=+30 the Created above.
        assert_eq!(started.elapsed(), Duration::from_secs(210));
    }

    /// The backoff wait is a `select!` against the shutdown token, not a
    /// bare sleep, so `close()` during an outage ends the stream rather
    /// than parking it for the whole backoff.
    #[tokio::test(start_paused = true)]
    async fn a_stream_cancelled_while_backing_off_ends_promptly() {
        let (client, script) = scripted_client(vec![unavailable(), unavailable()]);
        let shutdown = CancellationToken::new();
        let started = tokio::time::Instant::now();
        let mut lifecycle = scope_lifecycle_stream(client, shutdown.clone());
        let drained = tokio::spawn(async move { lifecycle.next().await });
        tokio::time::sleep(Duration::from_secs(1)).await;

        shutdown.cancel();

        assert!(drained.await.expect("lifecycle task joins").is_none());
        assert_eq!(
            started.elapsed(),
            Duration::from_secs(1),
            "cancellation must end the stream where it happens, not at the end of the backoff",
        );
        assert_eq!(
            script.requests().len(),
            1,
            "a cancelled stream must not spend another request first",
        );
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
