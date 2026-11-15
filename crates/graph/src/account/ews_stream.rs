use std::collections::HashSet;
#[cfg(test)]
use std::time::Duration;

use bifrost_types::{
    AccountOperation, CursorScope, DiagnosticText, HintPayload, InvalidationHint, PushSource,
};
use futures::StreamExt;

#[cfg(test)]
use crate::ews::streaming::{
    EwsStreamingEventType, find_frame_end, find_frame_start, local_name,
    parse_streaming_notifications,
};
use crate::ews::streaming::{
    EwsStreamingNotification, StreamingFrameDecoder, build_get_streaming_events_request,
    build_subscribe_request, build_unsubscribe_request, notifications_from_streaming_frame,
    parse_subscribe_response,
};
use crate::ews::{EwsClient, EwsError, EwsExecute, EwsHeaders};

use super::GraphAccount;
use super::graph_error::{GraphErrorContext, ews_error_to_account_error};
use super::push::EwsSubscriptionScope;

pub(crate) type EwsReleaser = std::sync::Arc<
    dyn Fn(String) -> futures::future::BoxFuture<'static, Result<(), EwsError>> + Send + Sync,
>;
type Completion = futures::future::Shared<futures::future::BoxFuture<'static, ()>>;
#[derive(Default)]
struct EwsLedger {
    retired: bool,
    entries: std::collections::HashMap<String, EwsEntry>,
}
struct EwsEntry {
    state: EwsEntryState,
    releaser: EwsReleaser,
}
enum EwsEntryState {
    Live,
    Orphaned,
    Releasing(Completion),
}

type EwsRegistrations =
    std::collections::HashMap<bifrost_types::SubscriptionHandle, super::push::EwsSubscriptionState>;

/// Owns EWS registrations, their wakeup generation and server release state.
/// Clones share all mutable state, including metered account views.
#[derive(Clone)]
pub(crate) struct EwsRegistry {
    registrations: std::sync::Arc<tokio::sync::RwLock<EwsRegistrations>>,
    topology: std::sync::Arc<tokio::sync::watch::Sender<u64>>,
    ledger: std::sync::Arc<std::sync::Mutex<EwsLedger>>,
}
impl Default for EwsRegistry {
    fn default() -> Self {
        Self {
            registrations: std::sync::Arc::default(),
            topology: std::sync::Arc::new(tokio::sync::watch::Sender::new(0)),
            ledger: std::sync::Arc::default(),
        }
    }
}
impl EwsRegistry {
    pub(crate) async fn register(
        &self,
        shutdown: &tokio_util::sync::CancellationToken,
        handle: bifrost_types::SubscriptionHandle,
        scopes: Vec<EwsSubscriptionScope>,
    ) -> bool {
        let mut registrations = self.registrations.write().await;
        if shutdown.is_cancelled() {
            return false;
        }
        registrations.insert(handle, super::push::EwsSubscriptionState { scopes });
        drop(registrations);
        // No await after commit. Observe topology before reading the scope
        // union so registration between that read and a wait cannot be lost.
        self.topology.send_modify(|n| *n = n.wrapping_add(1));
        true
    }
    pub(crate) async fn remove(&self, handle: &bifrost_types::SubscriptionHandle) {
        let removed = self.registrations.write().await.remove(handle).is_some();
        if removed {
            self.topology.send_modify(|n| *n = n.wrapping_add(1));
        }
    }
    fn topology(&self) -> tokio::sync::watch::Receiver<u64> {
        self.topology.subscribe()
    }
    async fn active_scopes(&self) -> Vec<EwsSubscriptionScope> {
        let states = self.registrations.read().await;
        dedupe_by_ews_folder(states.values().flat_map(|s| s.scopes.iter()))
    }
    async fn scopes_for_folder(&self, folder: &str) -> Vec<CursorScope> {
        let states = self.registrations.read().await;
        unique_scopes_for_folder(states.values().flat_map(|s| s.scopes.iter()), folder)
    }
    fn record(&self, id: String, releaser: EwsReleaser) -> bool {
        let mut ledger = self.ledger.lock().expect("EWS ledger lock");
        ledger.entries.insert(
            id,
            EwsEntry {
                state: EwsEntryState::Live,
                releaser,
            },
        );
        ledger.retired
    }
    fn orphans(&self) -> Vec<String> {
        self.ledger
            .lock()
            .expect("EWS ledger lock")
            .entries
            .iter()
            .filter(|(_, e)| matches!(e.state, EwsEntryState::Orphaned))
            .map(|(id, _)| id.clone())
            .collect()
    }
    fn orphan(&self, id: &str) {
        let mut ledger = self.ledger.lock().expect("EWS ledger lock");
        if let Some(entry) = ledger.entries.get_mut(id)
            && matches!(entry.state, EwsEntryState::Live)
        {
            entry.state = EwsEntryState::Orphaned;
        }
    }
    #[cfg(test)]
    pub(crate) fn registrations_for_tests(&self) -> &tokio::sync::RwLock<EwsRegistrations> {
        &self.registrations
    }
    #[cfg(test)]
    pub(crate) fn topology_for_tests(&self) -> &tokio::sync::watch::Sender<u64> {
        &self.topology
    }
    pub(crate) fn release(&self, tasks: &super::push_tasks::PushTasks, id: &str) -> Completion {
        use futures::FutureExt;
        let mut ledger = self.ledger.lock().expect("EWS ledger lock");
        let Some(entry) = ledger.entries.get_mut(id) else {
            return futures::future::ready(()).boxed().shared();
        };
        if let EwsEntryState::Releasing(done) = &entry.state {
            return done.clone();
        }
        let releaser = std::sync::Arc::clone(&entry.releaser);
        let id = id.to_owned();
        let owner = self.clone();
        let (tx, rx) = futures::channel::oneshot::channel();
        let done = async move {
            let _ = rx.await;
        }
        .boxed()
        .shared();
        entry.state = EwsEntryState::Releasing(done.clone());
        let task = tokio::spawn(async move {
            let result = releaser(id.clone()).await;
            let mut ledger = owner.ledger.lock().expect("EWS ledger lock");
            if result.is_ok() || ledger.retired {
                ledger.entries.remove(&id);
            } else if let Some(entry) = ledger.entries.get_mut(&id) {
                entry.state = EwsEntryState::Orphaned;
            }
            if let Err(error) = result {
                tracing::debug!(?error, "EWS Unsubscribe failed");
            }
            let _ = tx.send(());
        });
        // Ledger -> task registry is the lock order. Publish the handle before
        // close can observe Releasing, so a claimed release cannot slip between
        // its ledger snapshot and its task join.
        tasks.track(task);
        drop(ledger);
        done
    }
    pub(crate) fn retire(&self, tasks: &super::push_tasks::PushTasks) {
        let ids = {
            let mut ledger = self.ledger.lock().expect("EWS ledger lock");
            ledger.retired = true;
            ledger.entries.keys().cloned().collect::<Vec<_>>()
        };
        for id in ids {
            drop(self.release(tasks, &id));
        }
    }
}
struct LiveSubscription {
    account: GraphAccount,
    id: String,
}
impl Drop for LiveSubscription {
    fn drop(&mut self) {
        self.account.ews.orphan(&self.id);
    }
}

pub(crate) fn streaming_session(
    account: GraphAccount,
    stop: bifrost_types::SessionStop,
) -> bifrost_types::AccountStream<bifrost_types::PushSessionEvent> {
    let Some(net) = account.mailboxes.primary().account_net() else {
        return Box::pin(futures::stream::once(async {
            bifrost_types::PushSessionEvent::Failed(ews_error_to_account_error(
                EwsError::Internal("EWS session has no attached account transport".into()),
                GraphErrorContext::ews(AccountOperation::PushStream),
            ))
        }));
    };
    let ews = std::sync::Arc::new(EwsClient::new(
        net,
        account.mailboxes.primary().ews_url(),
        account.mailboxes.primary().outlook_auth(),
    ));
    ews_session(account, ews, stop)
}

fn ews_session<E: EwsExecute + Send + Sync + 'static>(
    account: GraphAccount,
    ews: std::sync::Arc<E>,
    stop: bifrost_types::SessionStop,
) -> bifrost_types::AccountStream<bifrost_types::PushSessionEvent> {
    use bifrost_types::{PushSessionEnd as End, PushSessionEvent as E};
    use futures::FutureExt;
    Box::pin(async_stream::stream! {
        let Some(_lease) = account.push_lease.try_claim() else { yield E::Failed(bifrost_types::push_lane_contract_error(bifrost_types::Protocol::Ews, bifrost_types::PushLane(0), "already driven")); return; };
        let orphans = account.ews.orphans();
        for id in orphans { drop(account.ews.release(&account.push_tasks, &id)); }
        let mut topology = account.ews.topology();
        let scopes = loop {
            topology.borrow_and_update();
            let scopes = account.ews.active_scopes().await;
            if !scopes.is_empty() { break scopes; }
            tokio::select! { () = stop.stopped() => { yield E::Ended(End::Stopped); return; }, () = account.shutdown.cancelled() => { yield E::Ended(End::Stopped); return; }, _ = topology.changed() => {} }
        };
        // Let Subscribe finish so its server id can be recorded and released.
        let id = match subscribe(ews.as_ref(), &scopes).await { Ok(id) => id, Err(e) => { yield E::Failed(ews_error_to_account_error(e, GraphErrorContext::ews(AccountOperation::PushSubscribe))); return; } };
        let transport = std::sync::Arc::clone(&ews);
        let releaser: EwsReleaser = std::sync::Arc::new(move |id| {
            let transport = std::sync::Arc::clone(&transport);
            async move { transport.execute(&build_unsubscribe_request(&id), &EwsHeaders::default(), bifrost_net::ReplayPolicy::Idempotent).await.map(|_| ()) }.boxed()
        });
        let retired = account.ews.record(id.clone(), releaser);
        let _live = LiveSubscription { account: account.clone(), id: id.clone() };
        if retired || stop.is_stopped() || account.shutdown.is_cancelled() { account.ews.release(&account.push_tasks, &id).await; yield E::Ended(End::Stopped); return; }
        yield E::Established;
        let outcome = 'poll: loop {
            let body = build_get_streaming_events_request(&id, 30);
            let headers = EwsHeaders::default();
            let response = tokio::select! {
                () = stop.stopped() => break E::Ended(End::Stopped),
                () = account.shutdown.cancelled() => break E::Ended(End::Stopped),
                _ = topology.changed() => break E::Ended(End::Reconfigure),
                response = ews.execute_streaming(&body, &headers) => response,
            };
            let mut response = match response { Ok(response) => response, Err(e) => break E::Failed(ews_error_to_account_error(e, GraphErrorContext::ews(AccountOperation::PushStream))) };
            let mut decoder = StreamingFrameDecoder::default();
            loop {
                let chunk = tokio::select! {
                    () = stop.stopped() => break 'poll E::Ended(End::Stopped),
                    () = account.shutdown.cancelled() => break 'poll E::Ended(End::Stopped),
                    _ = topology.changed() => break 'poll E::Ended(End::Reconfigure),
                    chunk = response.next() => chunk,
                };
                let Some(chunk) = chunk else { if decoder.has_partial_frame() { break 'poll E::Ended(End::Lost); } break; };
                let chunk = match chunk { Ok(chunk) => chunk, Err(e) => break 'poll E::Failed(ews_error_to_account_error(e, GraphErrorContext::ews(AccountOperation::PushStream))) };
                let frames = match decoder.push(&chunk) { Ok(frames) => frames, Err(_) => break 'poll E::Ended(End::Lost) };
                for frame in frames {
                    let notifications = match notifications_from_streaming_frame(&frame) {
                        Ok(n) => n,
                        Err(EwsError::MalformedXml(_)) => break 'poll E::Ended(End::Lost),
                        Err(e) => break 'poll E::Failed(ews_error_to_account_error(e, GraphErrorContext::ews(AccountOperation::PushStream))),
                    };
                    for hint in streaming_hints(&account, notifications).await { yield E::Invalidated(hint); }
                }
            }
            yield E::Healthy;
        };
        account.ews.release(&account.push_tasks, &id).await;
        yield outcome;
    })
}

async fn subscribe<E: EwsExecute>(
    ews: &E,
    scopes: &[EwsSubscriptionScope],
) -> Result<String, EwsError> {
    let body = build_subscribe_request(scopes.iter().map(|scope| scope.ews_folder_id.as_str()));
    let xml = ews
        .execute(
            &body,
            &EwsHeaders::default(),
            bifrost_net::ReplayPolicy::Never,
        )
        .await?;
    let subscriptions = parse_subscribe_response(&xml)
        .map_err(|error| EwsError::MalformedXml(DiagnosticText::support_only(error)))?;
    subscriptions.into_iter().next().ok_or_else(|| {
        EwsError::MalformedXml(DiagnosticText::support_only(
            "EWS Subscribe returned no StreamingSubscription".to_string(),
        ))
    })
}

async fn streaming_hints(
    account: &GraphAccount,
    notifications: Vec<EwsStreamingNotification>,
) -> Vec<InvalidationHint> {
    let mut hints = Vec::new();
    for notification in notifications {
        let payloads = match notification.parent_folder_id.as_deref() {
            Some(folder_id) => {
                let scopes = scopes_for_folder(account, folder_id).await;
                if scopes.is_empty() {
                    vec![HintPayload::Unknown]
                } else {
                    scopes
                        .into_iter()
                        .map(HintPayload::SpecificCursorScope)
                        .collect()
                }
            }
            None => vec![HintPayload::Unknown],
        };
        for payload in payloads {
            hints.push(InvalidationHint {
                source: PushSource::EwsStreaming,
                payload,
            });
        }
    }
    hints
}

/// The union a session subscribes to. The session inlines this read (see
/// `ews_session`); this wrapper exists for tests that only want the union.
#[cfg(test)]
async fn active_ews_scopes(account: &GraphAccount) -> Vec<EwsSubscriptionScope> {
    account.ews.active_scopes().await
}

/// The union of every handle's registrations, one entry per EWS folder.
///
/// `build_subscribe_request` emits one `<t:FolderId>` per entry, and the
/// same folder can be registered more than once (two `push_subscribe`
/// calls over overlapping scopes, or two `FolderType` scopes differing
/// only in `ObjectType` - the translation request already collapses those
/// onto one `ewsId`). A repeated `FolderId` in a Subscribe body is
/// something EWS may accept, ignore, or reject the whole subscription
/// over; nothing here can find out, so the body simply never contains one.
/// Which registration survives is immaterial - only `ews_folder_id` is
/// read from it.
///
/// Pure over the scope registrations so the rule is pinnable without a
/// live client.
fn dedupe_by_ews_folder<'a>(
    scopes: impl Iterator<Item = &'a EwsSubscriptionScope>,
) -> Vec<EwsSubscriptionScope> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut deduped = Vec::new();
    for scope in scopes {
        if seen.insert(scope.ews_folder_id.as_str()) {
            deduped.push(scope.clone());
        }
    }
    deduped
}

async fn scopes_for_folder(account: &GraphAccount, folder_id: &str) -> Vec<CursorScope> {
    account.ews.scopes_for_folder(folder_id).await
}

/// Every DISTINCT cursor scope registered against `folder_id`, in
/// first-seen order.
///
/// Fan-out is per scope, not per registration: one EWS folder can back
/// several scopes (a `FolderType` per `ObjectType` over one container),
/// and each of those needs its own invalidation because each owns its own
/// delta cursor. The SAME scope reaching us twice is a different story -
/// two handles registering overlapping scopes, or one request naming a
/// scope twice - and every repeat costs the reconciler another full
/// `changes_stream` run over a scope it is already reconciling. Distinct
/// scopes are kept; equal ones collapse.
///
/// Pure over the scope registrations so the routing rule is pinnable
/// without a live client.
fn unique_scopes_for_folder<'a>(
    scopes: impl Iterator<Item = &'a EwsSubscriptionScope>,
    folder_id: &str,
) -> Vec<CursorScope> {
    let mut seen: HashSet<&CursorScope> = HashSet::new();
    let mut routed = Vec::new();
    for scope in scopes {
        if scope.ews_folder_id == folder_id && seen.insert(&scope.scope) {
            routed.push(scope.scope.clone());
        }
    }
    routed
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod replay_tests;
