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
mod tests {
    use std::collections::VecDeque;

    use bifrost_types::SubscriptionHandle;

    use super::super::PushMode;
    use super::super::push::EwsSubscriptionState;
    use super::*;

    use crate::client::GraphClient;

    fn build_subscribe_request(scopes: &[EwsSubscriptionScope]) -> String {
        crate::ews::streaming::build_subscribe_request(
            scopes.iter().map(|scope| scope.ews_folder_id.as_str()),
        )
    }

    #[test]
    fn parses_streaming_notification_xml() {
        let notifications =
            parse_streaming_notifications(NOTIFICATION_XML).expect("parse should succeed");
        assert_eq!(notifications.len(), 2);
        assert_eq!(notifications[0].event_type, EwsStreamingEventType::NewMail);
        assert_eq!(notifications[0].item_id.as_deref(), Some("item-1"));
        assert_eq!(
            notifications[0].parent_folder_id.as_deref(),
            Some("folder-1")
        );
        assert_eq!(notifications[1].event_type, EwsStreamingEventType::Modified);
        assert_eq!(notifications[1].item_id.as_deref(), Some("item-2"));
    }

    #[test]
    fn streaming_frame_decoder_emits_before_the_outer_response_closes() {
        let frame = r#"<m:GetStreamingEventsResponseMessage ResponseClass="Success"><m:Notifications><t:Notification><t:SubscriptionId>sub-1</t:SubscriptionId><t:NewMailEvent><t:ItemId Id="item-1"/><t:ParentFolderId Id="folder-1"/></t:NewMailEvent></t:Notification></m:Notifications></m:GetStreamingEventsResponseMessage>"#;
        let mut decoder = StreamingFrameDecoder::default();
        let split = frame.len() / 2;
        let bytes = frame.as_bytes();
        assert!(
            decoder
                .push(&bytes[..split])
                .expect("first chunk")
                .is_empty()
        );
        let frames = decoder.push(&bytes[split..]).expect("second chunk");
        assert_eq!(frames, vec![frame.to_string()]);
        let notifications = notifications_from_streaming_frame(&frames[0]).expect("frame parses");
        assert_eq!(notifications.len(), 1);
        assert_eq!(
            notifications[0].parent_folder_id.as_deref(),
            Some("folder-1")
        );
    }

    /// An XML namespace prefix is an alias the writer picks. A response
    /// that binds the messages namespace to something other than `m:` -
    /// or makes it the default namespace and writes the element with no
    /// prefix at all - is exactly as valid, and the framing must recognize
    /// it. Byte-matching `<m:` dropped both shapes on the floor: no frame,
    /// no partial frame, so the stream ended reporting neither an
    /// invalidation nor a failure.
    #[test]
    fn the_framing_recognizes_any_namespace_prefix_and_the_default_namespace() {
        for frame in [
            r#"<a:GetStreamingEventsResponseMessage ResponseClass="Success"><a:Notifications><b:Notification><b:SubscriptionId>sub-1</b:SubscriptionId><b:NewMailEvent><b:ItemId Id="item-1"/><b:ParentFolderId Id="folder-1"/></b:NewMailEvent></b:Notification></a:Notifications></a:GetStreamingEventsResponseMessage>"#,
            r#"<GetStreamingEventsResponseMessage xmlns="http://schemas.microsoft.com/exchange/services/2006/messages" ResponseClass="Success"><Notifications><Notification xmlns="http://schemas.microsoft.com/exchange/services/2006/types"><SubscriptionId>sub-1</SubscriptionId><NewMailEvent><ItemId Id="item-1"/><ParentFolderId Id="folder-1"/></NewMailEvent></Notification></Notifications></GetStreamingEventsResponseMessage>"#,
        ] {
            let mut decoder = StreamingFrameDecoder::default();
            let bytes = frame.as_bytes();
            let split = bytes.len() / 2;
            assert!(
                decoder
                    .push(&bytes[..split])
                    .expect("first chunk")
                    .is_empty(),
                "{frame}"
            );
            assert!(decoder.has_partial_frame(), "{frame}");
            let frames = decoder.push(&bytes[split..]).expect("second chunk");
            assert_eq!(frames, vec![frame.to_string()]);
            assert!(!decoder.has_partial_frame(), "{frame}");
            let notifications =
                notifications_from_streaming_frame(&frames[0]).expect("frame parses");
            assert_eq!(notifications.len(), 1, "{frame}");
            assert_eq!(
                notifications[0].parent_folder_id.as_deref(),
                Some("folder-1"),
                "{frame}"
            );
        }
    }

    /// The local name must be a whole element name, not a substring: an
    /// element whose name merely ends with it, and an attribute value that
    /// happens to contain it, are not frames.
    #[test]
    fn the_framing_matches_whole_element_names_only() {
        assert_eq!(
            find_frame_start(br#"<m:NotAGetStreamingEventsResponseMessage>"#),
            None
        );
        assert_eq!(
            find_frame_start(br#"<m:Other why="GetStreamingEventsResponseMessage">"#),
            None
        );
        assert_eq!(
            find_frame_start(br#"<m:GetStreamingEventsResponseMessageExtra>"#),
            None
        );
        assert_eq!(
            find_frame_start(br#"<:GetStreamingEventsResponseMessage>"#),
            None
        );
        // A buffer that stops mid-name is not a hit yet; the boundary byte
        // decides, and it has not arrived.
        assert_eq!(
            find_frame_start(b"<m:GetStreamingEventsResponseMessag"),
            None
        );
        assert_eq!(
            find_frame_start(b"...<m:GetStreamingEventsResponseMessage>"),
            Some(3)
        );
        assert_eq!(
            find_frame_end(b"</zz:GetStreamingEventsResponseMessage >rest"),
            Some(40)
        );
    }

    #[test]
    fn parses_subscribe_response_xml() {
        let subscriptions =
            parse_subscribe_response(&subscribe_response_xml("sub-1")).expect("parse succeeds");
        assert_eq!(subscriptions, vec!["sub-1".to_string()]);
    }

    fn email_scope(folder: &str) -> CursorScope {
        CursorScope::FolderType {
            folder: bifrost_types::FolderId(folder.to_string()),
            ty: bifrost_types::ObjectType::Email,
        }
    }

    fn ews_scope(rest_id: &str, ews_id: &str) -> EwsSubscriptionScope {
        EwsSubscriptionScope {
            scope: email_scope(rest_id),
            ews_folder_id: ews_id.to_string(),
        }
    }

    #[tokio::test]
    async fn active_scopes_deduplicate_ews_folder_ids_but_routing_keeps_every_scope() {
        let account = GraphAccount::new_for_tests(
            crate::client::GraphClient::new("token"),
            super::super::PushMode::EwsStreaming,
        );
        account.ews.registrations_for_tests().write().await.insert(
            bifrost_types::SubscriptionHandle("first".to_string()),
            EwsSubscriptionState {
                scopes: vec![ews_scope("rest-a", "ews-shared")],
            },
        );
        account.ews.registrations_for_tests().write().await.insert(
            bifrost_types::SubscriptionHandle("second".to_string()),
            EwsSubscriptionState {
                scopes: vec![ews_scope("rest-b", "ews-shared")],
            },
        );

        let active = active_ews_scopes(&account).await;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].ews_folder_id, "ews-shared");

        let mut routed = scopes_for_folder(&account, "ews-shared")
            .await
            .into_iter()
            .map(|scope| match scope {
                CursorScope::FolderType { folder, .. } => folder.0,
                other => panic!("unexpected scope {other:?}"),
            })
            .collect::<Vec<_>>();
        routed.sort();
        assert_eq!(routed, ["rest-a", "rest-b"]);
    }

    /// Two handles registering the SAME scope is the overlap case
    /// `push_subscribe` allows: without collapsing them, one notification
    /// would drive the reconciler through `changes_stream` twice over one
    /// cursor. Scopes that merely share a folder stay distinct - each owns
    /// its own delta cursor and must be invalidated separately.
    #[test]
    fn routing_collapses_repeated_scopes_but_keeps_distinct_ones() {
        let mail = ews_scope("rest-a", "ews-shared");
        let duplicate = ews_scope("rest-a", "ews-shared");
        let contacts = EwsSubscriptionScope {
            scope: CursorScope::FolderType {
                folder: bifrost_types::FolderId("rest-a".to_string()),
                ty: bifrost_types::ObjectType::Contact,
            },
            ews_folder_id: "ews-shared".to_string(),
        };
        let elsewhere = ews_scope("rest-z", "ews-other");
        let registrations = [
            mail.clone(),
            duplicate,
            contacts.clone(),
            elsewhere.clone(),
            mail.clone(),
        ];

        let routed = unique_scopes_for_folder(registrations.iter(), "ews-shared");
        assert_eq!(routed, vec![mail.scope.clone(), contacts.scope]);

        // A folder nothing registered routes nowhere; the caller turns the
        // empty result into an account-wide `Unknown` hint.
        assert!(unique_scopes_for_folder(registrations.iter(), "ews-absent").is_empty());
    }

    /// The Subscribe body must name each EWS folder once even though the
    /// registrations that produced it are per scope.
    #[test]
    fn the_subscribe_union_carries_one_entry_per_ews_folder() {
        let registrations = [
            ews_scope("rest-a", "ews-shared"),
            ews_scope("rest-b", "ews-shared"),
            ews_scope("rest-z", "ews-other"),
            ews_scope("rest-a", "ews-shared"),
        ];

        let deduped = dedupe_by_ews_folder(registrations.iter());
        let folders = deduped
            .iter()
            .map(|scope| scope.ews_folder_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(folders, ["ews-shared", "ews-other"]);

        let body = build_subscribe_request(&deduped);
        assert_eq!(body.matches(r#"<t:FolderId Id="ews-shared"/>"#).count(), 1);
        assert_eq!(body.matches(r#"<t:FolderId Id="ews-other"/>"#).count(), 1);
    }

    /// The Subscribe body's folder set is the subscription's scope, which
    /// EWS answers once; the long poll and the unsubscribe each name one
    /// subscription. All three stay inside the single-answer invariant
    /// `build_soap_envelope` enforces.
    #[test]
    fn the_ews_stream_bodies_stay_within_the_single_answer_invariant() {
        let subscribe = build_subscribe_request(&[
            ews_scope("r1", "f1"),
            ews_scope("r2", "f2"),
            ews_scope("r3", "f3"),
        ]);
        assert_eq!(crate::ews::per_answer_request_ids(&subscribe), 0);
        assert_eq!(
            crate::ews::per_answer_request_ids(&build_get_streaming_events_request("sub-1", 30)),
            1
        );
        assert_eq!(
            crate::ews::per_answer_request_ids(&build_unsubscribe_request("sub-1")),
            0
        );
    }

    #[test]
    fn subscribe_request_lists_every_folder_and_the_six_event_types() {
        let body = build_subscribe_request(&[ews_scope("r1", "f1"), ews_scope("r2", "f2")]);
        assert!(body.contains(r#"<t:FolderId Id="f1"/>"#), "{body}");
        assert!(body.contains(r#"<t:FolderId Id="f2"/>"#), "{body}");
        for event in [
            "NewMailEvent",
            "CreatedEvent",
            "DeletedEvent",
            "ModifiedEvent",
            "MovedEvent",
            "CopiedEvent",
        ] {
            assert!(body.contains(event), "{event} missing from {body}");
        }
        // `StreamingSubscriptionRequest` admits only FolderIds and
        // EventTypes; a Watermark is a schema violation EWS rejects.
        assert!(!body.contains("Watermark"));
    }

    #[test]
    fn subscribe_request_escapes_xml_metacharacters() {
        let body = build_subscribe_request(&[ews_scope("rest-id", r#"a&b<c>"d'"#)]);
        assert!(
            body.contains(r#"<t:FolderId Id="a&amp;b&lt;c&gt;&quot;d&apos;"/>"#),
            "{body}"
        );
    }

    #[test]
    fn subscribe_request_uses_the_translated_ews_id_not_the_graph_rest_id() {
        let body = build_subscribe_request(&[ews_scope("rest-AAMk", "ews-AAE=")]);
        assert!(body.contains(r#"<t:FolderId Id="ews-AAE="/>"#), "{body}");
        assert!(!body.contains("rest-AAMk"), "{body}");
    }

    #[test]
    fn subscribe_request_builder_emits_an_empty_list_only_for_no_translations() {
        let body = build_subscribe_request(&[]);
        assert!(body.contains("<t:FolderIds></t:FolderIds>"), "{body}");
    }

    #[test]
    fn get_streaming_events_clamps_the_connection_timeout_to_the_ews_maximum() {
        assert!(
            build_get_streaming_events_request("sub-1", 120)
                .contains("<m:ConnectionTimeout>30</m:ConnectionTimeout>")
        );
        assert!(
            build_get_streaming_events_request("sub-1", 5)
                .contains("<m:ConnectionTimeout>5</m:ConnectionTimeout>")
        );
        assert!(
            build_get_streaming_events_request("a&b", 30)
                .contains("<t:SubscriptionId>a&amp;b</t:SubscriptionId>")
        );
    }

    #[test]
    fn unsubscribe_request_names_the_subscription_and_escapes_it() {
        let body = build_unsubscribe_request("a&b");
        assert!(body.contains("<m:Unsubscribe>"), "{body}");
        assert!(
            body.contains("<m:SubscriptionId>a&amp;b</m:SubscriptionId>"),
            "{body}"
        );
    }

    #[test]
    fn the_envelope_subscription_id_is_stamped_on_every_event_in_the_notification() {
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:PreviousWatermark>wm-0</t:PreviousWatermark>
              <t:MoreEvents>false</t:MoreEvents>
              <t:CreatedEvent>
                <t:Watermark>wm-1</t:Watermark>
                <t:ItemId Id="item-1"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:CreatedEvent>
              <t:DeletedEvent>
                <t:Watermark>wm-2</t:Watermark>
                <t:ItemId Id="item-2"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:DeletedEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

        let notifications = parse_streaming_notifications(xml).expect("parse should succeed");
        assert_eq!(notifications.len(), 2);
        for notification in &notifications {
            assert_eq!(notification.subscription_id.as_deref(), Some("sub-1"));
        }
        assert_eq!(notifications[0].event_type, EwsStreamingEventType::Created);
        assert_eq!(notifications[1].event_type, EwsStreamingEventType::Deleted);
    }

    #[test]
    fn a_status_frame_without_a_parent_folder_parses_without_fabricating_one() {
        // The session maps a missing parent folder onto an account-wide
        // `Unknown` hint; the parser must report the absence honestly
        // rather than defaulting to an empty FolderId.
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:NewMailEvent>
                <t:Watermark>wm-9</t:Watermark>
              </t:NewMailEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

        let notifications = parse_streaming_notifications(xml).expect("parse should succeed");
        assert_eq!(notifications.len(), 1);
        assert!(notifications[0].parent_folder_id.is_none());
        assert!(notifications[0].item_id.is_none());
    }

    #[test]
    fn unrecognized_event_elements_produce_no_notification() {
        // Forward compatibility: an EWS event type this build does not
        // know must be ignored, not turned into an untyped invalidation.
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:StatusEvent>
                <t:Watermark>wm-1</t:Watermark>
              </t:StatusEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

        assert!(
            parse_streaming_notifications(xml)
                .expect("parse should succeed")
                .is_empty()
        );
    }

    #[test]
    fn subscribe_response_without_a_subscription_id_yields_no_subscription() {
        // `subscribe` turns the empty vec into `MalformedXml`; pin that the
        // parser does not invent an empty id.
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:SubscribeResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:SubscribeResponseMessage ResponseClass="Success">
          <m:SubscriptionId></m:SubscriptionId>
        </m:SubscribeResponseMessage>
      </m:ResponseMessages>
    </m:SubscribeResponse>
  </s:Body>
</s:Envelope>"#;
        assert!(
            parse_subscribe_response(xml)
                .expect("parse should succeed")
                .is_empty()
        );
    }

    #[test]
    fn local_name_strips_any_namespace_prefix() {
        assert_eq!(local_name("t:ItemId"), "ItemId");
        assert_eq!(local_name("ItemId"), "ItemId");
        assert_eq!(local_name(""), "");
    }

    /// Both streaming parsers had no `GeneralRef` arm, so every reference -
    /// even `&amp;` - vanished from a subscription id, and an unknown entity
    /// or invalid character reference parsed on as if absent. References now
    /// fold back in, and an unresolvable one is a malformed response.
    #[test]
    fn stream_parsers_resolve_references_and_refuse_unresolvable_ones() {
        let notification = |id: &str| {
            format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:GetStreamingEventsResponse><m:Notifications><t:Notification>
<t:SubscriptionId>{id}</t:SubscriptionId>
<t:NewMailEvent><t:ItemId Id="i1"/><t:ParentFolderId Id="f1"/></t:NewMailEvent>
</t:Notification></m:Notifications></m:GetStreamingEventsResponse></s:Body></s:Envelope>"#
            )
        };
        let subscribe = |id: &str| {
            format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m"><s:Body><m:SubscribeResponse>
<m:ResponseMessages><m:SubscribeResponseMessage ResponseClass="Success">
<m:SubscriptionId>{id}</m:SubscriptionId>
</m:SubscribeResponseMessage></m:ResponseMessages></m:SubscribeResponse></s:Body></s:Envelope>"#
            )
        };

        let parsed = parse_streaming_notifications(&notification("a&amp;b&#x43;"))
            .expect("well-formed references parse");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].subscription_id.as_deref(), Some("a&bC"));
        assert_eq!(
            parse_subscribe_response(&subscribe("a&amp;b&#x43;")).expect("parses"),
            vec!["a&bC".to_string()]
        );

        for bad in ["a&bogus;b", "a&#xD800;b"] {
            assert!(
                parse_streaming_notifications(&notification(bad)).is_err(),
                "notification {bad:?}"
            );
            assert!(
                parse_subscribe_response(&subscribe(bad)).is_err(),
                "subscribe {bad:?}"
            );
        }
    }

    /// Attribute values were read raw: an escaped item or folder id kept its
    /// `&amp;` (so the invalidation named an id the server never meant), and
    /// an unresolvable reference passed through as text. Both the Start and
    /// the Empty spelling of the id elements unescape strictly now.
    #[test]
    fn stream_id_attributes_unescape_strictly() {
        let notification = |item: &str, folder: &str| {
            format!(
                r#"<s:Envelope xmlns:s="s" xmlns:m="m" xmlns:t="t"><s:Body>
<m:GetStreamingEventsResponse><m:Notifications><t:Notification>
<t:SubscriptionId>sub-1</t:SubscriptionId>
<t:NewMailEvent><t:ItemId Id="{item}" ChangeKey="c&amp;k"/>
<t:ParentFolderId Id="{folder}"></t:ParentFolderId></t:NewMailEvent>
</t:Notification></m:Notifications></m:GetStreamingEventsResponse></s:Body></s:Envelope>"#
            )
        };

        let parsed = parse_streaming_notifications(&notification("i&amp;1", "f&#x41;"))
            .expect("well-formed attributes parse");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].item_id.as_deref(), Some("i&1"));
        assert_eq!(parsed[0].item_change_key.as_deref(), Some("c&k"));
        assert_eq!(parsed[0].parent_folder_id.as_deref(), Some("fA"));

        for (item, folder) in [("i&bogus;", "f1"), ("i1", "f&#xD800;")] {
            assert!(
                parse_streaming_notifications(&notification(item, folder)).is_err(),
                "{item} / {folder}"
            );
        }
    }

    // ----- session tests over the scripted EWS transport -----

    const NOTIFICATION_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetStreamingEventsResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                                  xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetStreamingEventsResponseMessage ResponseClass="Success">
          <m:Notifications>
            <t:Notification>
              <t:SubscriptionId>sub-1</t:SubscriptionId>
              <t:NewMailEvent>
                <t:Watermark>wm-1</t:Watermark>
                <t:ItemId Id="item-1" ChangeKey="ck-1"/>
                <t:ParentFolderId Id="folder-1" ChangeKey="fck-1"/>
              </t:NewMailEvent>
              <t:ModifiedEvent>
                <t:Watermark>wm-2</t:Watermark>
                <t:ItemId Id="item-2"/>
                <t:ParentFolderId Id="folder-1"/>
              </t:ModifiedEvent>
            </t:Notification>
          </m:Notifications>
        </m:GetStreamingEventsResponseMessage>
      </m:ResponseMessages>
    </m:GetStreamingEventsResponse>
  </s:Body>
</s:Envelope>"#;

    fn subscribe_response_xml(subscription_id: &str) -> String {
        format!(
            r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:SubscribeResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:SubscribeResponseMessage ResponseClass="Success">
          <m:SubscriptionId>{subscription_id}</m:SubscriptionId>
        </m:SubscribeResponseMessage>
      </m:ResponseMessages>
    </m:SubscribeResponse>
  </s:Body>
</s:Envelope>"#
        )
    }

    fn unsubscribe_response_xml() -> String {
        r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:UnsubscribeResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:UnsubscribeResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
        </m:UnsubscribeResponseMessage>
      </m:ResponseMessages>
    </m:UnsubscribeResponse>
  </s:Body>
</s:Envelope>"#
            .to_string()
    }

    enum ScriptStep {
        Delayed(std::sync::Arc<tokio::sync::Notify>, String),
        Respond(String),
        Fail(EwsError),
        /// A long poll with nothing to say: the request is reported to the
        /// test and the future then never resolves, exactly like a hanging
        /// `GetStreamingEvents`. The session leaves it via its `select!`
        /// (stop, shutdown or topology change).
        Hang,
    }

    /// In-process scripted EWS transport. Each `execute` reports its body
    /// on the channel (which is how tests sequence deterministically,
    /// without wall-clock waits) and then plays the next scripted step.
    struct ScriptedEws {
        steps: tokio::sync::Mutex<VecDeque<ScriptStep>>,
        request_tx: tokio::sync::mpsc::UnboundedSender<String>,
    }

    fn scripted(
        steps: Vec<ScriptStep>,
    ) -> (ScriptedEws, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            ScriptedEws {
                steps: tokio::sync::Mutex::new(steps.into()),
                request_tx,
            },
            request_rx,
        )
    }

    impl EwsExecute for ScriptedEws {
        async fn execute(
            &self,
            body_xml: &str,
            _headers: &EwsHeaders,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<String, EwsError> {
            let step = self
                .steps
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| panic!("EWS script exhausted by request: {body_xml}"));
            let _ = self.request_tx.send(body_xml.to_string());
            match step {
                ScriptStep::Respond(xml) => Ok(xml),
                ScriptStep::Delayed(ready, xml) => {
                    ready.notified().await;
                    Ok(xml)
                }
                ScriptStep::Fail(error) => Err(error),
                ScriptStep::Hang => std::future::pending().await,
            }
        }
    }

    /// Registers a handle's scopes the way `subscribe_ews` does: map write,
    /// then topology bump.
    async fn register(account: &GraphAccount, handle: &str, scopes: Vec<EwsSubscriptionScope>) {
        account.ews.registrations_for_tests().write().await.insert(
            SubscriptionHandle(handle.to_string()),
            EwsSubscriptionState { scopes },
        );
        account
            .ews
            .topology_for_tests()
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    /// The envelope net builds when a `Never` request meets a 503: one
    /// attempt, no replay, a possible-commit ledger verdict.
    fn unreplayed_unavailable() -> EwsError {
        EwsError::Transport(bifrost_net::Error::AttemptHistory {
            source: Box::new(bifrost_net::Error::RetryBudgetExhausted {
                final_response: Some(bifrost_net::FinalResponse {
                    status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    headers: reqwest::header::HeaderMap::new(),
                    body: bytes::Bytes::new(),
                }),
                retry_after_history: Vec::new(),
                stop_reason: bifrost_net::RetryStopReason::ReplayForbidden,
            }),
            evidence: bifrost_net::RequestEvidence::new(
                Vec::new(),
                bifrost_net::CommitEvidence::MayHaveCommitted,
            ),
        })
    }

    include!("ews_session_tests.rs");
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use bifrost_net::test_support::{Canned, ScriptedDispatch, canned};
    fn fixture() -> (EwsClient, std::sync::Arc<ScriptedDispatch>) {
        let script = ScriptedDispatch::new([
            Canned::Error(bifrost_net::Error::Network {
                message: "lost".into(),
                transmission_state: bifrost_types::TransmissionState::InFlight,
                source: None,
            }),
            canned(reqwest::StatusCode::OK, b"<root/>"),
        ]);
        let mut policy = bifrost_net::RetryPolicy::default();
        policy.max_attempts = 2;
        policy.initial_backoff = Duration::ZERO;
        policy.max_backoff = Duration::ZERO;
        let net = bifrost_net::test_support::scripted_account(
            &script,
            bifrost_net::NetConfig::default(),
            vec![],
            std::sync::Arc::new(bifrost_net::StaticTokenSource::new("token", None)),
            policy,
        );
        let client = crate::client::GraphClient::new("graph-token").with_outlook_source(
            std::sync::Arc::new(bifrost_net::StaticTokenSource::new("token", None)),
        );
        (
            EwsClient::new(net, client.ews_url(), client.outlook_auth()),
            script,
        )
    }
    #[tokio::test]
    async fn ews_subscribe_is_not_replayed_and_reads_are() {
        let (client, script) = fixture();
        assert!(subscribe(&client, &[]).await.is_err());
        assert_eq!(script.requests().len(), 1);
        assert_eq!(
            script.requests()[0]
                .headers
                .get(reqwest::header::AUTHORIZATION)
                .expect("Exchange bearer"),
            "Bearer token",
        );
        let (client, script) = fixture();
        // The minimal XML can be rejected by the parser; the two wire attempts
        // pin the policy before parsing the response.
        let _ = client.find_folder("inbox", &EwsHeaders::default()).await;
        assert_eq!(script.requests().len(), 2);
    }
    #[tokio::test]
    async fn ews_streaming_request_is_not_replayed_by_net() {
        let (client, script) = fixture();
        assert!(
            client
                .execute_streaming("<GetStreamingEvents/>", &EwsHeaders::default())
                .await
                .is_err()
        );
        assert_eq!(script.requests().len(), 1);
        assert_eq!(
            script.requests()[0]
                .headers
                .get(reqwest::header::AUTHORIZATION)
                .expect("Exchange bearer"),
            "Bearer token",
        );
    }
}
