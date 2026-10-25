use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bifrost_types::{
    AccountStream, Batch, CursorScope, MembershipScope, PageBoundary, ScopeLifecycle,
    ScopeLifecycleEvent, SyncEvent,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::core::transport::HttpTransport;
use crate::mailbox::{Mailbox, MailboxChanges, MailboxGet, MailboxId, Property};

use super::capabilities::CoreLimits;
use super::state_cache::{self, StateMap};
use super::state_walk::StateWalk;

type MailAccount<T> = crate::account::Account<T>;

pub(crate) fn cursor_scopes(scopes: Vec<CursorScope>) -> AccountStream<SyncEvent<CursorScope>> {
    Box::pin(async_stream::stream! {
        if !scopes.is_empty() {
            yield SyncEvent::Batch(Batch {
                items: scopes,
                page_boundary: PageBoundary::Final,
                server_latency: Duration::ZERO,
                // Synthetic: the scopes were derived from the session
                // already in hand, so this batch performed no request
                // and genuinely cost nothing.
                bytes_in: 0,
                checkpoint: None,
            });
        }
        yield SyncEvent::Done(None);
    })
}

pub(crate) fn memberships<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_owners: Vec<MembershipScope>,
) -> AccountStream<SyncEvent<MembershipScope>> {
    Box::pin(async_stream::stream! {
        let (mail, tally) = mail.metered();
        let started = Instant::now();
        let response = mail
            .call(MailboxGet::new().properties([Property::Id]))
            .await;

        match response {
            Ok(response) => {
                let mut items = response
                    .into_list()
                    .into_iter()
                    .filter_map(|mut mailbox| {
                        let id = mailbox.take_id();
                        if id.as_str().is_empty() {
                            None
                        } else {
                            Some(MembershipScope::Mailbox(bifrost_types::MailboxId(
                                id.into_string(),
                            )))
                        }
                    })
                    .collect::<Vec<_>>();

                // Each foreign (shared/delegate) account contributes its
                // owner tag (`Mailbox(accountId)`) so the consumer maps
                // its foreign scopes to the shared-account identity.
                items.extend(foreign_owners);

                if !items.is_empty() {
                    yield SyncEvent::Batch(Batch {
                        items,
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: tally.take(),
                        checkpoint: None,
                    });
                }
                yield SyncEvent::Done(None);
            }
            Err(err) => {
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::new(bifrost_types::AccountOperation::DiscoverMemberships),
                );
            }
        }
    })
}

pub(crate) async fn fetch_mailbox_names<T: HttpTransport>(
    mail: &MailAccount<T>,
) -> crate::Result<(String, HashMap<String, String>)> {
    let response = mail
        .call(MailboxGet::new().properties([Property::Id, Property::Name]))
        .await?;
    let state = response.state().to_string();
    let mut names = HashMap::new();

    for mut mailbox in response.into_list() {
        let id = mailbox.take_id();
        if !id.as_str().is_empty() {
            names.insert(id.into_string(), mailbox.name().unwrap_or("").to_string());
        }
    }

    Ok((state, names))
}

/// Pause between lifecycle polls without outliving shutdown.
///
/// The stream is pull-based, so a bare sleep cannot leak a task - but a
/// consumer still polling after `close()` cancels the token would wait
/// out the rest of the interval before the loop's top-of-iteration check
/// ends the stream. Selecting on the token makes the end prompt by
/// construction; every call site falls through to that check, which is
/// where cancellation terminates the loop.
async fn poll_pause(
    shutdown: &CancellationToken,
    session_changes: &mut tokio::sync::watch::Receiver<u64>,
) {
    tokio::select! {
        () = shutdown.cancelled() => {}
        () = tokio::time::sleep(POLL_INTERVAL) => {}
        _ = session_changes.changed() => {}
    }
}

const POLL_INTERVAL: Duration = Duration::from_secs(300);

pub(crate) fn scope_lifecycle<T: HttpTransport>(
    mail: MailAccount<T>,
    limits: CoreLimits,
    mailbox_states: StateMap,
    account_id: String,
    mailbox_names: Arc<Mutex<HashMap<String, String>>>,
    shutdown: CancellationToken,
    client: crate::client::Client<T>,
) -> AccountStream<ScopeLifecycleEvent> {
    Box::pin(async_stream::stream! {
        let mut session_changes = client.session_changes();
        // Forward-progress guard for the pagination BURST this poller is
        // currently in, not for the poller's whole life. `Mailbox/changes`
        // is paginated here exactly as in `changes.rs`, so the same two
        // unbounded shapes exist (an unmoved state under
        // `hasMoreChanges: true`, and a pair of states alternating
        // forever); unlike a change walk, this stream is driven for the
        // life of the account, so an unguarded spin is a permanent hot
        // loop against one non-conformant server. The guard resets
        // whenever the loop reaches a poll pause, because a state legitimately
        // repeating across two polls minutes apart is not a spin.
        let mut walk: Option<StateWalk> = None;
        loop {
            if shutdown.is_cancelled() {
                break;
            }

            if !client.is_session_updated() {
                yield ScopeLifecycleEvent::Terminated(
                    super::capabilities::session_state_changed(),
                );
                break;
            }

            let since_state = state_cache::get(&mailbox_states, &account_id).await;

            let Some(since_state) = since_state else {
                walk = None;
                poll_pause(&shutdown, &mut session_changes).await;
                continue;
            };

            let state_walk = walk.get_or_insert_with(|| {
                StateWalk::lifecycle(
                    CursorScope::Type(bifrost_types::ObjectType::Mailbox),
                    &since_state,
                )
            });

            let max_changes = NonZeroUsize::new(limits.max_objects_in_get.max(1));
            let Some(max_changes) = max_changes else {
                break;
            };

            let response = mail
                .call(MailboxChanges::new(since_state.clone()).max_changes(max_changes))
                .await;

            match response {
                Ok(response) => {
                    let created = response.created().to_vec();
                    let updated = response.updated().to_vec();
                    let destroyed = response.destroyed().to_vec();
                    let new_state = response.new_state().to_string();

                    // Refuse a page that does not move the walk forward,
                    // BEFORE emitting anything from it or committing its
                    // state: the state never moved (or moved back), so
                    // nothing is lost. Unlike a change walk, which the
                    // engine can restart, this stream is the account's
                    // only lifecycle channel - so the termination is
                    // final for the account's lifetime, and
                    // `Protocol(ContractViolation)` is the honest report:
                    // the engine surfaces a non-conformant provider
                    // rather than burning a poll loop forever.
                    if let Some(fault) = state_walk.observe_changes_page(
                        &since_state,
                        &new_state,
                        response.has_more_changes(),
                    ) {
                        yield ScopeLifecycleEvent::Terminated(
                            super::error::contract_violation(
                                bifrost_types::AccountOperation::ScopeLifecycle,
                                None,
                                fault.describe("Mailbox/changes"),
                            ),
                        );
                        break;
                    }

                    let fetched = if !created.is_empty() || !updated.is_empty() {
                        match fetch_mailboxes(&mail, &limits, created.iter().chain(&updated)).await {
                            Ok(fetched) => fetched,
                            Err(err) => {
                                let acct = super::error::into_account_error(
                                    err,
                                    super::error::JmapErrorContext::new(
                                        bifrost_types::AccountOperation::ScopeLifecycle,
                                    ),
                                );
                                // The shared state has not advanced. Reopening
                                // retries this response without losing its diff.
                                yield ScopeLifecycleEvent::Terminated(acct);
                                break;
                            }
                        }
                    } else {
                        FetchedMailboxes {
                            list: Vec::new(),
                            not_found: Vec::new(),
                        }
                    };

                    let FetchedMailboxes { list: fetched, not_found } = fetched;
                    let mut answered: Vec<String> = Vec::new();
                    for mailbox in fetched {
                        let id = mailbox.id().map(ToString::to_string);
                        let name = mailbox.name().unwrap_or("").to_string();
                        let Some(id) = id else {
                            continue;
                        };
                        answered.push(id.clone());

                        let old_name = replace_mailbox_name(
                            &mailbox_names,
                            id.clone(),
                            name.clone(),
                        )
                        .await;

                        // A mailbox this poller has no name for is one the
                        // consumer has never been told about, whichever
                        // change set the server filed it under. `updated`
                        // is not evidence of a prior announcement: the
                        // window opened at the seeded state, and a mailbox
                        // created before that state and renamed after it
                        // arrives as an update the names map has never
                        // seen. Emitting a rename from `old_name: ""` there
                        // asserts a previous name that never existed and
                        // hands the consumer a scope it must rename into
                        // place rather than create. Discovery is the honest
                        // event, and it is also the only one that
                        // establishes the scope.
                        let announced = created
                            .iter()
                            .all(|created_id| created_id.as_str() != id.as_str())
                            .then_some(old_name)
                            .flatten();

                        match announced {
                            None => {
                                yield ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                                    MembershipScope::Mailbox(bifrost_types::MailboxId(id)),
                                ));
                            }
                            Some(old_name) if old_name != name => {
                                let scope = MembershipScope::Mailbox(
                                    bifrost_types::MailboxId(id),
                                );
                                yield ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Renamed {
                                    old: scope.clone(),
                                    new: scope,
                                    old_name,
                                    new_name: name,
                                });
                            }
                            Some(_) => {}
                        }
                    }

                    // A mailbox named in `created`/`updated` that the
                    // follow-up `Mailbox/get` did not answer was destroyed
                    // between the two calls. The state below commits
                    // regardless - it must, since the mailbox is gone and
                    // no later poll will ever mention it again - so the
                    // create cannot be left for a replay that will not
                    // happen. Surfacing it as a create followed by a delete
                    // keeps the engine's per-folder cursor model sound: the
                    // scope is established and then torn down, which is
                    // what actually happened on the server, and a consumer
                    // that only ever hears the delete would carry no scope
                    // for it to apply to. Ids the same response already
                    // reports as `destroyed` are left to that loop.
                    //
                    // An id the server named in `notFound` is the same
                    // case and takes the same path: RFC 8620 s5.1 makes
                    // `notFound` and outright omission the two ways one
                    // `/get` can decline to answer an id, and neither
                    // says anything more than "this mailbox is not there
                    // any more". Reconciling against the SUBMITTED ids
                    // covers both at once - a `notFound` id is by
                    // construction absent from `list`, so it lands here -
                    // and `not_found` is bound out above so the decision
                    // is stated rather than left implicit.
                    debug_assert!(
                        !not_found.iter().any(|gone| answered.iter().any(|id| id == gone)),
                        "Mailbox/get answered and disclaimed the same id",
                    );
                    for missing in created.iter().chain(&updated) {
                        let id = missing.as_str();
                        if answered.iter().any(|answered| answered == id)
                            || destroyed.iter().any(|gone| gone.as_str() == id)
                        {
                            continue;
                        }
                        let known = remove_mailbox_name(&mailbox_names, id).await;
                        let scope = MembershipScope::Mailbox(
                            bifrost_types::MailboxId(id.to_string()),
                        );
                        // An id the consumer never heard of is not created
                        // and deleted for its benefit; it never existed as
                        // far as this stream is concerned.
                        if known.is_none() {
                            yield ScopeLifecycleEvent::Lifecycle(
                                ScopeLifecycle::Created(scope.clone()),
                            );
                        }
                        yield ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Deleted(scope));
                    }

                    for destroyed_id in destroyed {
                        let id = destroyed_id.into_string();
                        drop(remove_mailbox_name(&mailbox_names, &id).await);
                        yield ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Deleted(MembershipScope::Mailbox(
                            bifrost_types::MailboxId(id),
                        )));
                    }

                    // Commit only after every follow-up needed to describe
                    // this changes response has completed successfully.
                    state_cache::set(&mailbox_states, &account_id, new_state.clone()).await;

                    if !response.has_more_changes() {
                        // The burst is over; the next poll starts a new
                        // walk from the just-committed state.
                        state_walk.reset_changes_burst(&new_state);
                        yield ScopeLifecycleEvent::Polled;
                        poll_pause(&shutdown, &mut session_changes).await;
                    }
                }
                Err(err) => {
                    // This is a state-based request even though lifecycle
                    // has no durable engine cursor. Supply its Mailbox scope
                    // to the common classifier, then handle expiry locally:
                    // the lifecycle reader cannot restart a scope on its own.
                    let acct = super::error::into_account_error(
                        err,
                        state_walk.error_context(
                            bifrost_types::AccountOperation::ScopeLifecycle,
                        ),
                    );
                    if state_walk.should_snapshot_reseed(&acct) {
                        // Expired history is conforming, not a progress-guard
                        // fault. This state is private to lifecycle, so an engine
                        // scope restart cannot repair it. Reconcile a complete
                        // snapshot before replacing the position. Snapshot
                        // failures retain both
                        // the old names and state for another attempt.
                        match fetch_mailbox_names(&mail).await {
                            Ok((state, names)) => {
                                let events = {
                                    let previous = mailbox_names.lock().await;
                                    mailbox_snapshot_diff(&previous, &names)
                                };
                                for event in events {
                                    yield ScopeLifecycleEvent::Lifecycle(event);
                                }
                                // Publish neither map until all events have
                                // been yielded and both locks are held. A
                                // dropped stream must not leave fresh names
                                // hiding unannounced events on the next reseed.
                                let mut previous = mailbox_names.lock().await;
                                let mut states = mailbox_states.lock().await;
                                *previous = names;
                                states.insert(account_id.clone(), Some(state.clone()));
                                state_walk.reset_changes_burst(&state);
                            }
                            Err(err) => {
                                let error = super::error::into_account_error(
                                    err,
                                    super::error::JmapErrorContext::new(
                                        bifrost_types::AccountOperation::ScopeLifecycle,
                                    ),
                                );
                                yield ScopeLifecycleEvent::Terminated(error);
                                break;
                            }
                        }
                        yield ScopeLifecycleEvent::Polled;
                        poll_pause(&shutdown, &mut session_changes).await;
                        continue;
                    }
                    // Let the engine record holds and choose the reconnect
                    // delay. Do not silently absorb retry or reconcile advice.
                    yield ScopeLifecycleEvent::Terminated(acct);
                    break;
                }
            }
        }
    })
}

/// A fresh state cannot replay expired history. Report the net topology
/// difference instead; transient create/delete pairs entirely inside the lost
/// interval have no surviving scope to establish. Sort ids for deterministic
/// delivery, and do not hold the names lock while yielding to the consumer.
fn mailbox_snapshot_diff(
    previous: &HashMap<String, String>,
    current: &HashMap<String, String>,
) -> Vec<ScopeLifecycle> {
    let mut ids: Vec<_> = previous.keys().chain(current.keys()).collect();
    ids.sort_unstable();
    ids.dedup();
    ids.into_iter()
        .filter_map(|id| {
            let scope = MembershipScope::Mailbox(bifrost_types::MailboxId(id.clone()));
            match (previous.get(id), current.get(id)) {
                (None, Some(_)) => Some(ScopeLifecycle::Created(scope)),
                (Some(_), None) => Some(ScopeLifecycle::Deleted(scope)),
                (Some(old), Some(new)) if old != new => Some(ScopeLifecycle::Renamed {
                    old: scope.clone(),
                    new: scope,
                    old_name: old.clone(),
                    new_name: new.clone(),
                }),
                _ => None,
            }
        })
        .collect()
}

/// What one `Mailbox/get` follow-up learned about the ids a changes
/// response named: the objects that came back, and the ids the server
/// declared `notFound`.
struct FetchedMailboxes {
    list: Vec<Mailbox>,
    not_found: Vec<String>,
}

/// Read the named mailboxes, batched at `maxObjectsInGet`.
///
/// The id list is bounded by `maxChanges` on the `Mailbox/changes` call
/// that produced it, and `maxChanges` happens to be fed from the same
/// limit - but that is an accident of the call site, not a bound this
/// function may assume. RFC 8620 s5.1 lets a server refuse a `/get` with
/// `requestTooLarge` on its own advertised `maxObjectsInGet`, so the
/// batching is done here, where the limit is known.
async fn fetch_mailboxes<'a, T: HttpTransport>(
    mail: &MailAccount<T>,
    limits: &CoreLimits,
    ids: impl Iterator<Item = &'a MailboxId>,
) -> crate::Result<FetchedMailboxes> {
    let ids = ids.cloned().collect::<Vec<_>>();
    let mut fetched = FetchedMailboxes {
        list: Vec::new(),
        not_found: Vec::new(),
    };
    if ids.is_empty() {
        return Ok(fetched);
    }

    for chunk in ids.chunks(limits.max_objects_in_get.max(1)) {
        let response = mail
            .call(
                MailboxGet::new()
                    .ids(chunk.to_vec())
                    .properties([Property::Id, Property::Name]),
            )
            .await?;
        fetched
            .not_found
            .extend(response.not_found().iter().map(ToString::to_string));
        fetched.list.extend(response.into_list());
    }

    Ok(fetched)
}

async fn replace_mailbox_name(
    names: &Arc<Mutex<HashMap<String, String>>>,
    id: String,
    name: String,
) -> Option<String> {
    let mut guard = names.lock().await;
    guard.insert(id, name)
}

async fn remove_mailbox_name(
    names: &Arc<Mutex<HashMap<String, String>>>,
    id: &str,
) -> Option<String> {
    let mut guard = names.lock().await;
    guard.remove(id)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;

    use bifrost_types::{AccountErrorKind, EngineDirective, RecoveryClass, SyncStateErrorKind};
    use futures::StreamExt;

    use super::*;
    use crate::core::transport::TransportError;

    /// A JMAP HTTP boundary answering a fixed script of response bodies.
    /// Requests are recorded; an unscripted request is an error rather
    /// than a plausible answer, so a loop that polls more than the test
    /// planned for cannot pass quietly.
    type Recorder = Arc<StdMutex<Vec<serde_json::Value>>>;

    struct ScriptTransport {
        replies: StdMutex<VecDeque<String>>,
        requests: Recorder,
    }

    impl ScriptTransport {
        fn new(replies: impl IntoIterator<Item = String>) -> Self {
            Self::recording(replies, &Recorder::default())
        }

        fn recording(replies: impl IntoIterator<Item = String>, requests: &Recorder) -> Self {
            Self {
                replies: StdMutex::new(replies.into_iter().collect()),
                requests: Arc::clone(requests),
            }
        }
    }

    impl HttpTransport for ScriptTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, TransportError> {
            let request: serde_json::Value =
                serde_json::from_slice(&body).expect("client emits JSON");
            self.requests
                .lock()
                .expect("recorded requests")
                .push(request);
            // Exhaustion answers a TERMINAL JMAP error rather than a
            // transport failure. The lifecycle loop retries transport
            // hiccups forever by design, so a transport-shaped
            // exhaustion would turn "the loop polled more than planned"
            // into a hang instead of a failed assertion.
            match self.replies.lock().expect("script").pop_front() {
                Some(reply) => Ok(bytes::Bytes::from(reply)),
                None => Ok(bytes::Bytes::from(
                    serde_json::json!({
                        "sessionState": "session-1",
                        "methodResponses": [[
                            "error",
                            {"type": "invalidArguments", "description": "script exhausted"},
                            "s0"
                        ]]
                    })
                    .to_string(),
                )),
            }
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, TransportError> {
            Err(TransportError::new("no upload reply"))
        }

        async fn download(&self, _url: &str) -> Result<bytes::Bytes, TransportError> {
            Err(TransportError::new("no download reply"))
        }

        async fn get_session(&self, _url: &str) -> Result<bytes::Bytes, TransportError> {
            Err(TransportError::new("no session reply"))
        }
    }

    fn test_session() -> crate::core::session::Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {"primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}},
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    /// One `Mailbox/changes` answer, stamped with whatever
    /// `sessionState` the test wants the server to claim.
    fn mailbox_changes_reply(session_state: &str, created: &[&str]) -> String {
        serde_json::json!({
            "sessionState": session_state,
            "methodResponses": [[
                "Mailbox/changes",
                {
                    "accountId": "primary",
                    "oldState": "mbx-1",
                    "newState": "mbx-2",
                    "hasMoreChanges": false,
                    "created": created,
                    "updated": [],
                    "destroyed": []
                },
                "s0"
            ]]
        })
        .to_string()
    }

    fn mailbox_get_reply(session_state: &str, id: &str, name: &str) -> String {
        serde_json::json!({
            "sessionState": session_state,
            "methodResponses": [[
                "Mailbox/get",
                {
                    "accountId": "primary",
                    "state": "mbx-2",
                    "list": [{"id": id, "name": name}],
                    "notFound": []
                },
                "s0"
            ]]
        })
        .to_string()
    }

    fn skip_polled(
        stream: AccountStream<ScopeLifecycleEvent>,
    ) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(
            stream.filter(|event| {
                futures::future::ready(!matches!(event, ScopeLifecycleEvent::Polled))
            }),
        )
    }

    fn lifecycle_stream(
        replies: impl IntoIterator<Item = String>,
    ) -> (
        AccountStream<ScopeLifecycleEvent>,
        CancellationToken,
        StateMap,
    ) {
        lifecycle_stream_with_names(replies, HashMap::new())
    }

    fn lifecycle_stream_with_names(
        replies: impl IntoIterator<Item = String>,
        names: HashMap<String, String>,
    ) -> (
        AccountStream<ScopeLifecycleEvent>,
        CancellationToken,
        StateMap,
    ) {
        let (stream, shutdown, states, _) = lifecycle_stream_with_limit(replies, names, 256);
        (stream, shutdown, states)
    }

    /// The full-control variant: caller picks `maxObjectsInGet` and gets
    /// the recorded request log back.
    fn lifecycle_stream_with_limit(
        replies: impl IntoIterator<Item = String>,
        names: HashMap<String, String>,
        max_objects_in_get: usize,
    ) -> (
        AccountStream<ScopeLifecycleEvent>,
        CancellationToken,
        StateMap,
        Recorder,
    ) {
        let (stream, shutdown, states, recorder) =
            raw_lifecycle_stream_with_limit(replies, names, max_objects_in_get);
        (skip_polled(stream), shutdown, states, recorder)
    }

    fn raw_lifecycle_stream_with_limit(
        replies: impl IntoIterator<Item = String>,
        names: HashMap<String, String>,
        max_objects_in_get: usize,
    ) -> (
        AccountStream<ScopeLifecycleEvent>,
        CancellationToken,
        StateMap,
        Recorder,
    ) {
        let recorder = Recorder::default();
        let client = crate::client::Client::with_transport(
            ScriptTransport::recording(replies, &recorder),
            test_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::account::Account::new(client.clone(), "primary");
        let states: StateMap = Arc::new(Mutex::new(HashMap::from([(
            "primary".to_string(),
            Some("mbx-1".to_string()),
        )])));
        let shutdown = CancellationToken::new();
        let stream = scope_lifecycle(
            mail,
            CoreLimits {
                max_objects_in_get,
                max_objects_in_set: 256,
            },
            Arc::clone(&states),
            "primary".to_string(),
            Arc::new(Mutex::new(names)),
            shutdown.clone(),
            client,
        );
        (stream, shutdown, states, recorder)
    }

    fn method_failure(kind: &str) -> String {
        serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [["error", {"type": kind}, "s0"]]
        })
        .to_string()
    }

    fn snapshot_reply(state: &str, names: &[(&str, &str)]) -> String {
        let list: Vec<_> = names
            .iter()
            .map(|(id, name)| serde_json::json!({"id": id, "name": name}))
            .collect();
        serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [["Mailbox/get", {
                "accountId": "primary", "state": state,
                "list": list, "notFound": []
            }, "s0"]]
        })
        .to_string()
    }

    #[tokio::test(start_paused = true)]
    async fn a_completed_mailbox_changes_burst_yields_polled() {
        let mut first: serde_json::Value =
            serde_json::from_str(&mailbox_changes_reply("session-1", &[])).expect("fixture");
        first["methodResponses"][0][1]["hasMoreChanges"] = true.into();
        let mut last: serde_json::Value =
            serde_json::from_str(&mailbox_changes_reply("session-1", &[])).expect("fixture");
        last["methodResponses"][0][1]["oldState"] = "mbx-2".into();
        last["methodResponses"][0][1]["newState"] = "mbx-3".into();
        let (mut stream, shutdown, states, requests) = raw_lifecycle_stream_with_limit(
            [first.to_string(), last.to_string()],
            HashMap::new(),
            256,
        );
        assert!(matches!(
            stream.next().await,
            Some(ScopeLifecycleEvent::Polled)
        ));
        assert_eq!(requests.lock().expect("requests").len(), 2);
        assert_eq!(
            states.lock().await.get("primary"),
            Some(&Some("mbx-3".into()))
        );
        shutdown.cancel();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_name_hydration_yields_no_polled() {
        let (mut stream, _, states, _) = raw_lifecycle_stream_with_limit(
            [
                mailbox_changes_reply("session-1", &["new"]),
                method_failure("serverFail"),
            ],
            HashMap::new(),
            256,
        );
        assert!(matches!(
            stream.next().await,
            Some(ScopeLifecycleEvent::Terminated(_))
        ));
        assert!(stream.next().await.is_none());
        assert_eq!(
            states.lock().await.get("primary"),
            Some(&Some("mbx-1".into()))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expired_lifecycle_history_reconciles_and_keeps_polling() {
        for code in ["cannotCalculateChanges", "tooManyChanges"] {
            let (mut stream, shutdown, states, requests) = lifecycle_stream_with_limit(
                [
                    method_failure(code),
                    snapshot_reply(
                        "mbx-fresh",
                        &[("a-new", "New"), ("c-renamed", "After"), ("d-same", "Same")],
                    ),
                    mailbox_changes_reply("session-1", &["e-later"]),
                    mailbox_get_reply("session-1", "e-later", "Later"),
                ],
                HashMap::from([
                    ("b-gone".to_string(), "Gone".to_string()),
                    ("c-renamed".to_string(), "Before".to_string()),
                    ("d-same".to_string(), "Same".to_string()),
                ]),
                256,
            );
            let Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Mailbox(id),
            ))) = stream.next().await
            else {
                panic!("expected snapshot create for {code}");
            };
            assert_eq!(id.0, "a-new");
            let Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Deleted(
                MembershipScope::Mailbox(id),
            ))) = stream.next().await
            else {
                panic!("expected snapshot delete for {code}");
            };
            assert_eq!(id.0, "b-gone");
            let Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Renamed {
                old,
                new,
                old_name,
                new_name,
            })) = stream.next().await
            else {
                panic!("expected snapshot rename for {code}");
            };
            assert_eq!(
                old,
                MembershipScope::Mailbox(bifrost_types::MailboxId("c-renamed".into()))
            );
            assert_eq!(new, old);
            assert_eq!((old_name.as_str(), new_name.as_str()), ("Before", "After"));
            assert_eq!(
                state_cache::get(&states, "primary").await.as_deref(),
                Some("mbx-1")
            );
            // Pull past the snapshot's final yield, committing its state and
            // driving the next poll on Tokio's virtual clock.
            let Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Mailbox(id),
            ))) = stream.next().await
            else {
                panic!("lifecycle did not survive {code}");
            };
            assert_eq!(id.0, "e-later");
            assert_eq!(
                state_cache::get(&states, "primary").await.as_deref(),
                Some("mbx-fresh")
            );
            {
                let requests = requests.lock().expect("requests");
                assert_eq!(requests.len(), 4);
                assert_eq!(requests[1]["methodCalls"][0][0], "Mailbox/get");
                assert!(requests[1]["methodCalls"][0][1]["ids"].is_null());
                assert_eq!(requests[2]["methodCalls"][0][1]["sinceState"], "mbx-fresh");
            }
            shutdown.cancel();
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_changes_and_followup_failures_reach_the_lifecycle_consumer() {
        for replies in [
            vec![method_failure("serverUnavailable")],
            vec![
                changes_reply(&["new"], &[], &[]),
                method_failure("serverUnavailable"),
            ],
        ] {
            let expected_requests = replies.len();
            let (mut stream, _shutdown, states, requests) =
                lifecycle_stream_with_limit(replies, HashMap::new(), 256);
            let started = tokio::time::Instant::now();
            assert!(matches!(
                stream.next().await,
                Some(ScopeLifecycleEvent::Terminated(_))
            ));
            assert_eq!(started.elapsed(), std::time::Duration::ZERO);
            assert_eq!(requests.lock().expect("requests").len(), expected_requests);
            assert_eq!(
                state_cache::get(&states, "primary").await.as_deref(),
                Some("mbx-1"),
                "the replacement reader must retry the uncommitted position",
            );
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_transient_reseed_failure_preserves_the_old_names_and_position() {
        let (mut stream, shutdown, states, requests) = lifecycle_stream_with_limit(
            [
                method_failure("cannotCalculateChanges"),
                method_failure("serverUnavailable"),
            ],
            HashMap::from([("known".to_string(), "Before".to_string())]),
            256,
        );
        let started = tokio::time::Instant::now();
        assert!(matches!(
            stream.next().await,
            Some(ScopeLifecycleEvent::Terminated(_))
        ));
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
        assert_eq!(
            state_cache::get(&states, "primary").await.as_deref(),
            Some("mbx-1")
        );
        assert_eq!(requests.lock().expect("requests").len(), 2);
        assert!(stream.next().await.is_none());
        shutdown.cancel();
    }

    #[tokio::test]
    async fn dropping_a_partial_reseed_does_not_hide_undelivered_events() {
        let client = crate::client::Client::with_transport(
            ScriptTransport::new([
                method_failure("cannotCalculateChanges"),
                snapshot_reply("mbx-fresh", &[("a-new", "New"), ("b-new", "New")]),
                method_failure("cannotCalculateChanges"),
                snapshot_reply("mbx-fresh", &[("a-new", "New"), ("b-new", "New")]),
            ]),
            test_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::account::Account::new(client.clone(), "primary");
        let states = Arc::new(Mutex::new(HashMap::from([(
            "primary".to_string(),
            Some("mbx-1".to_string()),
        )])));
        let names = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = CancellationToken::new();
        let open_stream = || {
            skip_polled(scope_lifecycle(
                mail.clone(),
                CoreLimits {
                    max_objects_in_get: 256,
                    max_objects_in_set: 256,
                },
                Arc::clone(&states),
                "primary".to_string(),
                Arc::clone(&names),
                shutdown.clone(),
                client.clone(),
            ))
        };
        let mut first = open_stream();
        assert!(matches!(
            first.next().await,
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(_),))
        ));
        drop(first);
        assert!(names.lock().await.is_empty());
        assert_eq!(
            state_cache::get(&states, "primary").await.as_deref(),
            Some("mbx-1")
        );

        let mut resumed = open_stream();
        for expected in ["a-new", "b-new"] {
            let Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Mailbox(id),
            ))) = resumed.next().await
            else {
                panic!("missing replayed create");
            };
            assert_eq!(id.0, expected);
        }
        shutdown.cancel();
        assert!(resumed.next().await.is_none());
        assert_eq!(names.lock().await.len(), 2);
        assert_eq!(
            state_cache::get(&states, "primary").await.as_deref(),
            Some("mbx-fresh")
        );
    }

    #[tokio::test]
    async fn a_permanent_reseed_failure_terminates_without_advancing() {
        let (mut stream, shutdown, states) = lifecycle_stream([
            method_failure("cannotCalculateChanges"),
            method_failure("forbidden"),
        ]);
        let Some(ScopeLifecycleEvent::Terminated(error)) = stream.next().await else {
            panic!("expected permanent snapshot failure");
        };
        assert!(error.recovery().is_terminal());
        assert_eq!(
            state_cache::get(&states, "primary").await.as_deref(),
            Some("mbx-1")
        );
        assert!(stream.next().await.is_none());
        shutdown.cancel();
    }

    #[tokio::test]
    async fn a_mailbox_rename_carries_both_names_even_when_the_id_is_stable() {
        let changes = serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[
                "Mailbox/changes",
                {
                    "accountId": "primary",
                    "oldState": "mbx-1",
                    "newState": "mbx-2",
                    "hasMoreChanges": false,
                    "created": [],
                    "updated": ["mbx-1"],
                    "destroyed": []
                },
                "s0"
            ]]
        })
        .to_string();
        let (mut stream, shutdown, _) = lifecycle_stream_with_names(
            [changes, mailbox_get_reply("session-1", "mbx-1", "Renamed")],
            HashMap::from([("mbx-1".to_string(), "Old".to_string())]),
        );

        match stream.next().await {
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Renamed {
                old,
                new,
                old_name,
                new_name,
            })) => {
                assert_eq!(old, new);
                assert_eq!(old_name, "Old");
                assert_eq!(new_name, "Renamed");
            }
            other => panic!("expected named rename event, got {other:?}"),
        }
        shutdown.cancel();
    }

    /// One `Mailbox/changes` answer naming arbitrary change sets.
    fn changes_reply(created: &[&str], updated: &[&str], destroyed: &[&str]) -> String {
        serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[
                "Mailbox/changes",
                {
                    "accountId": "primary",
                    "oldState": "mbx-1",
                    "newState": "mbx-2",
                    "hasMoreChanges": false,
                    "created": created,
                    "updated": updated,
                    "destroyed": destroyed
                },
                "s0"
            ]]
        })
        .to_string()
    }

    /// An `updated` mailbox this poller holds no name for was never
    /// announced to the consumer, so there is no old name to rename FROM.
    /// It used to emit `Renamed { old_name: "" }`, asserting a previous
    /// name that never existed and handing the consumer a scope to move
    /// rather than one to create.
    #[tokio::test]
    async fn an_updated_mailbox_with_no_known_name_is_a_discovery() {
        let (mut stream, shutdown, _) = lifecycle_stream([
            changes_reply(&[], &["mbx-unknown"], &[]),
            mailbox_get_reply("session-1", "mbx-unknown", "Archive"),
        ]);

        match stream.next().await {
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Mailbox(id),
            ))) => assert_eq!(id.0, "mbx-unknown"),
            other => panic!("expected a discovery, got {other:?}"),
        }
        shutdown.cancel();
    }

    /// A mailbox created and then destroyed between `Mailbox/changes` and
    /// the follow-up `Mailbox/get` is answered by neither call, yet the
    /// loop commits the new state past it - it must, since no later poll
    /// will ever mention that id again. The create therefore has to be
    /// surfaced here or it is lost forever; it is surfaced together with
    /// the deletion that overtook it, so the consumer establishes the
    /// scope and tears it down instead of receiving a delete for a scope
    /// it never had.
    #[tokio::test]
    async fn a_create_that_vanished_before_the_read_is_created_then_deleted() {
        let empty_get = serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[
                "Mailbox/get",
                {
                    "accountId": "primary",
                    "state": "mbx-2",
                    "list": [],
                    "notFound": ["mbx-ghost"]
                },
                "s0"
            ]]
        })
        .to_string();
        let (mut stream, shutdown, states) =
            lifecycle_stream([changes_reply(&["mbx-ghost"], &[], &[]), empty_get]);

        match stream.next().await {
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Mailbox(id),
            ))) => assert_eq!(id.0, "mbx-ghost"),
            other => panic!("expected the create to be surfaced, got {other:?}"),
        }
        match stream.next().await {
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Deleted(
                MembershipScope::Mailbox(id),
            ))) => assert_eq!(id.0, "mbx-ghost"),
            other => panic!("expected the deletion that overtook it, got {other:?}"),
        }
        shutdown.cancel();
        assert!(stream.next().await.is_none());
        assert_eq!(
            state_cache::get(&states, "primary").await,
            Some("mbx-2".to_string()),
            "the state still advances; the vanished create is not replayable"
        );
    }

    /// One `Mailbox/changes` answer with full control over the state it
    /// reports and whether it claims more pages behind it.
    fn paged_changes_reply(new_state: &str, has_more: bool, created: &[&str]) -> String {
        serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[
                "Mailbox/changes",
                {
                    "accountId": "primary",
                    "oldState": "mbx-1",
                    "newState": new_state,
                    "hasMoreChanges": has_more,
                    "created": created,
                    "updated": [],
                    "destroyed": []
                },
                "s0"
            ]]
        })
        .to_string()
    }

    /// `Mailbox/changes` is paginated here on the lifecycle poller's own
    /// loop, so it has the same unbounded shape `changes.rs` guards:
    /// `hasMoreChanges: true` with an unmoved `newState` never terminates.
    /// This stream is worse off than a change walk, because the engine
    /// drives it for the life of the account - the spin is permanent.
    ///
    /// Ablation shape, for this test and its oscillating sibling: asserting
    /// only the error KIND does not bite, because the scripted transport's
    /// exhaustion reply also classifies `Protocol(ContractViolation)`, so an
    /// unguarded loop that spins into the empty script fails the same way.
    /// The recorded REQUEST COUNT is what tells a guarded walk from a
    /// spinning one.
    #[tokio::test]
    async fn a_stuck_lifecycle_state_terminates_instead_of_looping() {
        let (mut stream, shutdown, states, requests) = lifecycle_stream_with_limit(
            [paged_changes_reply("mbx-1", true, &[])],
            HashMap::new(),
            256,
        );

        match stream.next().await {
            Some(ScopeLifecycleEvent::Terminated(err)) => {
                assert_eq!(
                    err.kind(),
                    &AccountErrorKind::Protocol(
                        bifrost_types::ProtocolErrorKind::ContractViolation
                    ),
                    "an unmoved state under hasMoreChanges is a contract breach"
                );
            }
            other => panic!("expected a contract-violation termination, got {other:?}"),
        }
        assert!(stream.next().await.is_none());
        assert_eq!(
            state_cache::get(&states, "primary").await,
            Some("mbx-1".to_string()),
            "the refused page is not committed; the state never moved"
        );
        // The count is the load-bearing half: an unguarded loop polls
        // again immediately (`hasMoreChanges` is true and there is no
        // pause), forever against a real server.
        assert_eq!(
            requests.lock().expect("recorded requests").len(),
            1,
            "the poller must not ask again after a page that did not move"
        );
        shutdown.cancel();
    }

    /// The oscillation the single-step guard cannot see: every step
    /// "moves" the state, and the pair repeats forever.
    #[tokio::test]
    async fn an_oscillating_lifecycle_state_terminates_instead_of_looping() {
        let (mut stream, shutdown, _states, requests) = lifecycle_stream_with_limit(
            [
                // mbx-1 -> mbx-2, served.
                paged_changes_reply("mbx-2", true, &[]),
                // mbx-2 -> mbx-1, a state this burst already resumed from.
                paged_changes_reply("mbx-1", true, &[]),
            ],
            HashMap::new(),
            256,
        );

        match stream.next().await {
            Some(ScopeLifecycleEvent::Terminated(err)) => assert_eq!(
                err.kind(),
                &AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ContractViolation),
                "a repeated state is a cycle, not progress"
            ),
            other => panic!("expected a contract-violation termination, got {other:?}"),
        }
        assert_eq!(
            requests.lock().expect("recorded requests").len(),
            2,
            "the walk stops at the repeat rather than paginating on"
        );
        shutdown.cancel();
    }

    /// The follow-up `Mailbox/get` was bounded only incidentally, by the
    /// `maxChanges` the changes call happened to carry. It must batch on
    /// its own `maxObjectsInGet`: five ids under a limit of two are three
    /// `Mailbox/get` calls, none of them over the limit.
    #[tokio::test]
    async fn the_lifecycle_mailbox_read_batches_at_max_objects_in_get() {
        let ids = ["m1", "m2", "m3", "m4", "m5"];
        let (mut stream, shutdown, _states, requests) = lifecycle_stream_with_limit(
            [
                paged_changes_reply("mbx-2", false, &ids),
                mailbox_get_reply("session-1", "m1", "One"),
                mailbox_get_reply("session-1", "m3", "Three"),
                mailbox_get_reply("session-1", "m5", "Five"),
            ],
            HashMap::new(),
            2,
        );

        // Drain the three creates the gets did answer plus the
        // created/deleted pairs for the ids they left out.
        for _ in 0..3 {
            assert!(matches!(
                stream.next().await,
                Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(_)))
            ));
        }
        shutdown.cancel();
        let _drained: Vec<_> = stream.collect().await;

        let gets: Vec<Vec<String>> = requests
            .lock()
            .expect("recorded requests")
            .iter()
            .filter_map(|request| {
                let call = request.get("methodCalls")?.get(0)?;
                (call.get(0)?.as_str()? == "Mailbox/get").then(|| {
                    call.get(1)
                        .and_then(|args| args.get("ids"))
                        .and_then(serde_json::Value::as_array)
                        .expect("a Mailbox/get carries ids")
                        .iter()
                        .map(|id| id.as_str().expect("string id").to_string())
                        .collect()
                })
            })
            .collect();
        assert_eq!(
            gets,
            vec![
                vec!["m1".to_string(), "m2".to_string()],
                vec!["m3".to_string(), "m4".to_string()],
                vec!["m5".to_string()],
            ],
            "the read batches at maxObjectsInGet rather than sending one call"
        );
    }

    /// The engine drives the scope-lifecycle stream for the whole life
    /// of an account, which makes it the one always-running consumer
    /// positioned to notice that the server has moved to a different
    /// session. It is not enough for the client's staleness flag to
    /// flip: `JmapAccount` froze its limits, capability set, primary and
    /// foreign routing, and push topology from the OLD session document,
    /// so the only correct consumer action is a full reopen. This pins
    /// the whole path - divergence on the wire, through the detector,
    /// into a classified `Terminated`, out as `RestartAccount`.
    #[tokio::test(start_paused = true)]
    async fn a_diverged_session_state_terminates_the_lifecycle_into_a_reopen() {
        // The server answers the first poll while claiming a session
        // the client has never read.
        let started = tokio::time::Instant::now();
        let (stream, _shutdown, _states) =
            lifecycle_stream([mailbox_changes_reply("session-2", &[])]);
        let events: Vec<_> = stream.collect().await;
        assert_eq!(
            tokio::time::Instant::now(),
            started,
            "the response-boundary signal must wake lifecycle immediately"
        );

        match events.as_slice() {
            [ScopeLifecycleEvent::Terminated(err)] => {
                assert_eq!(
                    err.kind(),
                    &AccountErrorKind::SyncState(SyncStateErrorKind::CapabilityChanged),
                    "divergence is a capability-shift class, not a transport error"
                );
                assert_eq!(
                    err.recovery(),
                    &RecoveryClass::Engine(EngineDirective::RestartAccount),
                    "an in-place refresh cannot heal a frozen account; the engine must reopen"
                );
            }
            other => panic!("expected a single terminal reopen directive, got {other:?}"),
        }
    }

    /// The other half of the guard's bite: a server that keeps its
    /// session must not have its lifecycle stream torn down. Without
    /// this, a detector wired backwards - or one that fired on every
    /// pass - would still satisfy the divergence test above while
    /// making the account unusable.
    #[tokio::test]
    async fn a_matching_session_state_keeps_the_lifecycle_stream_running() {
        let (mut stream, shutdown, states) = lifecycle_stream([
            mailbox_changes_reply("session-1", &["mbx-new"]),
            mailbox_get_reply("session-1", "mbx-new", "Archive"),
        ]);

        match stream.next().await {
            Some(ScopeLifecycleEvent::Lifecycle(ScopeLifecycle::Created(
                MembershipScope::Mailbox(id),
            ))) => assert_eq!(id.0, "mbx-new"),
            other => panic!("expected the created-mailbox lifecycle event, got {other:?}"),
        }
        // Cancelling before the next pull lets the loop run past its
        // commit and out through the top-of-iteration check, rather than
        // parking on the poll interval. That the stream ends here at all
        // is the cancellation guarantee `close()` depends on.
        shutdown.cancel();
        assert!(
            stream.next().await.is_none(),
            "a cancelled token ends the loop instead of sleeping out the interval"
        );
        // The poller committed its OWN position, and only after every
        // follow-up describing that response succeeded. Nothing else
        // writes this map, which is the whole reason it is separate from
        // the shared `mailbox_states` cache that a delta pass or a local
        // `Mailbox/set` may fast-forward past unseen lifecycle events.
        assert_eq!(
            state_cache::get(&states, "primary").await,
            Some("mbx-2".to_string()),
            "the lifecycle poller advances the map it was handed"
        );
    }
}
