use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bifrost_types::{
    AccountStream, Batch, BatchItemId, BatchSuccess, HydratedObject, HydratedObjectKind,
    ItemOutcome, ObjectId, PageBoundary, Projection, SyncEvent,
};
use futures::StreamExt;

use crate::core::transport::HttpTransport;
use crate::email::{EmailGet, EmailId, Property};

use super::capabilities::CoreLimits;
use super::inventory::{email_to_inventory, inventory_properties};

type MailAccount<T> = crate::account::Account<T>;

// JMAP advertises this same `max_wait` through its BatchingPolicy. Hydration
// receives CoreLimits, which currently carries only the wire object limits;
// keep the timer local rather than widening account construction and every
// CoreLimits fixture just to pass this one value.
const ROUTE_MAX_WAIT: Duration = Duration::from_millis(100);

struct PendingHydration {
    ids: Vec<ObjectId>,
    first_touch: tokio::time::Instant,
    last_touch: tokio::time::Instant,
}

impl PendingHydration {
    fn new(now: tokio::time::Instant) -> Self {
        Self {
            ids: Vec::new(),
            first_touch: now,
            last_touch: now,
        }
    }

    fn push(&mut self, id: ObjectId, now: tokio::time::Instant) {
        self.ids.push(id);
        self.last_touch = now;
    }

    /// The policy has one duration, so a quiet route flushes after that idle
    /// period and a busy route still flushes no later than that duration from
    /// its first item. Tracking both ends prevents a steady trickle from
    /// resetting the maximum wait forever.
    fn deadline(&self, max_wait: Duration) -> tokio::time::Instant {
        (self.last_touch + max_wait).min(self.first_touch + max_wait)
    }

    fn due_at(&self, now: tokio::time::Instant, max_wait: Duration) -> bool {
        now >= self.deadline(max_wait)
    }
}

/// Drain routes whose object cap or wait deadline has arrived, in stable
/// primary-then-account-id order. Hydration has no local rejection lane, so
/// its buffers do not need mutation's owner registration queue.
fn take_ready_routes(
    buffers: &mut HashMap<HydrationRoute, PendingHydration>,
    now: tokio::time::Instant,
    max_items: usize,
    max_wait: Duration,
    flush_all: bool,
) -> Vec<(HydrationRoute, Vec<ObjectId>)> {
    let mut ready: Vec<_> = buffers
        .iter()
        .filter(|(_, buffer)| {
            flush_all || buffer.ids.len() >= max_items || buffer.due_at(now, max_wait)
        })
        .map(|(route, _)| route.clone())
        .collect();
    ready.sort_by(|left, right| flush_order(left).cmp(&flush_order(right)));
    ready
        .into_iter()
        .filter_map(|route| buffers.remove(&route).map(|buffer| (route, buffer.ids)))
        .collect()
}

/// Which JMAP account an incoming hydration id routes to.
///
/// `Primary` is a bare native id. `Foreign(accountId)` is an id the foreign
/// inventory / changes projection qualified with its owning account, and it
/// MUST be fetched against that account's handle: `Email/get` is
/// accountId-scoped, so running it on the primary account either 404s or
/// resolves an unrelated primary object with the same id.
///
/// An id that parses as foreign but names an account this session has no
/// handle for routes `Primary` deliberately: the account disappeared
/// (revoked delegation), and the primary `Email/get` will report the id as
/// not found - which is the honest answer for an id we can no longer reach.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum HydrationRoute {
    Primary,
    Foreign(String),
}

/// Deterministic flush ordering for the trailing per-route buffers:
/// primary first, then foreign accounts by account id.
fn flush_order(route: &HydrationRoute) -> (u8, &str) {
    match route {
        HydrationRoute::Primary => (0, ""),
        HydrationRoute::Foreign(account) => (1, account.as_str()),
    }
}

/// Route one hydration id. Pure over the registration predicate so the
/// selection is unit-pinnable without a live session.
pub(crate) fn route_for_id<F>(id: &ObjectId, is_registered: F) -> HydrationRoute
where
    F: Fn(&str) -> bool,
{
    match super::foreign::parse_object(&id.0) {
        Some((account, _)) if is_registered(account) => {
            HydrationRoute::Foreign(account.to_string())
        }
        _ => HydrationRoute::Primary,
    }
}

pub(crate) fn stream<T: HttpTransport>(
    mail: MailAccount<T>,
    foreign_mail: Arc<HashMap<String, MailAccount<T>>>,
    limits: CoreLimits,
    mut ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    Box::pin(async_stream::stream! {
        if !matches!(projection, Projection::FlagsOnly | Projection::Metadata) {
            yield super::error::terminated_unsupported(
                bifrost_types::AccountOperation::Hydrate,
                None,
                "JMAP raw-MIME hydration projections need a MIME assembly path outside this wave",
            );
            return;
        }

        let limit = limits.max_objects_in_get.max(1);
        // One buffer per routing target: ids for the primary account and
        // ids for each foreign account cannot ride in the same
        // `Email/get`, because the call is accountId-scoped. Unlike the
        // mutation RouteBuffers, this path must flush on elapsed time while
        // `ids.next()` is pending; mutation's `last_touch` is an input-count
        // fairness bound and does not implement BatchingPolicy.max_wait.
        let mut buffers: HashMap<HydrationRoute, PendingHydration> = HashMap::new();
        loop {
            let deadline = buffers
                .values()
                .map(|buffer| buffer.deadline(ROUTE_MAX_WAIT))
                .min();
            let input = tokio::select! {
                biased;
                _ = async {
                    if let Some(deadline) = deadline {
                        tokio::time::sleep_until(deadline).await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => None,
                input = ids.next() => Some(input),
            };
            let now = tokio::time::Instant::now();
            let (ready, at_end) = match input {
                None => (
                    take_ready_routes(&mut buffers, now, limit, ROUTE_MAX_WAIT, false),
                    false,
                ),
                Some(Some(id)) => {
                    let route = route_for_id(&id, |account| foreign_mail.contains_key(account));
                    buffers
                        .entry(route)
                        .or_insert_with(|| PendingHydration::new(now))
                        .push(id, now);
                    (
                        take_ready_routes(&mut buffers, now, limit, ROUTE_MAX_WAIT, false),
                        false,
                    )
                }
                Some(None) => (
                    take_ready_routes(&mut buffers, now, limit, ROUTE_MAX_WAIT, true),
                    true,
                ),
            };

            for (route, mut batch) in ready {
                match fetch_route(&mail, &foreign_mail, &route, projection, &mut batch).await {
                    Ok(Some(batch)) => yield SyncEvent::Batch(batch),
                    Ok(None) => {}
                    Err(err) => {
                        yield super::error::terminated_from_jmap(
                            err,
                            super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Hydrate),
                        );
                        return;
                    }
                }
            }

            if at_end {
                break;
            }
        }

        yield SyncEvent::Done(None);
    })
}

/// Select the account handle for a route and fetch its buffered ids. A
/// foreign route whose handle vanished mid-stream falls back to the primary
/// handle rather than dropping the ids silently (the primary `Email/get`
/// then reports them as not found).
async fn fetch_route<T: HttpTransport>(
    mail: &MailAccount<T>,
    foreign_mail: &HashMap<String, MailAccount<T>>,
    route: &HydrationRoute,
    projection: Projection,
    batch: &mut Vec<ObjectId>,
) -> crate::Result<Option<Batch<ItemOutcome<HydratedObject>>>> {
    let (handle, owner) = match route {
        HydrationRoute::Primary => (mail, None),
        HydrationRoute::Foreign(account) => match foreign_mail.get(account) {
            Some(handle) => (handle, Some(account.as_str())),
            None => {
                // A route minted while the handle existed, drained after
                // it vanished (the map is fixed per account instance, so
                // this means a routing bug rather than a live topology
                // change). Degrading to the primary with the LITERAL id
                // is safe - the qualified form cannot collide with a
                // real primary id and comes back notFound - but it must
                // not be silent: the same condition at open produces a
                // SkippedScope, and a warn is the stream-side analog.
                tracing::warn!(
                    target: "bifrost.jmap.hydrate",
                    account,
                    "foreign hydration route has no handle; ids run on \
                     the primary route in literal form"
                );
                (mail, None)
            }
        },
    };
    fetch_batch(handle, projection, batch, owner).await
}

/// The wire form of a routed hydration id: the native id when the batch
/// is running against the id's own foreign account, the submitted id
/// LITERAL otherwise.
///
/// The literal fallback is what keeps the unregistered-foreign case
/// honest, and it must match what the mutation pipeline
/// (`mutation::wire_email_id`, `pim::wire_id_for_mail`) does: an id
/// qualified for an account this session has no handle for runs on the
/// primary route, and stripping it there would send the bare native id
/// to the PRIMARY `Email/get` - which, on an id collision, resolves an
/// unrelated primary message and reports its data under the foreign id.
/// Kept literal, the id cannot name any real primary object (`\u{1f}`
/// never occurs in an RFC 8620 id), so the server answers `notFound` -
/// the honest result for an id we can no longer reach.
pub(crate) fn wire_object_id<'a>(id: &'a ObjectId, owner: Option<&str>) -> &'a str {
    match (owner, super::foreign::parse_object(&id.0)) {
        (Some(owner), Some((account, native))) if account == owner => native,
        _ => &id.0,
    }
}

/// Fetch one route's buffered ids and reconcile the answer against them.
///
/// Outcome ids are the ids the CALLER submitted, verbatim - the foreign
/// qualification already rides on them, so nothing is re-encoded from the
/// native id the server echoed back.
async fn fetch_batch<T: HttpTransport>(
    mail: &MailAccount<T>,
    projection: Projection,
    batch: &mut Vec<ObjectId>,
    owner: Option<&str>,
) -> crate::Result<Option<Batch<ItemOutcome<HydratedObject>>>> {
    let started = Instant::now();
    let requested: Vec<ObjectId> = std::mem::take(batch);
    if requested.is_empty() {
        return Ok(None);
    }
    // Exactly one `/jmap/api` POST per emitted batch on this path, so
    // the accumulator is created and read within this call rather than
    // hoisted to the stream: routing splits a chunk across the primary
    // and each foreign account, and each of those emits its own batch.
    let (mail, tally) = mail.metered();
    // The wire call takes the native id only when the batch runs against
    // the id's own foreign account; an unregistered-foreign id stays
    // literal on the primary route (see `wire_object_id`).
    // Preserve repeated submissions on the wire and in the outcome slice:
    // the engine-facing contract is one outcome per submitted occurrence.
    // JMAP returns one object per id, and `reconcile_hydration` fans that
    // correlated object back out to each matching occurrence.
    let ids = requested
        .iter()
        .map(|id| EmailId::new(wire_object_id(id, owner).to_string()))
        .collect::<Vec<_>>();
    let properties = properties_for_projection(projection);
    let response = mail
        .call(EmailGet::new().ids(ids).properties(properties))
        .await?;

    let state = response.state().to_string();
    let not_found = response.not_found().to_vec();
    let items = reconcile_hydration(
        &requested,
        response.into_list(),
        &not_found,
        projection,
        &state,
        owner,
    );

    if items.is_empty() {
        return Ok(None);
    }

    Ok(Some(Batch {
        items,
        page_boundary: PageBoundary::Page,
        server_latency: started.elapsed(),
        bytes_in: tally.take(),
        checkpoint: None,
    }))
}

/// Reconcile one `Email/get` answer against the ids that were submitted.
///
/// Pure, so the accounting is unit-pinnable without a live transport.
///
/// The closed per-item contract is "every submitted id leaves on exactly
/// one lane", and neither half of the response can carry that alone:
///
/// - `notFound` decodes as empty when the server omits it (the decoder is
///   deliberately lenient so one absent empty array cannot fail every
///   sibling call in the request), and a present `notFound` can still miss
///   an id the server also left out of `list`.
/// - `list` can echo an id that was never requested, or echo one twice.
///
/// So ids are matched from the SUBMITTED side. Anything the server sent
/// that does not correlate is discarded rather than minted into an
/// outcome, and any submitted id left over becomes a retryable
/// `Protocol(PartialResponse)` - not a terminal contract violation, which
/// would drop the id permanently for a condition the next `Email/get`
/// usually clears.
fn reconcile_hydration(
    requested: &[ObjectId],
    list: Vec<crate::email::Email>,
    not_found: &[EmailId],
    projection: Projection,
    state: &str,
    owner: Option<&str>,
) -> Vec<ItemOutcome<HydratedObject>> {
    // Keyed by the exact id that went on the wire (`wire_object_id`):
    // native for the routed foreign owner, literal otherwise - so an
    // unregistered-foreign id can never be correlated with a primary
    // object that happens to share its native part.
    let requested_native: HashSet<&str> = requested
        .iter()
        .map(|id| wire_object_id(id, owner))
        .collect();
    // Index the answer by native id, keeping only objects that were
    // actually asked for. An object with no `id`, or one the batch never
    // submitted, cannot be correlated with anything and is dropped rather
    // than minted into an outcome for something nobody requested; the
    // submitted id it failed to answer is picked up by the sweep below.
    // A server that echoes the same id twice supplies one object: first
    // wins, so a duplicated answer cannot displace the correlated one.
    let mut by_native = HashMap::new();
    for email in list {
        let Some(native) = email.id().map(ToString::to_string) else {
            continue;
        };
        if requested_native.contains(native.as_str()) {
            by_native.entry(native).or_insert(email);
        }
    }
    let not_found: HashSet<&str> = not_found.iter().map(EmailId::as_str).collect();
    let mut items = Vec::new();

    // Drive from the submitted slice, including repeated ids. One server
    // object therefore satisfies every occurrence of that submitted id,
    // preserving the stream's one-outcome-per-submission contract.
    for id in requested {
        let native = wire_object_id(id, owner);
        let Some(email) = by_native.get(native).cloned() else {
            let error = if not_found.contains(native) {
                super::error::get_id_not_found(
                    id.0.clone(),
                    super::error::JmapErrorContext::message(
                        bifrost_types::AccountOperation::Hydrate,
                        id.0.clone(),
                    ),
                )
            } else {
                super::error::get_id_unanswered(
                    &id.0,
                    super::error::JmapErrorContext::message(
                        bifrost_types::AccountOperation::Hydrate,
                        id.0.clone(),
                    ),
                )
            };
            items.push(ItemOutcome::Failed(bifrost_types::BatchFailure::new(
                BatchItemId(id.0.clone()),
                error,
            )));
            continue;
        };
        if matches!(projection, Projection::FlagsOnly) && !email.has_keywords() {
            items.push(ItemOutcome::Failed(bifrost_types::BatchFailure::new(
                BatchItemId(id.0.clone()),
                super::error::missing_property(
                    id.0.clone(),
                    "keywords",
                    super::error::JmapErrorContext::message(
                        bifrost_types::AccountOperation::Hydrate,
                        id.0.clone(),
                    ),
                ),
            )));
            continue;
        }
        let id = id.clone();
        let kind = match projection {
            Projection::FlagsOnly => HydratedObjectKind::FlagsOnly(
                email
                    .keywords()
                    .into_iter()
                    .map(str::to_string)
                    .collect::<HashSet<_>>(),
            ),
            Projection::Metadata => {
                let mut entry = email_to_inventory(email, state);
                // A foreign-qualified id must yield a foreign-qualified
                // entry, exactly as the foreign inventory walk mints one.
                // This is the attribution channel for foreign changes:
                // the account-level change stream emits only qualified
                // ids, and the consumer learns WHICH shared folder the
                // message sits in from these memberships. Left bare, the
                // native `mailboxIds` collide with same-id primary
                // mailboxes, and the bare entry/blob ids would route
                // later reads through the primary account.
                // Gated on the ROUTE owner, not on parsing the submitted
                // id: an unregistered-foreign id rides the primary route
                // with its literal wire form, and qualifying its entry
                // under the dead account would double-encode the ids.
                if let Some(owner) = owner {
                    let owner = bifrost_types::MailboxId(owner.to_string());
                    super::inventory::qualify_foreign_memberships(&mut entry.memberships, &owner);
                    super::inventory::qualify_foreign_ids(&mut entry, &owner);
                }
                HydratedObjectKind::Metadata(entry)
            }
            _ => HydratedObjectKind::FlagsOnly(HashSet::new()),
        };
        let item_id = BatchItemId(id.0.clone());
        let hydrated = HydratedObject {
            id,
            kind,
            blobs: Vec::new(),
        };
        items.push(ItemOutcome::Succeeded(BatchSuccess::new(item_id, hydrated)));
    }

    items
}

fn properties_for_projection(projection: Projection) -> Vec<Property> {
    match projection {
        Projection::FlagsOnly => vec![Property::Id, Property::Keywords, Property::MailboxIds],
        Projection::Metadata => inventory_properties(),
        _ => vec![Property::Id],
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use serde_json::{Value, json};

    use super::*;
    use crate::core::transport::{HttpTransport, TransportError};

    #[test]
    fn flags_only_email_without_keywords_is_a_missing_field_failure() {
        let requested = [ObjectId("m1".into()), ObjectId("m2".into())];
        let items = reconcile_hydration(
            &requested,
            vec![
                serde_json::from_value(json!({ "id": "m1" })).unwrap(),
                email("m2"),
            ],
            &[],
            Projection::FlagsOnly,
            "s1",
            None,
        );
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[0], ItemOutcome::Failed(f) if matches!(
            f.error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::MissingField
            )
        )));
        assert!(matches!(&items[1], ItemOutcome::Succeeded(_)));
    }

    struct HydrationTransport {
        requests: Arc<Mutex<Vec<Value>>>,
    }

    impl HttpTransport for HydrationTransport {
        async fn api_request(&self, _url: &str, body: Vec<u8>) -> Result<Bytes, TransportError> {
            let request: Value = serde_json::from_slice(&body)
                .map_err(|error| TransportError::with_source("request is not JSON", error))?;
            self.requests
                .lock()
                .expect("request log")
                .push(request.clone());
            let call = &request["methodCalls"][0];
            let args = &call[1];
            let list: Vec<Value> = args["ids"]
                .as_array()
                .expect("Email/get ids")
                .iter()
                .map(|id| json!({ "id": id, "keywords": { "$seen": true } }))
                .collect();
            let response = json!({
                "sessionState": "session-1",
                "methodResponses": [[
                    call[0],
                    {
                        "accountId": args["accountId"],
                        "state": "mail-state-2",
                        "list": list,
                        "notFound": []
                    },
                    call[2]
                ]]
            });
            Ok(Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("hydration test does not upload"))
        }

        async fn download(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("hydration test does not download"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new(
                "hydration test does not refresh session",
            ))
        }
    }

    fn hydration_session() -> crate::core::session::Session {
        serde_json::from_value(json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/api",
            "downloadUrl": "https://example.test/dl/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/es",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    #[test]
    fn foreign_id_routes_to_the_foreign_account_and_strips_to_native() {
        let registered: HashSet<String> = ["acct-9".to_string()].into_iter().collect();
        let is_registered = |account: &str| registered.contains(account);

        let foreign = ObjectId(super::super::foreign::encode_object("acct-9", "M1"));
        assert_eq!(
            route_for_id(&foreign, is_registered),
            HydrationRoute::Foreign("acct-9".to_string()),
        );
        // The `Email/get` call carries the bare native id; the accountId
        // rides on the selected handle, not the id string.
        assert_eq!(super::super::foreign::native_object(&foreign.0), "M1");

        // A bare id is primary.
        assert_eq!(
            route_for_id(&ObjectId("M1".to_string()), is_registered),
            HydrationRoute::Primary,
        );
        // An id naming an account this session cannot reach falls back to
        // primary rather than fabricating a handle.
        let gone = ObjectId(super::super::foreign::encode_object("acct-gone", "M1"));
        assert_eq!(route_for_id(&gone, is_registered), HydrationRoute::Primary);
    }

    #[test]
    fn each_projection_requests_the_properties_it_actually_reads() {
        // `FlagsOnly` builds its outcome from `keywords`, so dropping that
        // property would silently hydrate every message with an empty flag
        // set instead of failing. `Metadata` must ask for exactly the
        // inventory property set, or a hydrated entry and the inventory
        // entry it replaces would carry different fingerprints.
        let flags_only = properties_for_projection(Projection::FlagsOnly);
        assert!(flags_only.contains(&Property::Keywords));
        assert!(flags_only.contains(&Property::Id));

        assert_eq!(
            properties_for_projection(Projection::Metadata),
            inventory_properties()
        );
    }

    #[test]
    fn a_foreign_route_and_a_primary_route_never_share_a_batch_buffer() {
        // `stream` keys its per-request buffers on `HydrationRoute`, so the
        // routes must not compare equal - one `Email/get` is scoped to one
        // accountId and cannot carry ids from two accounts.
        assert_ne!(
            HydrationRoute::Primary,
            HydrationRoute::Foreign("acct-9".to_string())
        );
        assert_ne!(
            HydrationRoute::Foreign("acct-9".to_string()),
            HydrationRoute::Foreign("acct-7".to_string())
        );
    }

    #[test]
    fn later_items_do_not_reset_a_routes_max_wait_deadline() {
        let first = tokio::time::Instant::now();
        let max_wait = Duration::from_millis(100);
        let mut buffer = PendingHydration::new(first);
        buffer.push(
            ObjectId("m1".to_string()),
            first + Duration::from_millis(80),
        );

        assert_eq!(buffer.deadline(max_wait), first + max_wait);
        assert!(buffer.due_at(first + max_wait, max_wait));
    }

    /// A single partial route must flush while its producer is still open.
    /// Paused Tokio time advances to the policy deadline without wall-clock
    /// sleeping, and the request remains scoped to the foreign account.
    #[tokio::test(start_paused = true)]
    async fn a_partial_route_flushes_at_max_wait_while_input_is_pending() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let client = crate::client::Client::with_transport(
            HydrationTransport {
                requests: Arc::clone(&requests),
            },
            hydration_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let mail = crate::account::Account::new(client.clone(), "primary");
        let foreign_mail = Arc::new(HashMap::from([(
            "shared".to_string(),
            crate::account::Account::new(client, "shared"),
        )]));
        let foreign_id = ObjectId(super::super::foreign::encode_object("shared", "M1"));
        let yielded_id = foreign_id.clone();
        let input: AccountStream<ObjectId> = Box::pin(
            futures::stream::once(async move { yielded_id }).chain(futures::stream::pending()),
        );
        let mut events = stream(
            mail,
            foreign_mail,
            CoreLimits {
                max_objects_in_get: 2,
                max_objects_in_set: 2,
            },
            input,
            Projection::FlagsOnly,
        );

        let event = events.next().await.expect("timed batch is emitted");
        let SyncEvent::Batch(batch) = event else {
            panic!("the timed route should produce a batch, got {event:?}");
        };
        assert_eq!(batch.items.len(), 1);
        assert_eq!(outcome_id(&batch.items[0]), foreign_id.0.as_str());

        let requests = requests.lock().expect("request log");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["methodCalls"][0][1]["accountId"], "shared");
        assert_eq!(requests[0]["methodCalls"][0][1]["ids"], json!(["M1"]));
    }

    fn email(id: &str) -> crate::email::Email {
        serde_json::from_value(serde_json::json!({ "id": id, "keywords": { "$seen": true } }))
            .expect("email fixture decodes")
    }

    fn outcome_id(outcome: &ItemOutcome<HydratedObject>) -> &str {
        match outcome {
            ItemOutcome::Succeeded(success) => &success.item.0,
            ItemOutcome::Failed(failure) => &failure.item.0,
            ItemOutcome::Uncertain(uncertain) => &uncertain.item.0,
        }
    }

    fn reported_ids(outcomes: &[ItemOutcome<HydratedObject>]) -> Vec<&str> {
        let mut ids: Vec<&str> = outcomes.iter().map(outcome_id).collect();
        ids.sort_unstable();
        ids
    }

    /// The closed per-item contract: an id the server answered in NEITHER
    /// `list` nor `notFound` still has to leave on a lane. `notFound`
    /// decodes as empty when a server omits it, so relying on it alone
    /// makes the id evaporate - the engine's read-back then sees a
    /// hydration that reported nothing at all for a target it submitted.
    ///
    /// The class matters as much as the presence: `Protocol(PartialResponse)`
    /// retries (the next `Email/get` normally answers), where a terminal
    /// contract violation would drop the id for good.
    #[test]
    fn an_id_answered_in_neither_list_nor_not_found_becomes_a_retryable_partial_response() {
        let requested = vec![
            ObjectId("m0".to_string()),
            ObjectId("m1".to_string()),
            ObjectId("m2".to_string()),
        ];
        // `m1` came back; `m2` was declared missing; `m0` was simply
        // dropped, with an EMPTY `notFound` - exactly what an omitted
        // `notFound` decodes to.
        let outcomes = reconcile_hydration(
            &requested,
            vec![email("m1")],
            &[EmailId::new("m2")],
            Projection::FlagsOnly,
            "s1",
            None,
        );

        assert_eq!(reported_ids(&outcomes), vec!["m0", "m1", "m2"]);

        let m0 = outcomes
            .iter()
            .find(|outcome| outcome_id(outcome) == "m0")
            .expect("m0 is accounted for");
        let ItemOutcome::Failed(failure) = m0 else {
            panic!("an unanswered id is a failure, got {m0:?}");
        };
        assert!(matches!(
            failure.error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::PartialResponse
            )
        ));
        assert!(failure.error.recovery().is_retryable());
        assert!(!failure.error.recovery().is_terminal());
        // The method response itself arrived; only this id's answer did
        // not. Defaulting to `Unsent` would tell the consumer nothing
        // crossed the wire.
        assert_eq!(
            failure.error.telemetry_fields().transmission_state,
            Some(bifrost_types::TransmissionState::Acknowledged)
        );

        let m2 = outcomes
            .iter()
            .find(|outcome| outcome_id(outcome) == "m2")
            .expect("m2 is accounted for");
        let ItemOutcome::Failed(failure) = m2 else {
            panic!("a notFound id is a failure, got {m2:?}");
        };
        // A declared `notFound` is a different fact from an unanswered
        // id: the object is gone, so retrying it forever is wrong.
        assert!(matches!(
            failure.error.kind(),
            bifrost_types::AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message)
        ));

        assert!(matches!(
            outcomes
                .iter()
                .find(|outcome| outcome_id(outcome) == "m1")
                .expect("m1 is accounted for"),
            ItemOutcome::Succeeded(_)
        ));
    }

    #[test]
    fn repeated_submitted_ids_each_receive_an_outcome() {
        let requested = vec![ObjectId("m1".to_string()), ObjectId("m1".to_string())];
        let outcomes = reconcile_hydration(
            &requested,
            vec![email("m1")],
            &[],
            Projection::FlagsOnly,
            "s1",
            None,
        );

        assert_eq!(outcomes.len(), 2);
        assert!(
            outcomes
                .iter()
                .all(|outcome| matches!(outcome, ItemOutcome::Succeeded(_)))
        );
        assert_eq!(reported_ids(&outcomes), vec!["m1", "m1"]);
    }

    /// Outcomes are keyed by the id the CALLER submitted. A foreign id is
    /// qualified with its owning account and goes on the wire stripped to
    /// its native form, so transcribing the echoed id would answer under
    /// an id the caller never handed in.
    #[test]
    fn a_foreign_id_is_answered_under_the_qualified_id_the_caller_submitted() {
        let qualified = ObjectId(super::super::foreign::encode_object("acct-9", "M1"));
        let requested = vec![qualified.clone()];
        let outcomes = reconcile_hydration(
            &requested,
            vec![email("M1")],
            &[],
            Projection::FlagsOnly,
            "s1",
            Some("acct-9"),
        );

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcome_id(&outcomes[0]), qualified.0);
        let ItemOutcome::Succeeded(success) = &outcomes[0] else {
            panic!("the foreign id hydrated");
        };
        assert_eq!(success.output.id, qualified);
    }

    /// Metadata hydration is the attribution channel for foreign changes:
    /// the account-level change stream emits only qualified ids, and the
    /// consumer learns WHICH shared folder a message sits in from the
    /// hydrated memberships. So the metadata entry must come back in the
    /// owner's namespace, exactly as the foreign inventory walk mints it:
    /// bare native `mailboxIds` collide with same-id primary mailboxes,
    /// and bare entry/blob ids would route later reads through the
    /// primary account.
    #[test]
    fn a_foreign_metadata_hydration_is_qualified_like_the_foreign_inventory() {
        let qualified = ObjectId(super::super::foreign::encode_object("acct-9", "M1"));
        let requested = vec![qualified.clone()];
        let answered: crate::email::Email = serde_json::from_value(serde_json::json!({
            "id": "M1",
            "blobId": "B1",
            "threadId": "T1",
            "size": 10,
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email fixture decodes");

        let outcomes = reconcile_hydration(
            &requested,
            vec![answered],
            &[],
            Projection::Metadata,
            "s1",
            Some("acct-9"),
        );

        assert_eq!(outcomes.len(), 1);
        let ItemOutcome::Succeeded(success) = &outcomes[0] else {
            panic!("the foreign id hydrated: {:?}", outcomes[0]);
        };
        let HydratedObjectKind::Metadata(entry) = &success.output.kind else {
            panic!("expected a metadata entry");
        };
        assert_eq!(
            entry.id, qualified,
            "the entry id must match the submitted id"
        );
        assert_eq!(
            entry.blob_id.as_ref().map(|blob| blob.0.as_str()),
            Some(super::super::foreign::encode_object("acct-9", "B1").as_str()),
            "a bare blob id would download through the primary account"
        );
        assert_eq!(
            entry.thread_id.as_ref().map(|thread| thread.0.as_str()),
            Some(super::super::foreign::encode_object("acct-9", "T1").as_str()),
            "a bare thread id would expand through the primary Thread/get, and on \
             an id collision a thread-keyed mutation would rewrite an unrelated \
             primary thread's messages"
        );
        assert!(
            entry
                .memberships
                .contains(&bifrost_types::MembershipScope::Folder(
                    super::super::foreign::encode_foreign("acct-9", "inbox")
                )),
            "membership must land in the owner's container namespace: {:?}",
            entry.memberships
        );
        assert!(
            !entry
                .memberships
                .contains(&bifrost_types::MembershipScope::Mailbox(
                    bifrost_types::MailboxId("inbox".to_string())
                )),
            "a bare native membership conflates the share with the primary"
        );
        assert!(
            entry
                .memberships
                .contains(&bifrost_types::MembershipScope::Mailbox(
                    bifrost_types::MailboxId("acct-9".to_string())
                )),
            "the owner tag identifies the share"
        );

        // A primary id's metadata stays bare: qualification keys off the
        // route's owner, and the primary route has none.
        let bare = vec![ObjectId("M1".to_string())];
        let answered: crate::email::Email = serde_json::from_value(serde_json::json!({
            "id": "M1",
            "mailboxIds": {"inbox": true},
            "keywords": {}
        }))
        .expect("email fixture decodes");
        let outcomes =
            reconcile_hydration(&bare, vec![answered], &[], Projection::Metadata, "s1", None);
        let ItemOutcome::Succeeded(success) = &outcomes[0] else {
            panic!("the primary id hydrated");
        };
        let HydratedObjectKind::Metadata(entry) = &success.output.kind else {
            panic!("expected a metadata entry");
        };
        assert!(
            entry
                .memberships
                .contains(&bifrost_types::MembershipScope::Mailbox(
                    bifrost_types::MailboxId("inbox".to_string())
                ))
        );
    }

    /// An id qualified for an account this session has no handle for rides
    /// the PRIMARY route with its literal (still-qualified) wire form, so
    /// the primary `Email/get` answers `notFound` - it can never resolve a
    /// primary message that happens to share the native part. Stripping
    /// instead (what hydration used to do) would, on such a collision,
    /// hydrate the unrelated primary message's data under the foreign id.
    /// The mutation pipeline (`wire_email_id` / `wire_id_for_mail`)
    /// already keeps these ids literal; hydration must agree.
    #[test]
    fn an_unregistered_foreign_id_is_never_correlated_with_a_primary_object() {
        let departed = ObjectId(super::super::foreign::encode_object("acct-gone", "M1"));
        let requested = vec![departed.clone()];
        // A primary message whose id collides with the foreign native id.
        // With the literal wire form the server cannot echo it for this
        // request, but a hostile/confused response must still not match.
        let outcomes = reconcile_hydration(
            &requested,
            vec![email("M1")],
            &[],
            Projection::FlagsOnly,
            "s1",
            // Primary route: the account is not registered.
            None,
        );

        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcome_id(&outcomes[0]), departed.0);
        assert!(
            matches!(&outcomes[0], ItemOutcome::Failed(_)),
            "a same-native-id primary object must not answer the foreign id: {:?}",
            outcomes[0]
        );

        // And the wire form itself stays literal on the primary route.
        assert_eq!(wire_object_id(&departed, None), departed.0);
        // ...while the registered-owner route strips to the native id.
        assert_eq!(wire_object_id(&departed, Some("acct-gone")), "M1");
        assert_eq!(wire_object_id(&departed, Some("acct-other")), departed.0);
    }

    /// A response object that does not correlate with a submitted id is
    /// DISCARDED rather than minted into an outcome: emitting it would
    /// hand the consumer an id it never asked for while the id it did ask
    /// for stayed silent. Same for a repeated answer, which would put two
    /// outcomes on one id's lane.
    #[test]
    fn unrequested_repeated_and_id_less_response_objects_are_discarded() {
        let requested = vec![ObjectId("m0".to_string()), ObjectId("m1".to_string())];
        let id_less: crate::email::Email =
            serde_json::from_value(serde_json::json!({ "keywords": {} })).expect("decodes");
        let outcomes = reconcile_hydration(
            &requested,
            vec![email("m0"), email("m0"), email("stranger"), id_less],
            // A `notFound` naming an id that was never requested is
            // discarded on the same grounds.
            &[EmailId::new("also-a-stranger")],
            Projection::FlagsOnly,
            "s1",
            None,
        );

        assert_eq!(reported_ids(&outcomes), vec!["m0", "m1"]);
        assert!(matches!(
            outcomes
                .iter()
                .find(|outcome| outcome_id(outcome) == "m0")
                .expect("m0 is accounted for"),
            ItemOutcome::Succeeded(_)
        ));
        // `m1` was never answered at all, so it rides the partial lane.
        assert!(matches!(
            outcomes
                .iter()
                .find(|outcome| outcome_id(outcome) == "m1")
                .expect("m1 is accounted for"),
            ItemOutcome::Failed(_)
        ));
    }
}
