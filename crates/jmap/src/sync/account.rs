use futures::{FutureExt as _, StreamExt as _};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFuture, AccountOperation, AccountStream,
    AddressBook, AddressBookId, AttachmentHandle, BlobHandle, ByteRange, Calendar, CalendarEvent,
    ChangeCursor, CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch,
    ContactSearchRequest, ContainerId, ContainerKind, ContainerList, CostClass, CursorDescriptor,
    CursorEstablishment, CursorScope, DirectoryCard, DirectoryGroup, DirectoryGroupId,
    DirectoryGroupMember, DraftHandle, DraftPatch, ErrorScope, EventCreate, EventId, EventPatch,
    EventRange, EventSearchRequest, FilterValidation, HostedAttachment, HydratedObject,
    HydrationProjection, Identity, IdentityId, IdentityPatch, Importance, InventoryEvent,
    InventoryPartition, InventoryPartitioning, ItemOutcome, Label, MembershipScope, Message,
    MutationSuccess, MutationTarget, ObjectId, Page, Priority, Projection, QuotaInfo, RsvpStatus,
    ScopeLifecycleEvent, SearchRequest, SendAs, SendReceipt, SendRequest, ServerFilter,
    ServerFilterCreate, ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent,
    SyncStrategy, ThreadHydration, ThreadId, VacationConfig,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::core::transport::HttpTransport;
use crate::transport_reqwest::ReqwestTransport;

use super::{
    blob, calendar_ops, changes, contacts, discover, filters, hydrate, inventory, mutation, pim,
    push, state,
};

pub(crate) type CloseCompletion =
    futures::future::Shared<futures::future::BoxFuture<'static, Result<(), AccountError>>>;

/// Start account teardown once, in a task that outlives any caller waiting on
/// it. The initializer is invoked only when the returned future is first
/// polled, so a future dropped before polling cannot mark the account closed
/// without starting shutdown.
fn close_once<F>(
    completion: Arc<std::sync::OnceLock<CloseCompletion>>,
    start: impl FnOnce() -> F + Send + 'static,
) -> AccountFuture<Result<(), AccountError>>
where
    F: Future<Output = Result<(), AccountError>> + Send + 'static,
{
    Box::pin(async move {
        let completion = completion
            .get_or_init(|| {
                let task = tokio::spawn(start());
                async move { task.await.expect("JMAP account teardown task panicked") }
                    .boxed()
                    .shared()
            })
            .clone();
        completion.await
    })
}

/// The engine-facing account is pinned to the production transport, not
/// generic over one: WebSocket push is reqwest-specific, so `JmapAccount`
/// cannot be parameterized without either dropping push or growing a second
/// transport trait for it.
///
/// The accepted cost is a testing one, and it is a known, bounded residual
/// rather than an oversight. Because no scripted transport can be installed
/// at this level, a WHOLE `Account` call - `capabilities()`,
/// `describe_cursor`, the mutation doors - is undrivable hermetically; only
/// the false direction (a refusal that never reaches the wire) can be pinned
/// here, which is what `capability_contract_tests.rs` documents and does.
/// Everything pure is instead extracted into free functions and tested
/// directly, and the sync request helpers below this struct stay generic over
/// `HttpTransport` so a scripted transport reaches them. Closing the residual
/// means threading the transport generic through `JmapAccount` (and giving
/// push its own seam), not adding a test-support type elsewhere.
type MailAccount<T = ReqwestTransport> = super::registry::Slot<T>;
type SubmissionAccount<T = ReqwestTransport> =
    super::registry::Slot<T, crate::core::capability::Submission>;

pub(crate) struct JmapAccount {
    pub(crate) client: Client,
    pub(crate) registry: Arc<super::registry::Registry>,
    pub(crate) self_emails: Vec<String>,
    pub(crate) caps: AccountCapabilities,
    pub(crate) ws: push::WsState,
    pub(crate) subscriptions: Arc<Mutex<HashMap<SubscriptionHandle, push::DataTypeSet>>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) closed: Arc<AtomicBool>,
    /// Shared teardown result. The first polled `close()` starts one
    /// background teardown; later callers join that same completion.
    pub(crate) close_completion: Arc<std::sync::OnceLock<CloseCompletion>>,
    pub(crate) subscription_seq: AtomicU64,
    // Mailbox observations are not mutation guards. Only the lifecycle walk
    // owns sidebar progress; a local write must never fast-forward it.
    pub(crate) lifecycle_topology: Arc<Mutex<discover::LifecycleTopology>>,
}

impl JmapAccount {
    pub(crate) fn new(
        client: Client,
        registry: Arc<super::registry::Registry>,
        self_emails: Vec<String>,
        caps: AccountCapabilities,
        ws: push::WsState,
        shutdown: CancellationToken,
        lifecycle_topology: discover::LifecycleTopology,
    ) -> Self {
        Self {
            client,
            registry,
            self_emails,
            caps,
            ws,
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            shutdown,
            closed: Arc::new(AtomicBool::new(false)),
            close_completion: Arc::new(std::sync::OnceLock::new()),
            subscription_seq: AtomicU64::new(1),
            lifecycle_topology: Arc::new(Mutex::new(lifecycle_topology)),
        }
    }

    pub(crate) fn slot_for_scope(&self, scope: &CursorScope) -> &super::registry::Slot {
        self.registry
            .scope(scope)
            .reachable()
            .expect("departed scopes are refused before slot selection")
    }

    /// Resolve a thread-keyed operation to its owning account.
    ///
    /// A foreign thread id is owner-qualified in the object namespace (the
    /// foreign inventory and hydration mint it that way), and `Thread/get`
    /// is accountId-scoped with the thread id as its only operand. Routed
    /// to the primary account, a foreign thread id either finds nothing
    /// or, on an id collision, resolves an UNRELATED primary thread whose
    /// messages the caller's mutation then rewrites. As with a message id,
    /// an id naming an account this session no longer holds stays on the
    /// primary route in its literal form so the server, not a guessed
    /// local route, reports the miss.
    fn mail_for_thread(&self, thread: &ThreadId) -> MailAccount {
        self.registry.object(&thread.0).clone()
    }

    fn mail_for_mutation_target(&self, target: &MutationTarget) -> MailAccount {
        match target {
            MutationTarget::Message(id) => self.registry.object(&id.0).clone(),
            MutationTarget::Thread(id) => self.registry.object(&id.0).clone(),
            _ => self.registry.primary.clone(),
        }
    }

    /// Draft handles contain only an Email id, so the whole draft lifecycle
    /// must use one account namespace. Submission is selected when present,
    /// matching draft_send; Mail remains the usable fallback on accounts
    /// without Submission, where sending a draft is unavailable.
    fn draft_account(&self, op: AccountOperation) -> Result<MailAccount, AccountError> {
        draft_mail_account(
            &self.registry.primary,
            self.registry.family(&self.registry.families.submission),
            op,
        )
    }

    /// The owning shared-account identity for a foreign `Folder` scope,
    /// or `None` for a primary scope. Drives the revocation-isolation
    /// decision (quarantine the foreign scope vs escalate account-wide).
    pub(crate) fn owner_of_scope(&self, scope: &CursorScope) -> Option<bifrost_types::PrincipalId> {
        match self.registry.scope(scope) {
            super::registry::Route::Shared(slot) => {
                Some(bifrost_types::PrincipalId(slot.mail.id_str().to_owned()))
            }
            super::registry::Route::Primary(_) | super::registry::Route::Departed => None,
        }
    }

    pub(crate) fn is_unregistered_foreign_scope(&self, scope: &CursorScope) -> bool {
        matches!(self.registry.scope(scope), super::registry::Route::Departed)
    }

    pub(crate) fn cursor_scopes(&self) -> Vec<CursorScope> {
        self.registry.cursor_scopes()
    }

    pub(crate) fn foreign_owner_memberships(&self) -> Vec<MembershipScope> {
        self.registry.memberships()
    }

    pub(crate) fn next_subscription_handle(&self) -> SubscriptionHandle {
        let next = self.subscription_seq.fetch_add(1, Ordering::AcqRel);
        SubscriptionHandle(format!("jmap-ws-{next}"))
    }
}

/// Every scope is one full walk. Email pages within that walk by anchor;
/// splitting it into positional partitions would discard that anchor at
/// every engine boundary and make deletion shifts lossy again.
fn partitioning_for_scope(
    _scope: &CursorScope,
    _max_objects_in_get: usize,
) -> InventoryPartitioning {
    InventoryPartitioning::Full
}

/// Probe the selected account immediately before returning a cursor.
/// The open-time seed is only the membership gate: reusing its state for a
/// later `RestartScope` can repeat `cannotCalculateChanges` forever after the
/// server expires that old position.
async fn fresh_cursor_establishment<T: HttpTransport>(
    mail: MailAccount<T>,
    scope: CursorScope,
) -> Result<CursorEstablishment, AccountError> {
    let email_states = mail.email_states.clone();
    let data_type = state::JmapScopeRepr::from_cursor_scope(&scope)
        .ok()
        .and_then(|scope| scope.data_type());
    match &data_type {
        Some(crate::DataType::Email | crate::DataType::Mailbox) => {}
        _ => return Err(unseeded_scope_error(scope)),
    };
    // Capture the cache value before the network read. If a concurrent
    // mutation advances it while the probe is in flight, the CAS below keeps
    // that newer state instead of replacing it with this probe's older one.
    let cached_before_probe = email_states.guard().await;
    let state_string = match data_type {
        Some(crate::DataType::Email) => mail
            .call(crate::email::EmailGet::new().ids(Vec::<crate::email::EmailId>::new()))
            .await
            .map(|response| response.into_state().into_string()),
        Some(crate::DataType::Mailbox) => mail
            .call(crate::mailbox::MailboxGet::new().ids(Vec::<crate::mailbox::MailboxId>::new()))
            .await
            .map(|response| response.into_state().into_string()),
        _ => unreachable!("scope kind was checked above"),
    }
    .map_err(|error| {
        super::error::into_account_error(
            error,
            super::error::JmapErrorContext::cursor(
                AccountOperation::EstablishCursor,
                scope.clone(),
            ),
        )
    })?;
    if matches!(data_type, Some(crate::DataType::Email)) {
        email_states
            .observe_probe(cached_before_probe.as_ref(), state_string.clone().into())
            .await;
    }
    Ok(cursor_from_state(scope, state_string))
}

fn cursor_from_state(scope: CursorScope, state_string: String) -> CursorEstablishment {
    let server_state = state::encode_for_scope(&scope, state_string)
        .expect("seeded JMAP cursor scopes have encodable payloads");
    CursorEstablishment::Ready(ChangeCursor {
        scope,
        server_state,
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(
            state::OUTER_CURSOR_ENVELOPE_VERSION,
        ),
    })
}

/// An unseeded scope is unsupported rather than a cursor over an empty
/// state, which would present as a working cursor that returns nothing.
fn unseeded_scope_error(scope: CursorScope) -> AccountError {
    super::error::unsupported_error(
        AccountOperation::EstablishCursor,
        Some(ErrorScope::Cursor(scope)),
        bifrost_types::UnsupportedReason::CursorScope,
        "JMAP did not seed a cursor for this scope",
    )
}

/// A draft must live where it will be submitted. Refuse a Submission account
/// without Mail instead of silently creating the draft in another namespace.
fn draft_mail_account<T: HttpTransport>(
    mail: &MailAccount<T>,
    submission: Option<&SubmissionAccount<T>>,
    op: AccountOperation,
) -> Result<MailAccount<T>, AccountError> {
    match submission {
        Some(slot) => slot
            .view::<crate::core::capability::Mail>()
            .map_err(|error| {
                super::error::into_account_error(error, super::error::JmapErrorContext::new(op))
            }),
        None => Ok(mail.clone()),
    }
}

/// Uploaded blob ids are account scoped, just like the drafts consuming them.
fn upload_draft_attachment<T: crate::core::transport::HttpTransport>(
    mail: &MailAccount<T>,
    submission: Option<&SubmissionAccount<T>>,
    bytes: AccountStream<Result<bytes::Bytes, AccountError>>,
    mime: String,
) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
    match submission {
        Some(submission) => pim::attachment_upload(submission.clone(), bytes, mime),
        None => pim::attachment_upload(mail.clone(), bytes, mime),
    }
}

/// Pure descriptor logic behind `Account::describe_cursor`, split out
/// for direct testing.
///
/// Decoding alone is not support: legacy `Thread` and `Query` cursors
/// still decode (their scope tags survive the envelope bump so old
/// durable rows stay readable) but `changes::stream` terminates both
/// `Unsupported(SyncChanges)` - thread inventory derives from Email
/// inventory, and the v1 contract registers no query definitions to
/// turn a query id into a filter. Reporting `Cheap`/`ServerCursor` for
/// one would promise a strategy that dies on its first poll, so only
/// the scopes the change stream actually drives report one.
fn describe_cursor_support(cursor: &ChangeCursor) -> CursorDescriptor {
    let supported = state::decode_cursor(cursor)
        .ok()
        .and_then(|(scope, _)| scope.data_type())
        .is_some();
    CursorDescriptor {
        cost_class: if supported {
            CostClass::Cheap
        } else {
            CostClass::Expensive
        },
        strategy: if supported {
            SyncStrategy::ServerCursor
        } else {
            SyncStrategy::None
        },
        freshness: None,
    }
}

impl Account for JmapAccount {
    fn protocol(&self) -> bifrost_types::Protocol {
        bifrost_types::Protocol::Jmap
    }

    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, priority: Priority) {
        self.client.transport().set_priority(priority);
    }

    fn set_bandwidth_cap(&self, bps: Option<bifrost_types::BandwidthCap>) {
        self.client.transport().set_bandwidth_cap(bps);
    }

    fn describe_cursor(&self, cursor: &ChangeCursor) -> CursorDescriptor {
        let mut descriptor = describe_cursor_support(cursor);
        if self.is_unregistered_foreign_scope(&cursor.scope) {
            descriptor.cost_class = CostClass::Expensive;
            descriptor.strategy = SyncStrategy::None;
        }
        descriptor
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        discover::cursor_scopes(self.cursor_scopes())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        discover::memberships(
            self.registry.primary.mail.clone(),
            self.foreign_owner_memberships(),
        )
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        discover::scope_lifecycle(
            self.registry.primary.mail.clone(),
            self.registry.primary.limits,
            Arc::clone(&self.lifecycle_topology),
            self.shutdown.clone(),
            self.client.clone(),
        )
    }

    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        if !self.registry.seeded(&scope) {
            return Box::pin(async move { Err(unseeded_scope_error(scope)) });
        }
        let mail = self.slot_for_scope(&scope).clone();
        Box::pin(fresh_cursor_establishment(mail, scope))
    }

    fn inventory_stream(&self, scope: CursorScope) -> AccountStream<InventoryEvent> {
        if self.is_unregistered_foreign_scope(&scope) {
            let domain = bifrost_types::CoverageDomain::full(scope.clone());
            let err =
                super::error::unregistered_foreign_scope(scope, AccountOperation::SyncInventory);
            return Box::pin(
                async_stream::stream! { yield super::error::terminated(err); }
                    .map(bifrost_types::lift_complete_walk(domain)),
            );
        }
        let mail = self.slot_for_scope(&scope).clone();
        let owner = self.owner_of_scope(&scope);
        let domain = bifrost_types::CoverageDomain::full(scope.clone());
        Box::pin(
            inventory::stream(mail.mail, mail.limits, scope, owner)
                .map(bifrost_types::lift_complete_walk(domain)),
        )
    }

    fn inventory_partitioning(&self, scope: &CursorScope) -> InventoryPartitioning {
        partitioning_for_scope(scope, self.registry.primary.limits.max_objects_in_get.get())
    }

    fn inventory_partition_stream(
        &self,
        scope: CursorScope,
        partition: InventoryPartition,
    ) -> AccountStream<InventoryEvent> {
        if self.is_unregistered_foreign_scope(&scope) {
            let domain = bifrost_types::CoverageDomain::full(scope.clone());
            let err =
                super::error::unregistered_foreign_scope(scope, AccountOperation::SyncInventory);
            return Box::pin(
                async_stream::stream! { yield super::error::terminated(err); }
                    .map(bifrost_types::lift_complete_walk(domain)),
            );
        }
        let mail = self.slot_for_scope(&scope).clone();
        let owner = self.owner_of_scope(&scope);
        // The domain is the PARTITION's extent, not the scope's. A partition
        // that finished cleanly proves nothing about its neighbours, and a
        // full-scope claim here would discharge their debt for free.
        //
        // `uid_validity` is unused: JMAP advertises `Time` / `PageCount`
        // partitioning, never `Uid`, so the UID arm is unreachable from this
        // account.
        let domain = bifrost_types::CoverageDomain::for_partition(scope.clone(), &partition);
        Box::pin(
            inventory::stream_partition(mail.mail, mail.limits, scope, partition, owner)
                .map(bifrost_types::lift_complete_walk(domain)),
        )
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        // A foreign-qualified id must hydrate against its OWNING account
        // (the same selection `Registry::scope` makes on the changes path);
        // `Email/get` is accountId-scoped, so routing it through the primary
        // account cannot reach a shared mailbox's mail.
        hydrate::stream(Arc::clone(&self.registry), ids, projection)
    }

    fn changes_stream(
        &self,
        cursor: ChangeCursor,
    ) -> AccountStream<SyncEvent<bifrost_types::Change>> {
        if self.is_unregistered_foreign_scope(&cursor.scope) {
            let err = super::error::unregistered_foreign_scope(
                cursor.scope,
                AccountOperation::SyncChanges,
            );
            return Box::pin(async_stream::stream! { yield super::error::terminated(err); });
        }
        let mail = self.slot_for_scope(&cursor.scope).clone();
        changes::stream(mail, cursor)
    }

    fn push_subscribe(
        &self,
        scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        let scopes = scopes.to_vec();
        // A departed share cannot produce hints in this session. Outlook users
        // expect a refusal for that scope, while other requested scopes still
        // subscribe successfully; never claim a dead share is being watched.
        let registered: Vec<bool> = scopes
            .iter()
            .map(|scope| !self.is_unregistered_foreign_scope(scope))
            .collect();
        let routed_scopes = scopes
            .iter()
            .zip(&registered)
            .filter(|(_, registered)| **registered)
            .map(|(scope, _)| scope.clone())
            .collect::<Vec<_>>();
        // Ruling: when EVERY requested scope is a departed share, report each
        // scope as refused rather than failing the whole call. An Outlook user
        // whose share was removed expects that share shown as unwatched, the
        // same per-scope answer a mixed request gives it, not a generic push
        // failure. Nothing is registered on the wire, so no handle is minted.
        if !scopes.is_empty() && routed_scopes.is_empty() {
            let expected = (0..scopes.len())
                .map(|index| bifrost_types::BatchItemId(index.to_string()))
                .collect::<Vec<_>>();
            let mut outcomes = bifrost_types::BatchOutcomeBuilder::new();
            for (id, scope) in expected.iter().cloned().zip(scopes) {
                outcomes.push_failed(
                    id,
                    super::error::unsupported_error(
                        AccountOperation::PushSubscribe,
                        Some(bifrost_types::ErrorScope::Cursor(scope)),
                        bifrost_types::UnsupportedReason::CursorScope,
                        "JMAP push requires a supported scope in a registered account",
                    ),
                );
            }
            let result = bifrost_types::PushSubscription::new(
                None,
                outcomes
                    .finalize(&expected)
                    .expect("every requested push scope is accounted for once"),
            )
            .map_err(|failure| {
                super::error::internal_invariant(
                    AccountOperation::PushSubscribe,
                    format!("JMAP push subscription invariant failed: {failure}"),
                )
            });
            return Box::pin(async move { result });
        }
        let future = push::subscribe(
            self.client.clone(),
            self.caps.push,
            self.next_subscription_handle(),
            routed_scopes,
            push::SubscribeState {
                subscriptions: Arc::clone(&self.subscriptions),
                enabled: Arc::clone(&self.ws.enabled),
                push_state: Arc::clone(&self.ws.push_state),
                closed: Arc::clone(&self.closed),
            },
        );
        Box::pin(async move {
            let (handle, accepted) = future.await?;
            let mut accepted = accepted.into_iter();
            let accepted: Vec<bool> = registered
                .into_iter()
                .map(|registered| registered && accepted.next().unwrap_or(false))
                .collect();
            let expected = (0..scopes.len())
                .map(|index| bifrost_types::BatchItemId(index.to_string()))
                .collect::<Vec<_>>();
            let mut outcomes = bifrost_types::BatchOutcomeBuilder::new();
            for ((id, scope), accepted) in expected.iter().cloned().zip(scopes).zip(accepted) {
                if accepted {
                    outcomes.push_succeeded(id, scope);
                } else {
                    outcomes.push_failed(
                        id,
                        super::error::unsupported_error(
                            AccountOperation::PushSubscribe,
                            Some(bifrost_types::ErrorScope::Cursor(scope)),
                            bifrost_types::UnsupportedReason::CursorScope,
                            "JMAP push requires a supported scope in a registered account",
                        ),
                    );
                }
            }
            bifrost_types::PushSubscription::new(
                Some(handle),
                outcomes
                    .finalize(&expected)
                    .expect("every requested push scope is accounted for once"),
            )
            .map_err(|failure| {
                super::error::internal_invariant(
                    AccountOperation::PushSubscribe,
                    format!("JMAP push subscription invariant failed: {failure}"),
                )
            })
        })
    }

    fn push_unsubscribe(
        &self,
        handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        push::unsubscribe(
            self.client.clone(),
            handle,
            Arc::clone(&self.subscriptions),
            Arc::clone(&self.ws.enabled),
            Arc::clone(&self.ws.push_state),
        )
    }

    fn push_lanes(&self) -> Vec<bifrost_types::PushLaneSpec> {
        if !self.ws.available {
            return Vec::new();
        }
        vec![bifrost_types::PushLaneSpec {
            lane: bifrost_types::PushLane(0),
            kind: bifrost_types::PushLaneKind::Connection,
            backoff: bifrost_types::BackoffPolicy::new(
                self.ws.policy.initial,
                self.ws.policy.max,
                false,
            ),
        }]
    }
    fn push_stream(
        &self,
        lane: bifrost_types::PushLane,
        stop: bifrost_types::SessionStop,
        _gate: bifrost_types::PushGate,
    ) -> AccountStream<bifrost_types::PushSessionEvent> {
        if lane != bifrost_types::PushLane(0) || !self.ws.available {
            return Box::pin(futures::stream::iter([
                bifrost_types::PushSessionEvent::Failed(bifrost_types::push_lane_contract_error(
                    bifrost_types::Protocol::Jmap,
                    lane,
                    "not a lane of this account",
                )),
            ]));
        }
        push::session(
            self.client.clone(),
            Arc::clone(&self.ws.enabled),
            Arc::clone(&self.ws.push_state),
            self.shutdown.clone(),
            self.ws.policy,
            Arc::clone(&self.ws.routing),
            self.ws.lease.clone(),
            stop,
        )
    }

    fn open_blob(&self, handle: BlobHandle) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open(Arc::clone(&self.registry), handle)
    }

    fn open_blob_range(
        &self,
        handle: BlobHandle,
        range: ByteRange,
    ) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_range(handle, range)
    }

    fn open_raw_rfc822(&self, message: ObjectId) -> AccountStream<SyncEvent<bytes::Bytes>> {
        blob::open_raw_rfc822(Arc::clone(&self.registry), message)
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        op: bifrost_types::FlagOp,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::set_flags(Arc::clone(&self.registry), targets, op)
    }

    fn bulk_set_flags_if_version(
        self: Arc<Self>,
        targets: AccountStream<bifrost_types::MailMutationTarget>,
        op: bifrost_types::FlagOp,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::versioned_stream(
            Arc::clone(&self.registry),
            targets,
            mutation::MutationKind::Flags(op),
        )
    }

    fn bulk_move_if_version(
        self: Arc<Self>,
        targets: AccountStream<bifrost_types::MailMutationTarget>,
        destination: MembershipScope,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        let MembershipScope::Mailbox(mailbox) = destination else {
            return bifrost_types::mutation::versioned_mail_stream(targets, |target| {
                bifrost_types::mutation::refused_mail_target(
                    target.id,
                    super::error::unsupported(bifrost_types::AccountOperation::BulkMove),
                )
            });
        };
        mutation::versioned_stream(
            Arc::clone(&self.registry),
            targets,
            mutation::MutationKind::Move(crate::mailbox::MailboxId::new(mailbox.0)),
        )
    }

    fn bulk_destroy_if_version(
        self: Arc<Self>,
        targets: AccountStream<bifrost_types::MailMutationTarget>,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::versioned_stream(
            Arc::clone(&self.registry),
            targets,
            mutation::MutationKind::Destroy,
        )
    }

    fn bulk_move_from_if_version(
        self: Arc<Self>,
        targets: AccountStream<bifrost_types::MailMutationTarget>,
        destination: MembershipScope,
        _source: Option<MembershipScope>,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.bulk_move_if_version(targets, destination)
    }

    fn bulk_move(
        &self,
        targets: AccountStream<ObjectId>,
        destination: MembershipScope,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::move_to(Arc::clone(&self.registry), targets, destination)
    }

    fn bulk_destroy(
        &self,
        targets: AccountStream<ObjectId>,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        mutation::destroy(Arc::clone(&self.registry), targets)
    }

    fn add_to_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::add_to_container(mail.clone(), target, container)
    }

    fn remove_from_container(
        &self,
        target: MutationTarget,
        container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::remove_from_container(mail.clone(), target, container)
    }

    fn set_keyword(
        &self,
        target: MutationTarget,
        keyword: bifrost_types::Flag,
        value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_keyword(mail.clone(), target, keyword, value)
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetLabelMembership,
            None,
            bifrost_types::UnsupportedReason::OperationUnavailable,
            "JMAP does not support label membership",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetCategory,
            None,
            bifrost_types::UnsupportedReason::OperationUnavailable,
            "JMAP does not support categories",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::SetExtendedProperty,
            None,
            bifrost_types::UnsupportedReason::OperationUnavailable,
            "JMAP does not support extended properties",
        );
        Box::pin(async move { Err(err) })
    }

    fn set_is_read(
        &self,
        target: MutationTarget,
        is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_is_read(mail.clone(), target, is_read)
    }

    fn set_importance(
        &self,
        target: MutationTarget,
        level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_mutation_target(&target);
        pim::set_importance(mail.clone(), target, level)
    }

    fn send_message(
        &self,
        request: SendRequest,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        if let Some(send_as) = request.send_as.as_ref() {
            let id = send_as.mailbox().0.clone();
            if let Err(err) = route_send_as(
                send_as,
                !!self.registry.has_shared_submission(),
                request.scheduled.is_some(),
                self.registry.shared_slot(id.as_str()).is_some(),
                self.registry
                    .shared_slot(&id)
                    .is_some_and(|slot| slot.submission),
            ) {
                return Box::pin(async move { Err(err) });
            }
            let mail = self
                .registry
                .shared_slot(id.as_str())
                .cloned()
                .expect("route_send_as validated shared-slot membership");
            let mail = match mail.view::<crate::core::capability::Submission>() {
                Ok(account) => account,
                Err(error) => {
                    let error = super::error::into_account_error(
                        error,
                        super::error::JmapErrorContext::new(AccountOperation::Send),
                    );
                    return Box::pin(async move { Err(error) });
                }
            };
            let self_address = self
                .self_emails
                .first()
                .cloned()
                .map(bifrost_types::Address::bare);
            return pim::send_message(
                mail,
                0,
                Some(pim::ForeignSubmission {
                    mode: send_as.clone(),
                    self_address,
                }),
                request,
            );
        }
        let Some(submission) = self
            .registry
            .family(&self.registry.families.submission)
            .cloned()
            .clone()
        else {
            let err = super::error::unsupported_error(
                AccountOperation::Send,
                None,
                bifrost_types::UnsupportedReason::SubmissionUnavailable,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // Route through the resolved `Submission`-capability account
        // handle, not `self.registry.primary.mail`. They are usually the same accountId,
        // but `primary_account::<Submission>()` can resolve a distinct
        // account; the draft `Email/set` and the `EmailSubmission/set`
        // must both target the submission account.
        pim::send_message(
            submission,
            self.registry.families.max_delayed_send,
            None,
            request,
        )
    }

    fn send_raw_message(
        &self,
        raw: bytes::Bytes,
        save_to_sent: Option<bool>,
    ) -> AccountFuture<Result<SendReceipt, AccountError>> {
        let Some(submission) = self
            .registry
            .family(&self.registry.families.submission)
            .cloned()
            .clone()
        else {
            let err = super::error::unsupported_error(
                AccountOperation::Send,
                None,
                bifrost_types::UnsupportedReason::SubmissionUnavailable,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // Same submission-account routing as `send_message`: the import +
        // EmailSubmission must both target the submission-capable account.
        pim::send_raw_message(submission, raw, save_to_sent)
    }

    fn attachment_upload(
        &self,
        bytes: AccountStream<Result<bytes::Bytes, AccountError>>,
        mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        upload_draft_attachment(
            &self.registry.primary,
            self.registry.family(&self.registry.families.submission),
            bytes,
            mime,
        )
    }

    fn host_attachment(
        &self,
        _bytes: bytes::Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::HostAttachment,
            None,
            bifrost_types::UnsupportedReason::OperationUnavailable,
            "JMAP has no cloud-drive attachment hosting",
        );
        Box::pin(async move { Err(err) })
    }

    fn draft_create(&self, patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        let draft_account = match self.draft_account(AccountOperation::DraftCreate) {
            Ok(account) => account,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        pim::draft_create(draft_account, patch)
    }

    fn draft_update(
        &self,
        draft: DraftHandle,
        patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        let draft_account = match self.draft_account(AccountOperation::DraftUpdate) {
            Ok(account) => account,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        pim::draft_update(draft_account, draft, patch)
    }

    fn draft_discard(&self, draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        let draft_account = match self.draft_account(AccountOperation::DraftDiscard) {
            Ok(account) => account,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        pim::draft_discard(draft_account, draft)
    }

    fn draft_send(&self, draft: DraftHandle) -> AccountFuture<Result<SendReceipt, AccountError>> {
        let Some(submission) = self
            .registry
            .family(&self.registry.families.submission)
            .cloned()
            .clone()
        else {
            let err = super::error::unsupported_error(
                AccountOperation::DraftSend,
                None,
                bifrost_types::UnsupportedReason::SubmissionUnavailable,
                "JMAP submission capability not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The `EmailSubmission/set` and the Sent-folder move both target
        // the resolved submission account, not necessarily `self.registry.primary.mail`.
        pim::draft_send(submission, draft)
    }

    fn cancel_scheduled_send(&self, handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        let cancellable = self
            .registry
            .family(&self.registry.families.submission)
            .cloned()
            .clone()
            .filter(|_| self.registry.families.max_delayed_send != 0);
        let Some(submission) = cancellable else {
            let err = super::error::unsupported_error(
                AccountOperation::CancelScheduledSend,
                None,
                bifrost_types::UnsupportedReason::ScheduledSendUnavailable,
                "JMAP scheduled send not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The submission id was minted on the `Submission` account; the
        // `EmailSubmission/set` cancel must go to that account.
        pim::cancel_scheduled_send(submission, handle)
    }

    fn reschedule_send(
        &self,
        handle: ObjectId,
        scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        let reschedulable = self
            .registry
            .family(&self.registry.families.submission)
            .cloned()
            .clone()
            .filter(|_| self.registry.families.max_delayed_send != 0);
        let Some(submission) = reschedulable else {
            let err = super::error::unsupported_error(
                AccountOperation::RescheduleSend,
                None,
                bifrost_types::UnsupportedReason::ScheduledSendUnavailable,
                "JMAP scheduled send not available",
            );
            return Box::pin(async move { Err(err) });
        };
        // The submission id was minted on the `Submission` account; the
        // cancel + recreate must both target that account.
        pim::reschedule_send(
            submission,
            self.registry.families.max_delayed_send,
            handle,
            scheduled,
        )
    }

    fn search(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        pim::search(Arc::clone(&self.registry), request)
    }

    fn search_messages(
        &self,
        request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        pim::search_messages(Arc::clone(&self.registry), request)
    }

    fn containers_list(&self) -> AccountFuture<Result<ContainerList, AccountError>> {
        pim::containers_list(Arc::clone(&self.registry))
    }

    fn container_create(
        &self,
        kind: ContainerKind,
        name: String,
        parent: Option<ContainerId>,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        let mail = parent
            .as_ref()
            .map_or(&self.registry.primary, |id| self.registry.object(&id.0));
        pim::container_create(mail.clone(), kind, name, parent, style)
    }

    fn container_rename(
        &self,
        container: ContainerId,
        name: String,
        style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.registry.object(&container.0);
        pim::container_rename(mail.clone(), container, name, style)
    }

    fn container_move(
        &self,
        container: ContainerId,
        new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.registry.object(&container.0);
        pim::container_move(mail.clone(), container, new_parent)
    }

    fn container_delete(&self, container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.registry.object(&container.0);
        pim::container_delete(mail.clone(), container)
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        pim::identities_list(
            self.registry
                .family(&self.registry.families.submission)
                .cloned()
                .clone(),
        )
    }

    fn identity_update(
        &self,
        identity: IdentityId,
        patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        pim::identity_update(
            self.registry
                .family(&self.registry.families.submission)
                .cloned()
                .clone(),
            identity,
            patch,
        )
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        pim::vacation_get(
            self.registry
                .family(&self.registry.families.vacation)
                .cloned()
                .clone(),
        )
    }

    fn vacation_set(&self, config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        pim::vacation_set(
            self.registry
                .family(&self.registry.families.vacation)
                .cloned()
                .clone(),
            config,
        )
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        pim::quota_get(
            self.registry
                .family(&self.registry.families.quota)
                .cloned()
                .clone(),
        )
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        filters::list(
            self.registry
                .family(&self.registry.families.sieve)
                .map(|slot| slot.mail.clone()),
        )
    }

    fn filter_create(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        filters::create(
            self.registry
                .family(&self.registry.families.sieve)
                .map(|slot| slot.mail.clone()),
            filter,
        )
    }

    fn filter_update(
        &self,
        filter: ServerFilterId,
        patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        filters::update(
            self.registry
                .family(&self.registry.families.sieve)
                .map(|slot| slot.mail.clone()),
            filter,
            patch,
        )
    }

    fn filter_delete(&self, filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        filters::delete(
            self.registry
                .family(&self.registry.families.sieve)
                .map(|slot| slot.mail.clone()),
            filter,
        )
    }

    fn filter_validate(
        &self,
        filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        filters::validate(
            self.registry
                .family(&self.registry.families.sieve)
                .map(|slot| slot.mail.clone()),
            filter,
        )
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        contacts::address_books_list(
            self.registry
                .family(&self.registry.families.contacts)
                .map(|slot| slot.mail.clone()),
        )
    }

    fn contacts_list(
        &self,
        address_book: Option<AddressBookId>,
        page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::list(
            self.registry
                .family(&self.registry.families.contacts)
                .map(|slot| slot.mail.clone()),
            address_book,
            page_cursor,
        )
    }

    fn contact_get(&self, contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        contacts::get(
            self.registry
                .family(&self.registry.families.contacts)
                .map(|slot| slot.mail.clone()),
            contact,
        )
    }

    fn contact_create(
        &self,
        contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        contacts::create(
            self.registry
                .family(&self.registry.families.contacts)
                .map(|slot| slot.mail.clone()),
            contact,
        )
    }

    fn contact_update(
        &self,
        contact: ContactId,
        patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        contacts::update(
            self.registry
                .family(&self.registry.families.contacts)
                .map(|slot| slot.mail.clone()),
            contact,
            patch,
        )
    }

    fn contact_delete(&self, contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        contacts::delete(
            self.registry
                .family(&self.registry.families.contacts)
                .map(|slot| slot.mail.clone()),
            contact,
        )
    }

    fn contact_delete_if_version(
        &self,
        contact: ContactId,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        contacts::delete_if_version(
            self.registry
                .family(&self.registry.families.contacts)
                .map(|slot| slot.mail.clone()),
            contact,
            expected_version,
        )
    }

    fn contact_search(
        &self,
        request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        contacts::search(
            self.registry
                .family(&self.registry.families.contacts)
                .map(|slot| slot.mail.clone()),
            request,
        )
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryCard>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectorySearch,
            None,
            bifrost_types::UnsupportedReason::OperationUnavailable,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryGroup>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectoryGroupsList,
            None,
            bifrost_types::UnsupportedReason::OperationUnavailable,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn directory_group_expand(
        &self,
        _group: DirectoryGroupId,
        _page_cursor: Option<bifrost_types::PageCursor>,
    ) -> AccountFuture<Result<Page<DirectoryGroupMember>, AccountError>> {
        let err = super::error::unsupported_error(
            AccountOperation::DirectoryGroupExpand,
            None,
            bifrost_types::UnsupportedReason::OperationUnavailable,
            "JMAP has no organization-directory concept",
        );
        Box::pin(async move { Err(err) })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        calendar_ops::calendars_list(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
        )
    }

    fn events_in_range(
        &self,
        range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar_ops::events_in_range(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            range,
        )
    }

    fn event_get(&self, event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        calendar_ops::get(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            event,
        )
    }

    fn event_create(&self, event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        calendar_ops::create(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            event,
        )
    }

    fn event_update(
        &self,
        event: EventId,
        patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::update(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            event,
            patch,
        )
    }

    fn event_delete(&self, event: EventId) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::delete(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            event,
        )
    }

    fn event_delete_if_version(
        &self,
        event: EventId,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::delete_if_version(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            event,
            expected_version,
        )
    }

    fn event_rsvp(
        &self,
        event: EventId,
        status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::rsvp(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            self.self_emails.clone(),
            event,
            status,
        )
    }

    fn event_rsvp_if_version(
        &self,
        event: EventId,
        status: RsvpStatus,
        expected_version: Option<bifrost_types::ServerVersion>,
    ) -> AccountFuture<Result<(), AccountError>> {
        calendar_ops::rsvp_if_version(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            self.self_emails.clone(),
            event,
            status,
            expected_version,
        )
    }

    fn event_search(
        &self,
        request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        calendar_ops::search(
            self.registry
                .family(&self.registry.families.calendars)
                .map(|slot| slot.mail.clone()),
            request,
        )
    }

    fn thread_hydrate(
        &self,
        thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        pim::thread_hydrate(Arc::clone(&self.registry), thread)
    }

    fn message_hydrate(
        &self,
        message: ObjectId,
        projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        pim::message_hydrate(Arc::clone(&self.registry), message, projection)
    }

    fn move_thread(
        &self,
        thread: ThreadId,
        target: ContainerId,
        source: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_thread(&thread);
        pim::move_thread(mail.clone(), thread, target, source)
    }

    fn apply_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        match label.provenance.kind {
            ContainerKind::Label => self.set_keyword(target, label.provenance.native.into(), true),
            ContainerKind::Folder => self.add_to_container(target, label.id),
            _ => {
                let err = super::error::unsupported_error(
                    AccountOperation::SetLabelMembership,
                    None,
                    bifrost_types::UnsupportedReason::LabelKind,
                    "JMAP does not support this label kind",
                );
                Box::pin(async move { Err(err) })
            }
        }
    }

    fn remove_label(
        &self,
        target: MutationTarget,
        label: Label,
    ) -> AccountFuture<Result<(), AccountError>> {
        match label.provenance.kind {
            ContainerKind::Label => self.set_keyword(target, label.provenance.native.into(), false),
            ContainerKind::Folder => self.remove_from_container(target, label.id),
            _ => {
                let err = super::error::unsupported_error(
                    AccountOperation::SetLabelMembership,
                    None,
                    bifrost_types::UnsupportedReason::LabelKind,
                    "JMAP does not support this label kind",
                );
                Box::pin(async move { Err(err) })
            }
        }
    }

    fn delete_thread(
        &self,
        thread: ThreadId,
        current: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        let mail = self.mail_for_thread(&thread);
        pim::delete_thread(mail.clone(), thread, current)
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        let closed = Arc::clone(&self.closed);
        let close_completion = Arc::clone(&self.close_completion);
        let client = self.client.clone();
        let shutdown = self.shutdown.clone();
        let subscriptions = Arc::clone(&self.subscriptions);
        let enabled = Arc::clone(&self.ws.enabled);
        let push_state = Arc::clone(&self.ws.push_state);
        // The same bound a push session applies to its own setup awaits.
        // The disable below is a WebSocket sink write, and a half-open
        // socket parks it indefinitely - which would block the engine's
        // teardown and reopen on a connection that is already gone.
        // Teardown must be bounded on exactly the paths that motivated
        // bounding the session's setup.
        let teardown_timeout = self.ws.teardown_timeout();

        close_once(close_completion, move || {
            // This transition must precede the push locks: a subscribe
            // already holding them may finish and then be disabled below,
            // while later subscribers observe the closed gate.
            closed.store(true, Ordering::Release);
            shutdown.cancel();
            async move {
                let result = match tokio::time::timeout(
                    teardown_timeout,
                    super::push::disable_for_close(client, subscriptions, enabled, push_state),
                )
                .await
                {
                    Ok(result) => result,
                    // The unsubscribe never made it out. The connection is
                    // being dropped anyway and the server drops the
                    // subscription with it, so this is not a close failure.
                    Err(_) => Ok(()),
                };
                // No reader task to join: push sessions are engine-driven and
                // observe `shutdown`, cancelled above, at every await.
                close_outcome(result)
            }
        })
    }
}

/// What `close()` reports for its best-effort push disable.
///
/// Three outcomes are not close failures, for one reason: RFC 8887 push is
/// enabled per connection, so a subscription cannot outlive its
/// connection. No connection ever made (`WebSocketNotConnected`) has no
/// subscription. A sink write that failed (`WebSocketSend`) found the
/// connection finished - every failed write leaves the stream closed for
/// good - so its subscription is already gone; this used to fail `close()`
/// as a `Protocol(PartialResponse)` after every WebSocket drop, for a
/// teardown that had nothing left to tear down. The timeout arm in
/// `close()` is the same argument for a wedged sink.
fn close_outcome(result: crate::Result<()>) -> Result<(), AccountError> {
    match result {
        Ok(()) | Err(crate::Error::WebSocketNotConnected | crate::Error::WebSocketSend(_)) => {
            Ok(())
        }
        Err(err) => Err(super::error::into_account_error(
            err,
            super::error::JmapErrorContext::new(bifrost_types::AccountOperation::Close),
        )),
    }
}

/// Pure send-as routing decision. Returns `Ok(())` when the request may be
/// dispatched to the foreign account named by `send_as`, or the boundary
/// rejection otherwise. Precedence: the feature being absent outright is
/// answered first; then scheduled foreign sends (their bare submission handles
/// cannot be safely cancelled or rescheduled through the primary account); then
/// an id absent from the seeded routing table, which is a malformed request
/// (the consumer got it from foreign membership ownership); then a
/// known-but-not-submission-capable id, which is unsupported.
///
/// `feature_available` is `pim_methods.send_as`, i.e. whether ANY seeded
/// foreign account advertises submission. It has to be consulted here because
/// `SendRequest::send_as` makes the capability flag decide between the two
/// rejection classes, and without it an account that advertises `send_as ==
/// false` still answered `Request(Malformed)` - telling a consumer to correct
/// an argument when no argument would have worked.
///
/// This cannot refuse a request that previously routed: anything that reached
/// `Ok(())` selected a shared slot with submission support, so that feature is available,
/// which is exactly the condition that makes `feature_available` true.
// Preserve the precedence below: an Outlook user must first learn whether
// send-as exists at all, then whether scheduling is supported, then whether
// their chosen mailbox is valid and can submit. These are distinct refusal
// reasons exposed through AccountError::unsupported_reason.
fn route_send_as(
    send_as: &SendAs,
    feature_available: bool,
    scheduled: bool,
    known: bool,
    submission_capable: bool,
) -> Result<(), AccountError> {
    if !feature_available {
        return Err(bifrost_types::compose::classify_send_as_rejection(
            feature_available,
            bifrost_types::Protocol::Jmap,
            bifrost_types::DiagnosticText::support_only(
                "no seeded foreign account advertises submission, \
                 so send-as is unavailable on this account",
            ),
        ));
    }
    if scheduled {
        return Err(super::error::unsupported_error(
            AccountOperation::Send,
            None,
            bifrost_types::UnsupportedReason::ForeignScheduledSend,
            "scheduled foreign send is not supported",
        ));
    }
    if !known {
        return Err(super::error::send_as_unknown_account(send_as.mailbox()));
    }
    if !submission_capable {
        return Err(super::error::unsupported_error(
            AccountOperation::Send,
            None,
            bifrost_types::UnsupportedReason::ForeignSubmissionUnavailable,
            "foreign account does not advertise submission",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use bifrost_types::{
        AccountError, AccountErrorKind, AccountOperation, CursorScope, ObjectType, PrincipalId,
        QueryId, SendAs,
    };
    use tokio_util::sync::CancellationToken;

    use super::super::{foreign, state};
    use super::{
        SubmissionAccount, describe_cursor_support, draft_mail_account, fresh_cursor_establishment,
        partitioning_for_scope, route_send_as, unseeded_scope_error,
    };
    use bifrost_types::{ChangeCursor, CostClass, SyncStrategy};

    fn folder_scope(account_id: &str) -> CursorScope {
        CursorScope::Folder(foreign::encode_foreign_account(account_id))
    }

    /// A close after a WebSocket drop finds the sink dead. The push
    /// subscription died with that connection, so the disable that could
    /// not be written is not a close failure. Anything else still is.
    #[test]
    fn a_dead_websocket_sink_is_not_a_close_failure() {
        super::close_outcome(Err(crate::Error::WebSocketSend(
            tokio_websockets::Error::AlreadyClosed,
        )))
        .expect("a refused frame on a closed connection");
        super::close_outcome(Err(crate::Error::WebSocketSend(
            tokio_websockets::Error::Io(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        )))
        .expect("a write that failed mid-flush closes the connection too");
        super::close_outcome(Err(crate::Error::WebSocketNotConnected))
            .expect("no connection, no subscription");

        let error = super::close_outcome(Err(crate::Error::NotParsable("frame".to_string())))
            .expect_err("an unrelated failure is still reported");
        assert_eq!(error.operation(), Some(AccountOperation::Close));
    }

    /// Close starts only when a caller polls it. Once started, the spawned
    /// teardown outlives that caller and later close calls join its result.
    #[tokio::test]
    async fn close_starts_on_poll_and_survives_caller_cancellation() {
        let completion = Arc::new(std::sync::OnceLock::new());
        let starts = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicBool::new(false));
        let shutdown = CancellationToken::new();

        let unpolled_starts = Arc::clone(&starts);
        let unpolled_closed = Arc::clone(&closed);
        let unpolled_shutdown = shutdown.clone();
        drop(super::close_once(Arc::clone(&completion), move || {
            unpolled_starts.fetch_add(1, Ordering::Relaxed);
            unpolled_closed.store(true, Ordering::Release);
            unpolled_shutdown.cancel();
            std::future::ready(Ok::<(), AccountError>(()))
        }));
        assert_eq!(starts.load(Ordering::Relaxed), 0);
        assert!(!closed.load(Ordering::Acquire));
        assert!(!shutdown.is_cancelled());

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let first_starts = Arc::clone(&starts);
        let first_closed = Arc::clone(&closed);
        let first_shutdown = shutdown.clone();
        let mut first = super::close_once(Arc::clone(&completion), move || {
            first_starts.fetch_add(1, Ordering::Relaxed);
            first_closed.store(true, Ordering::Release);
            first_shutdown.cancel();
            async move {
                let _ = started_tx.send(());
                release_rx.await.expect("teardown release");
                Ok::<(), AccountError>(())
            }
        });
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(closed.load(Ordering::Acquire));
        assert!(shutdown.is_cancelled());
        started_rx.await.expect("teardown was spawned");
        drop(first);

        let unexpected_starts = Arc::new(AtomicUsize::new(0));
        let second_starts = Arc::clone(&unexpected_starts);
        let mut second = super::close_once(completion, move || {
            second_starts.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok::<(), AccountError>(()))
        });
        assert!(futures::poll!(second.as_mut()).is_pending());
        release_tx.send(()).expect("teardown still waits");
        second.await.expect("shared teardown completes");

        assert_eq!(starts.load(Ordering::Relaxed), 1);
        assert_eq!(unexpected_starts.load(Ordering::Relaxed), 0);
    }

    /// A full inventory is one anchored walk. Splitting it into positional
    /// page partitions would lose the anchor at every partition boundary.
    #[test]
    fn inventory_is_not_split_across_unstable_positional_boundaries() {
        for scope in [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Type(ObjectType::Mailbox),
            folder_scope("shared"),
            CursorScope::Account,
        ] {
            assert!(
                matches!(
                    partitioning_for_scope(&scope, 42),
                    bifrost_types::InventoryPartitioning::Full
                ),
                "{scope:?} enumerates in one pass"
            );
        }
    }

    #[derive(Clone)]
    struct CursorProbeTransport {
        calls: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl crate::core::transport::HttpTransport for CursorProbeTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).expect("request JSON");
            let call = request["methodCalls"][0].clone();
            self.calls.lock().expect("calls").push(call.clone());
            let method = call[0].as_str().expect("method name").to_string();
            let call_id = call[2].as_str().expect("call id").to_string();
            let account = call[1]["accountId"].as_str().expect("account id");
            let state = match method.as_str() {
                "Email/get" => format!("fresh-email-{account}"),
                "Mailbox/get" => format!("fresh-mailbox-{account}"),
                other => panic!("unexpected method {other}"),
            };
            let response = serde_json::json!({
                "sessionState": "session-1",
                "methodResponses": [[method, {
                    "accountId": account,
                    "state": state,
                    "list": [],
                    "notFound": []
                }, call_id]]
            });
            Ok(bytes::Bytes::from(response.to_string()))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no upload"))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no download"))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new("no session"))
        }
    }

    fn cursor_probe_session() -> crate::core::session::Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 2,
                    "maxObjectsInSet": 2,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                "shared": {"name": "Shared", "isPersonal": false, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session parses")
    }

    #[tokio::test]
    async fn cursor_establishment_probes_fresh_state_for_each_seeded_scope() {
        let transport = CursorProbeTransport {
            calls: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let client = crate::client::Client::with_transport(
            transport.clone(),
            cursor_probe_session(),
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let email_states: super::super::state_cache::StateMap =
            crate::sync::state_cache::seeded(HashMap::from([
                ("primary".to_string(), Some("open-email".into())),
                ("shared".to_string(), Some("open-shared-email".into())),
            ]));
        let cases = [
            (
                crate::account::Account::new(client.clone(), "primary"),
                CursorScope::Type(ObjectType::Email),
                "fresh-email-primary",
                "primary",
            ),
            (
                crate::account::Account::new(client.clone(), "primary"),
                CursorScope::Type(ObjectType::Mailbox),
                "fresh-mailbox-primary",
                "primary",
            ),
            (
                crate::account::Account::new(client, "shared"),
                folder_scope("shared"),
                "fresh-email-shared",
                "shared",
            ),
        ];

        for (mail, scope, expected_state, account_id) in cases {
            let established = fresh_cursor_establishment(
                crate::sync::registry::Slot::from_test(mail, Arc::clone(&email_states)),
                scope.clone(),
            )
            .await
            .expect("a seeded cursor is re-probed");
            let bifrost_types::CursorEstablishment::Ready(cursor) = established else {
                panic!("seeded cursor is Ready");
            };
            assert_eq!(cursor.scope, scope);
            let (_, state_string) = state::decode_cursor(&cursor).expect("cursor decodes");
            assert_eq!(state_string, expected_state);
            assert_eq!(cursor.advanced_through, None);
            assert_eq!(
                cursor.envelope_version.get(),
                state::OUTER_CURSOR_ENVELOPE_VERSION
            );
            if !matches!(scope, CursorScope::Type(ObjectType::Mailbox)) {
                let cached = super::super::state_cache::guard(&email_states, account_id).await;
                assert_eq!(cached.as_deref(), Some(expected_state));
            }
        }

        let calls = transport.calls.lock().expect("calls").clone();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0][0], "Email/get");
        assert_eq!(calls[0][1]["ids"], serde_json::json!([]));
        assert_eq!(calls[1][0], "Mailbox/get");
        assert_eq!(calls[1][1]["ids"], serde_json::json!([]));
        assert_eq!(calls[2][0], "Email/get");
        assert_eq!(calls[2][1]["accountId"], "shared");
    }

    /// An unseeded scope is `Unsupported`, NOT a cursor over an empty
    /// state - the latter would present as a working cursor that returns
    /// nothing forever. The error must carry the cursor scope so the
    /// engine can route it.
    #[test]
    fn an_unseeded_scope_is_unsupported_and_names_its_scope() {
        let scope = CursorScope::Type(ObjectType::Mailbox);

        let error = unseeded_scope_error(scope.clone());
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Unsupported(AccountOperation::EstablishCursor)
        ));
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(scope)),
            "the engine routes the failure by cursor scope"
        );
    }

    #[derive(Clone)]
    struct DraftUploadTransport {
        expected_account: &'static str,
    }

    impl crate::core::transport::HttpTransport for DraftUploadTransport {
        async fn api_request(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            panic!("attachment upload needs no API request");
        }

        async fn upload(
            &self,
            url: &str,
            body: Vec<u8>,
            content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            assert_eq!(
                url,
                format!("https://example.test/upload/{}", self.expected_account)
            );
            assert_eq!(body, b"attachment bytes");
            assert_eq!(content_type, Some("text/plain"));
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "accountId": self.expected_account,
                    "blobId": "uploaded-blob",
                    "type": "text/plain",
                    "size": body.len()
                })
                .to_string(),
            ))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            panic!("attachment upload needs no download");
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            panic!("session is supplied in memory");
        }
    }

    #[tokio::test]
    async fn draft_attachment_upload_uses_draft_namespace_with_mail_fallback() {
        for (has_submission, expected_account) in [(true, "submission"), (false, "mail")] {
            let session = serde_json::from_value(serde_json::json!({
                "capabilities": {
                    "urn:ietf:params:jmap:core": {
                        "maxSizeUpload": 50_000_000,
                        "maxConcurrentUpload": 4,
                        "maxSizeRequest": 10_000_000,
                        "maxConcurrentRequests": 4,
                        "maxCallsInRequest": 16,
                        "maxObjectsInGet": 500,
                        "maxObjectsInSet": 500,
                        "collationAlgorithms": []
                    }
                },
                "accounts": {"submission": {"name": "Submission", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}},
                "primaryAccounts": {},
                "username": "user@example.test",
                "apiUrl": "https://example.test/api",
                "uploadUrl": "https://example.test/upload/{accountId}",
                "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
                "eventSourceUrl": "https://example.test/events",
                "state": "session-1"
            }))
            .expect("session parses");
            let client = crate::client::Client::with_transport(
                DraftUploadTransport { expected_account },
                session,
                "https://example.test/session",
            )
            .expect("client builds");
            let mail = crate::sync::registry::Slot::new(client.clone(), "mail");
            let submission = SubmissionAccount::new(client, "submission");
            let selected = has_submission.then_some(&submission);
            let stream = Box::pin(futures::stream::iter(vec![Ok(bytes::Bytes::from_static(
                b"attachment bytes",
            ))]));
            let handle =
                super::upload_draft_attachment(&mail, selected, stream, "text/plain".to_string())
                    .await
                    .expect("upload succeeds in draft account");
            assert!(!handle.0.is_empty());
            assert_eq!(
                draft_mail_account(&mail, selected, AccountOperation::DraftCreate)
                    .unwrap()
                    .id_str(),
                expected_account
            );
        }
    }

    /// The draft lifecycle follows the submission namespace, and a Submission
    /// account without Mail is refused rather than silently swapped for the
    /// Mail account: a draft created elsewhere could never be sent from it.
    #[test]
    fn draft_lifecycle_refuses_a_submission_account_without_mail() {
        let session = serde_json::from_value(serde_json::json!({
            "capabilities": {},
            "accounts": {
                "mail": {"name": "Mail", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                "submission": {"name": "Submission", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:submission": {}}}
            },
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/api",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "eventSourceUrl": "https://example.test/events",
            "state": "session-1"
        }))
        .expect("session parses");
        let client = crate::client::Client::with_transport(
            DraftUploadTransport {
                expected_account: "unused",
            },
            session,
            "https://example.test/session",
        )
        .expect("client builds");
        let mail = crate::sync::registry::Slot::new(client.clone(), "mail");
        let submission = SubmissionAccount::new(client, "submission");
        for op in [
            AccountOperation::DraftCreate,
            AccountOperation::DraftUpdate,
            AccountOperation::DraftDiscard,
        ] {
            assert!(
                draft_mail_account(&mail, Some(&submission), op).is_err(),
                "{op:?} must not fall back to the Mail account"
            );
        }
    }

    #[test]
    fn send_as_refusal_reasons_follow_routing_precedence() {
        use bifrost_types::UnsupportedReason;
        for (available, scheduled, known, capable, reason) in [
            (
                false,
                true,
                false,
                false,
                UnsupportedReason::SendAsUnavailable,
            ),
            (
                true,
                true,
                false,
                false,
                UnsupportedReason::ForeignScheduledSend,
            ),
            (
                true,
                false,
                true,
                false,
                UnsupportedReason::ForeignSubmissionUnavailable,
            ),
        ] {
            let error = route_send_as(&as_shared(), available, scheduled, known, capable)
                .expect_err("refused");
            assert_eq!(error.unsupported_reason(), Some(reason));
            assert_eq!(
                error.kind(),
                &AccountErrorKind::Unsupported(AccountOperation::Send)
            );
        }
        let unknown =
            route_send_as(&as_shared(), true, false, false, false).expect_err("unknown mailbox");
        assert_eq!(unknown.unsupported_reason(), None);
        assert_eq!(
            unknown.kind(),
            &AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
    }

    fn as_shared() -> SendAs {
        SendAs::As(PrincipalId("foreign-id".to_string()))
    }

    #[test]
    fn route_send_as_known_submission_capable_ok() {
        assert!(route_send_as(&as_shared(), true, false, true, true).is_ok());
    }

    #[test]
    fn route_send_as_scheduled_is_unsupported() {
        let err =
            route_send_as(&as_shared(), true, true, true, true).expect_err("scheduled rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    #[test]
    fn route_send_as_unknown_account_is_malformed() {
        let err = route_send_as(&as_shared(), true, false, false, false)
            .expect_err("unknown id rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
    }

    #[test]
    fn route_send_as_known_not_submission_capable_is_unsupported() {
        let err = route_send_as(&as_shared(), true, false, true, false)
            .expect_err("no submission rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::Unsupported(AccountOperation::Send)
        ));
    }

    /// With the feature absent, an unknown id is `Unsupported`, NOT the
    /// `Malformed` the same inputs produce when it is present.
    ///
    /// This is the capability flag selecting the rejection class, which is the
    /// rule `SendRequest::send_as` states and which this function did not read
    /// until 2026-09-15: an account advertising `send_as == false` answered
    /// `Request(Malformed)`, telling a consumer to correct an argument when no
    /// argument could have worked. The contrast with
    /// `route_send_as_unknown_account_is_malformed` - identical but for this
    /// one bool - is the whole assertion.
    #[test]
    fn route_send_as_unknown_account_is_unsupported_when_the_feature_is_absent() {
        let err = route_send_as(&as_shared(), false, false, false, false)
            .expect_err("send-as is unavailable");
        assert!(
            matches!(
                err.kind(),
                AccountErrorKind::Unsupported(AccountOperation::Send)
            ),
            "got {:?}",
            err.kind()
        );
    }

    fn cursor(scope: CursorScope) -> ChangeCursor {
        state::cursor_for_scope(scope, "state-1").expect("scope encodes")
    }

    #[test]
    fn supported_scopes_describe_a_server_cursor() {
        for scope in [
            CursorScope::Type(ObjectType::Email),
            CursorScope::Type(ObjectType::Mailbox),
            CursorScope::Folder(foreign::encode_foreign("acct-9", "mbx-1")),
        ] {
            let descriptor = describe_cursor_support(&cursor(scope.clone()));
            assert_eq!(descriptor.cost_class, CostClass::Cheap, "{scope:?}");
            assert_eq!(descriptor.strategy, SyncStrategy::ServerCursor, "{scope:?}");
        }
    }

    #[test]
    fn legacy_thread_and_query_cursors_are_not_advertised_as_usable() {
        // Both decode (their scope tags are unchanged by the envelope
        // version bump, so old durable rows stay readable) but
        // `changes::stream` terminates them Unsupported(SyncChanges), so
        // the descriptor must not promise a strategy that dies on its
        // first poll. Strategy `None` routes the consumer back through
        // inventory instead.
        for scope in [
            CursorScope::Type(ObjectType::Thread),
            CursorScope::Query(QueryId("unread-in-inbox".to_string())),
        ] {
            let legacy = cursor(scope.clone());
            assert!(
                state::decode_cursor(&legacy).is_ok(),
                "premise: the legacy cursor still decodes ({scope:?})"
            );
            let descriptor = describe_cursor_support(&legacy);
            assert_eq!(descriptor.cost_class, CostClass::Expensive, "{scope:?}");
            assert_eq!(descriptor.strategy, SyncStrategy::None, "{scope:?}");
        }
    }

    #[test]
    fn an_undecodable_cursor_describes_no_strategy() {
        let mut broken = cursor(CursorScope::Type(ObjectType::Email));
        broken.server_state.bytes.pop();
        let descriptor = describe_cursor_support(&broken);
        assert_eq!(descriptor.cost_class, CostClass::Expensive);
        assert_eq!(descriptor.strategy, SyncStrategy::None);
    }
}
