//! Read-back guard and campaign read-back tests.
//!
//! `FlagsAccount` returns predetermined flag sets per id and drives the
//! published guards and the scripted cohort campaigns. `JudgingAccount`
//! overrides `Account::flag_target`. `ReassertAccount` scripts every
//! mutation round and read-back observation to pin desired-state
//! reassertion after a valid unsatisfied observation.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bifrost_sync::{Error, IdempotencyVendor, SyncEngine, run_readback_guard};
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountErrorBuilder, AccountErrorKind,
    AccountFactory, AccountFuture, AccountId, AccountStream, AttachmentHandle, Batch,
    BatchingPolicy, BlobHandle, BlobRangeSupport, ByteRange, Cause, Change, ChangeCursor,
    CloudUploadMeta, ContactCard, ContactCreate, ContactId, ContactPatch, ContactSearchRequest,
    ContainerId, ContainerKind, Control, ConvenienceShape, CursorDescriptor, CursorEstablishment,
    CursorFreshness, CursorScope, DraftHandle, DraftPatch, EventCreate, EventId, EventPatch,
    EventRange, EventSearchRequest, FilterRuleShape, FilterValidation, FlagOp, HostedAttachment,
    HydratedObject, HydratedObjectKind, HydrationProjection, IdempotencyKey, Identity, IdentityId,
    IdentityPatch, Importance, InventoryEvent, ItemOutcome, MembershipScope, Message,
    MutationCapabilities, MutationConcurrency, MutationReplaySafety, MutationSuccess,
    MutationTarget, ObjectId, OpenedAccount, Page, PageBoundary, PimMethodSupport, Priority,
    Projection, ProtocolKind, PushCapability, QuotaInfo, QuotaSignal, RateLimitClass, RequestCause,
    RsvpStatus, ScopeLifecycleEvent, SearchRequest, SendRequest, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch, SubscriptionHandle, SyncEvent, ThreadHydration, ThreadId,
    VacationConfig, WatchEvent,
};
use bifrost_types::{AddressBook, AddressBookId};
use bifrost_types::{Calendar, CalendarEvent};
use bytes::Bytes;
use futures::stream::{self, StreamExt};

/// Convenience for stubs that just need to signal "this operation is
/// not implemented in this test double." Maps to `Unsupported(op)` +
/// `ClientBug` recovery so any unexpected call is obvious in output.
fn unsupported(op: bifrost_types::AccountOperation) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Unsupported(op),
        Cause::Request(RequestCause::Unsupported { operation: op }),
    )
    .operation(op)
    .try_build()
    .expect("valid account error classification")
}

/// Synthetic Account that returns predetermined flags for each id on
/// `get_stream(Projection::FlagsOnly)`. All other methods are
/// unreachable in this test.
struct FlagsAccount {
    caps: AccountCapabilities,
    flag_table: std::collections::HashMap<ObjectId, HashSet<String>>,
    campaign: Option<Arc<CampaignState>>,
}

// Script selection stays atomic so a test can change the next action while
// another campaign is sleeping on a virtual-time deadline.
const COHORT_APPLY: usize = 1;
const COHORT_RETRY_ONCE: usize = 2;
const COHORT_ERROR_IN_SECOND: usize = 3;
const COHORT_UNCERTAIN: usize = 4;
const COHORT_RETRY_BESIDE_UNCERTAIN: usize = 5;
type CohortCall = (Vec<ObjectId>, IdempotencyKey, tokio::time::Instant);

#[derive(Default)]
struct CampaignState {
    cohort_mode: AtomicUsize,
    target_keys:
        std::sync::Mutex<std::collections::HashMap<ObjectId, Vec<bifrost_types::ThrottleKey>>>,
    calls: std::sync::Mutex<Vec<CohortCall>>,
    observations: std::sync::Mutex<Vec<Vec<ObjectId>>>,
    duplicate_observations: AtomicBool,
    engine_block_first: AtomicBool,
    /// First attempt: one item raises an engine directive while a SIBLING item
    /// comes back `Uncertain`, i.e. parked in the read-back lane.
    engine_block_beside_uncertain: AtomicBool,
    /// First attempt: a PER-ITEM retryable failure carrying a long provider
    /// hint, after which the stream ends `Done` rather than `Terminated`.
    per_item_retry_then_done: AtomicBool,
    gate_first_campaign: AtomicBool,
    release_first_campaign: tokio::sync::Notify,
    submissions: AtomicUsize,
    submitted: tokio::sync::Notify,
    /// Gate the FIRST read-back hydration, so a test can observe what the
    /// engine allows to start while a read-back is in flight.
    gate_readback: AtomicBool,
    readbacks: AtomicUsize,
    readback_started: tokio::sync::Notify,
    release_readback: tokio::sync::Notify,
    keys: std::sync::Mutex<Vec<IdempotencyKey>>,
    /// How many targets each successive submission actually carried, so a
    /// test can observe the width of a resubmission rather than only its
    /// count.
    widths: std::sync::Mutex<Vec<usize>>,
    /// Stamped on the `per_item_retry_then_done` failure, making it a
    /// throttle held against this budget.
    stamp_budget: std::sync::Mutex<Option<bifrost_types::ThrottleBudget>>,
    /// Declared by `throttle_keys` for every mutation target.
    object_keys: std::sync::Mutex<Vec<bifrost_types::ThrottleKey>>,
}

struct FlagsFactory(Arc<FlagsAccount>);

impl AccountFactory for FlagsFactory {
    fn open(&self, _account: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let concrete = Arc::clone(&self.0);
        let account: Arc<dyn Account> = concrete;
        Box::pin(async move { Ok(OpenedAccount::complete(account)) })
    }
}

fn retryable_transport() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network),
        Cause::Transport(bifrost_types::TransportCause::new(
            bifrost_types::TransportKind::Network,
            None,
        )),
    )
    .try_build()
    .expect("valid retryable transport error")
}

fn caps() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::None,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::None,
            replay_safety: MutationReplaySafety::None,
            reassertion: bifrost_types::MutationReassertion::NONE,
        },
        batching_policy: BatchingPolicy {
            max_items: 100,
            max_wait: std::time::Duration::from_millis(50),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::None,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport::default(),
        filter_rule_shape: FilterRuleShape::None,
        conveniences: ConvenienceShape::default(),
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

impl Account for FlagsAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.caps
    }

    fn set_priority(&self, _priority: Priority) {}

    fn set_bandwidth_cap(&self, _bps: Option<u64>) {}

    fn throttle_keys(
        &self,
        _account: &AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        match (work, self.campaign.as_ref()) {
            (bifrost_types::ThrottleWork::Objects(ids), Some(campaign)) => {
                let mut keys = campaign
                    .object_keys
                    .lock()
                    .expect("object keys lock")
                    .clone();
                let per_target = campaign.target_keys.lock().expect("target keys");
                keys.extend(
                    ids.iter()
                        .filter_map(|id| per_target.get(id))
                        .flatten()
                        .cloned(),
                );
                keys
            }
            _ => Vec::new(),
        }
    }

    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        unreachable!("describe_cursor not exercised by readback guard tests")
    }

    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        Box::pin(stream::empty())
    }

    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        Box::pin(stream::empty())
    }

    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        Box::pin(stream::empty())
    }

    fn establish_initial_cursor(
        &self,
        _scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::EstablishCursor,
            ))
        })
    }

    fn inventory_stream(&self, _scope: CursorScope) -> AccountStream<InventoryEvent> {
        Box::pin(stream::empty())
    }

    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        assert_eq!(projection, Projection::FlagsOnly);
        let table = self.flag_table.clone();
        let observer = self.campaign.clone();
        let gate = self.campaign.as_ref().and_then(|campaign| {
            (campaign.gate_readback.load(Ordering::SeqCst)
                && campaign.readbacks.fetch_add(1, Ordering::SeqCst) == 0)
                .then(|| Arc::clone(campaign))
        });
        let collected: Pin<Box<dyn futures::Future<Output = Vec<ObjectId>> + Send>> =
            Box::pin(async move { ids.collect::<Vec<_>>().await });
        let s = async move {
            if let Some(campaign) = gate {
                campaign.readback_started.notify_one();
                campaign.release_readback.notified().await;
            }
            let mut ids: Vec<ObjectId> = collected.await;
            if let Some(observer) = observer {
                observer
                    .observations
                    .lock()
                    .expect("observations")
                    .push(ids.clone());
                if observer.duplicate_observations.load(Ordering::SeqCst) {
                    ids.extend(ids.clone());
                }
            }
            let items: Vec<ItemOutcome<HydratedObject>> = ids
                .into_iter()
                .map(|id| {
                    let flags = table.get(&id).cloned().unwrap_or_default();
                    let hydrated = HydratedObject {
                        id: id.clone(),
                        kind: HydratedObjectKind::FlagsOnly(flags),
                        blobs: Vec::new(),
                    };
                    ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
                        bifrost_types::BatchItemId(id.0),
                        hydrated,
                    ))
                })
                .collect();
            SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::from_millis(1),
                bytes_in: 0,
                checkpoint: None,
            })
        };
        Box::pin(stream::once(s).chain(stream::once(async { SyncEvent::Done(None) })))
    }

    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        Box::pin(stream::empty())
    }

    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::PushSubscribe)) })
    }

    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::PushUnsubscribe,
            ))
        })
    }

    fn push_stream(&self) -> AccountStream<WatchEvent> {
        Box::pin(stream::empty())
    }

    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        Box::pin(stream::empty())
    }

    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        _op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        let Some(campaign) = &self.campaign else {
            return Box::pin(stream::empty());
        };
        campaign.keys.lock().expect("keys lock").push(key.clone());
        if campaign.cohort_mode.load(Ordering::SeqCst) != 0 {
            let campaign = Arc::clone(campaign);
            return Box::pin(stream::once(async move {
                let ids: Vec<ObjectId> = targets.collect().await;
                let mode = campaign.cohort_mode.load(Ordering::SeqCst);
                let previous = {
                    let mut calls = campaign.calls.lock().expect("calls");
                    let previous: std::collections::HashSet<_> = calls
                        .iter()
                        .flat_map(|(ids, _, _)| ids.iter().cloned())
                        .collect();
                    calls.push((ids.clone(), key, tokio::time::Instant::now()));
                    previous
                };
                campaign.submitted.notify_one();
                if mode == COHORT_ERROR_IN_SECOND && ids.iter().any(|id| id.0 == "b") {
                    return SyncEvent::Terminated(unsupported(
                        bifrost_types::AccountOperation::UpdateFlags,
                    ));
                }
                let items = ids
                    .into_iter()
                    .map(|id| {
                        let item = bifrost_types::BatchItemId(id.0.clone());
                        if mode == COHORT_ERROR_IN_SECOND
                            || mode == COHORT_UNCERTAIN
                            || (mode == COHORT_RETRY_BESIDE_UNCERTAIN && id.0 == "b")
                        {
                            return ItemOutcome::Uncertain(bifrost_types::BatchUncertain::new(
                                item,
                                retryable_transport(),
                            ));
                        }
                        if (mode == COHORT_RETRY_ONCE || mode == COHORT_RETRY_BESIDE_UNCERTAIN)
                            && !previous.contains(&id)
                        {
                            let mut builder = AccountErrorBuilder::new(
                                AccountErrorKind::Server(
                                    bifrost_types::ServerErrorKind::RateLimited,
                                ),
                                Cause::Server(bifrost_types::ServerCause::RateLimited {
                                    retry_hint: Some(bifrost_types::RetryHint::After(
                                        std::time::Duration::from_secs(30),
                                    )),
                                }),
                            );
                            if let Some(bifrost_types::ThrottleKey::Budget(budget)) = campaign
                                .target_keys
                                .lock()
                                .expect("keys")
                                .get(&id)
                                .and_then(|keys| {
                                    keys.iter().find(|key| {
                                        matches!(key, bifrost_types::ThrottleKey::Budget(_))
                                    })
                                })
                            {
                                builder = builder.throttle_budget(budget.clone());
                            }
                            return ItemOutcome::Failed(bifrost_types::BatchFailure::new(
                                item,
                                builder.try_build().expect("retry error"),
                            ));
                        }
                        ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
                            item,
                            MutationSuccess::Applied,
                        ))
                    })
                    .collect();
                SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Final,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: None,
                })
            }));
        }
        let attempt = campaign.submissions.fetch_add(1, Ordering::SeqCst);
        campaign.submitted.notify_one();
        if campaign.gate_first_campaign.load(Ordering::SeqCst) && attempt == 0 {
            let campaign = Arc::clone(campaign);
            return Box::pin(stream::once(async move {
                campaign.release_first_campaign.notified().await;
                let items = targets
                    .map(|id| {
                        ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
                            bifrost_types::BatchItemId(id.0),
                            MutationSuccess::Applied,
                        ))
                    })
                    .collect()
                    .await;
                SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Final,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: None,
                })
            }));
        }
        if campaign.per_item_retry_then_done.load(Ordering::SeqCst) && attempt == 0 {
            let stamp = campaign.stamp_budget.lock().expect("stamp lock").clone();
            return Box::pin(
                stream::once(async move {
                    let ids: Vec<ObjectId> = targets.collect().await;
                    let mut builder = AccountErrorBuilder::new(
                        AccountErrorKind::Server(bifrost_types::ServerErrorKind::RateLimited),
                        Cause::Server(bifrost_types::ServerCause::RateLimited {
                            retry_hint: Some(bifrost_types::RetryHint::After(
                                std::time::Duration::from_secs(HINTED_RETRY_SECS),
                            )),
                        }),
                    );
                    if let Some(budget) = stamp {
                        builder = builder
                            .throttle_scope(bifrost_types::ThrottleScope::Account)
                            .throttle_budget(budget);
                    }
                    let error = builder
                        .try_build()
                        .expect("valid rate-limited server error");
                    SyncEvent::Batch(Batch {
                        items: vec![ItemOutcome::Failed(bifrost_types::BatchFailure::new(
                            bifrost_types::BatchItemId(ids[0].0.clone()),
                            error,
                        ))],
                        page_boundary: PageBoundary::Final,
                        server_latency: std::time::Duration::ZERO,
                        bytes_in: 0,
                        checkpoint: None,
                    })
                })
                // Ends DONE, not Terminated: there is no stream-level retry
                // advice, so only the per-item advice can supply the delay.
                .chain(stream::once(async { SyncEvent::Done(None) })),
            );
        }
        if campaign
            .engine_block_beside_uncertain
            .load(Ordering::SeqCst)
            && attempt == 0
        {
            return Box::pin(stream::once(async move {
                let ids: Vec<ObjectId> = targets.collect().await;
                let blocked = AccountErrorBuilder::new(
                    AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CursorInvalid),
                    Cause::State(bifrost_types::StateCause::CursorInvalid),
                )
                .scope(bifrost_types::ErrorScope::Cursor(CursorScope::Account))
                .try_build()
                .expect("valid engine-blocked error");
                let items = vec![
                    ItemOutcome::Failed(bifrost_types::BatchFailure::new(
                        bifrost_types::BatchItemId(ids[0].0.clone()),
                        blocked,
                    )),
                    ItemOutcome::Uncertain(bifrost_types::BatchUncertain::new(
                        bifrost_types::BatchItemId(ids[1].0.clone()),
                        retryable_transport(),
                    )),
                ];
                SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Final,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: None,
                })
            }));
        }
        if campaign.engine_block_first.load(Ordering::SeqCst) && attempt == 0 {
            return Box::pin(stream::once(async move {
                let mut ids: Vec<ObjectId> = targets.collect().await;
                let first = ids.remove(0);
                let error = AccountErrorBuilder::new(
                    AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CursorInvalid),
                    Cause::State(bifrost_types::StateCause::CursorInvalid),
                )
                .scope(bifrost_types::ErrorScope::Cursor(CursorScope::Account))
                .try_build()
                .expect("valid engine-blocked error");
                SyncEvent::Batch(Batch {
                    items: vec![ItemOutcome::Failed(bifrost_types::BatchFailure::new(
                        bifrost_types::BatchItemId(first.0),
                        error,
                    ))],
                    page_boundary: PageBoundary::Final,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: None,
                })
            }));
        }
        if attempt == 0 {
            // Drain first so the submission width is recorded, then terminate
            // retryably without emitting any per-item outcome. Whether the
            // account consumed its own input stream is invisible to the engine,
            // which sweeps unresolved ids out of `remaining`, so this records
            // the width without changing what the campaign observes.
            let campaign = Arc::clone(campaign);
            return Box::pin(stream::once(async move {
                let width = targets.count().await;
                campaign.widths.lock().expect("widths lock").push(width);
                SyncEvent::Terminated(retryable_transport())
            }));
        }
        let widths = Arc::clone(campaign);
        Box::pin(stream::once(async move {
            let targets = {
                let collected: Vec<ObjectId> = targets.collect().await;
                widths
                    .widths
                    .lock()
                    .expect("widths lock")
                    .push(collected.len());
                stream::iter(collected)
            };
            let items = targets
                .map(|id| {
                    ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
                        bifrost_types::BatchItemId(id.0),
                        MutationSuccess::Applied,
                    ))
                })
                .collect()
                .await;
            SyncEvent::Batch(Batch {
                items,
                page_boundary: PageBoundary::Final,
                server_latency: std::time::Duration::ZERO,
                bytes_in: 0,
                checkpoint: None,
            })
        }))
    }

    /// Echoes the destination it was handed back as a single
    /// `Succeeded` item id, so a caller can prove which arguments
    /// reached the wire method.
    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        let item = ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
            bifrost_types::BatchItemId(format!("{destination:?}")),
            MutationSuccess::Applied,
        ));
        Box::pin(stream::iter([SyncEvent::Batch(Batch {
            items: vec![item],
            page_boundary: PageBoundary::Page,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: None,
        })]))
    }

    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        Box::pin(stream::empty())
    }

    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Ok(()) })
    }

    // PIM primitives stubbed to Err(Unsupported); the read-back guard
    // only exercises `get_stream`.
    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::AddToContainer)) })
    }

    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::RemoveFromContainer,
            ))
        })
    }

    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetKeyword)) })
    }

    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::SetLabelMembership,
            ))
        })
    }

    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetCategory)) })
    }

    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::SetExtendedProperty,
            ))
        })
    }

    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetIsRead)) })
    }

    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SetImportance)) })
    }

    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::Send)) })
    }

    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::AttachmentUpload,
            ))
        })
    }

    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HostAttachment)) })
    }

    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftCreate)) })
    }

    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftUpdate)) })
    }

    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftDiscard)) })
    }

    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::DraftSend)) })
    }

    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::CancelScheduledSend,
            ))
        })
    }

    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::RescheduleSend)) })
    }

    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::Search)) })
    }

    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::SearchMessages)) })
    }

    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainersList)) })
    }

    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerCreate,
            ))
        })
    }

    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerRename,
            ))
        })
    }

    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContainerMove)) })
    }

    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::ContainerDelete,
            ))
        })
    }

    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::IdentitiesList)) })
    }

    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::IdentityUpdate)) })
    }

    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::VacationGet)) })
    }

    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::VacationSet)) })
    }

    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::QuotaGet)) })
    }

    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FiltersList)) })
    }

    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterCreate)) })
    }

    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterUpdate)) })
    }

    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterDelete)) })
    }

    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::FilterValidate)) })
    }

    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::AddressBooksList,
            ))
        })
    }

    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactsList)) })
    }

    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactGet)) })
    }

    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactCreate)) })
    }

    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactUpdate)) })
    }

    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactDelete)) })
    }

    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::ContactSearch)) })
    }

    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryCard>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectorySearch,
            ))
        })
    }

    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroup>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectoryGroupsList,
            ))
        })
    }

    fn directory_group_expand(
        &self,
        _group: bifrost_types::DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroupMember>, AccountError>> {
        Box::pin(async {
            Err(unsupported(
                bifrost_types::AccountOperation::DirectoryGroupExpand,
            ))
        })
    }

    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::CalendarsList)) })
    }

    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventsInRange)) })
    }

    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventGet)) })
    }

    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventCreate)) })
    }

    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventUpdate)) })
    }

    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventDelete)) })
    }

    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventRsvp)) })
    }

    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::EventSearch)) })
    }

    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HydrateThread)) })
    }

    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::HydrateMessage)) })
    }
}

fn set(items: &[&str]) -> HashSet<String> {
    items.iter().map(|s| (*s).to_string()).collect()
}

#[tokio::test]
async fn readback_guard_marks_applied_as_skipped() {
    // Three ids retried. The server actually applied \Seen to id `a`
    // and id `b` (so they read back with the flag), but not to `c`.
    // The guard must mark a + b as skipped, leave c as still_failed.
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&["\\Seen", "\\Flagged"]));
    table.insert(ObjectId("c".into()), set(&["\\Flagged"]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
        campaign: None,
    };
    let ids = vec![
        ObjectId("a".into()),
        ObjectId("b".into()),
        ObjectId("c".into()),
    ];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Add(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 2);
    assert_eq!(outcome.still_failed, 1);
}

#[tokio::test]
async fn readback_guard_handles_remove() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&[]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
        campaign: None,
    };
    let ids = vec![ObjectId("a".into()), ObjectId("b".into())];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Remove(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 1, "b is already without \\Seen");
    assert_eq!(outcome.still_failed, 1, "a still has \\Seen");
}

#[tokio::test]
async fn readback_guard_handles_set() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("a".into()), set(&["\\Seen"]));
    table.insert(ObjectId("b".into()), set(&["\\Seen", "\\Flagged"]));
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: table,
        campaign: None,
    };
    let ids = vec![ObjectId("a".into()), ObjectId("b".into())];
    let outcome = run_readback_guard(&acc, ids, &FlagOp::Set(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 1, "only a matches exact set");
    assert_eq!(outcome.still_failed, 1);
}

#[tokio::test]
async fn readback_guard_with_no_ids_is_no_op() {
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: std::collections::HashMap::new(),
        campaign: None,
    };
    let outcome = run_readback_guard(&acc, vec![], &FlagOp::Add(set(&["\\Seen"])))
        .await
        .expect("guard ok");
    assert_eq!(outcome.skipped, 0);
    assert_eq!(outcome.still_failed, 0);
}

/// The `bulk_move_from` default impl must forward to `bulk_move` with
/// the destination intact. An Account that only implements the
/// relocating form (IMAP / Graph / JMAP) already vacates the source as
/// part of the move, so dropping `source` here is the correct default,
/// but the destination must survive.
#[tokio::test]
async fn bulk_move_from_defaults_to_bulk_move() {
    let acc = FlagsAccount {
        caps: caps(),
        flag_table: std::collections::HashMap::new(),
        campaign: None,
    };
    let destination = MembershipScope::Label(bifrost_types::LabelId("Label_42".into()));
    let source = MembershipScope::Label(bifrost_types::LabelId("Label_7".into()));
    let events: Vec<_> = acc
        .bulk_move_from(
            Box::pin(stream::empty()),
            destination.clone(),
            Some(source),
            key(),
        )
        .collect()
        .await;
    let ids = events
        .into_iter()
        .filter_map(|event| match event {
            SyncEvent::Batch(batch) => Some(batch.items),
            _ => None,
        })
        .flatten()
        .filter_map(|item| match item {
            ItemOutcome::Succeeded(success) => Some(success.item.0),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![format!("{destination:?}")]);
}

fn key() -> IdempotencyKey {
    IdempotencyKey {
        run_id: bifrost_types::RunId("test-run".into()),
        sequence: 0,
        protocol_salt: bifrost_types::ProtocolSalt::Gmail("test-salt".into()),
    }
}

/// Cross-check: a guard run against an arc'd Account behaves the same.
#[tokio::test]
async fn readback_guard_works_through_arc() {
    let mut table = std::collections::HashMap::new();
    table.insert(ObjectId("only".into()), set(&["\\Seen"]));
    let acc: Arc<dyn Account> = Arc::new(FlagsAccount {
        caps: caps(),
        flag_table: table,
        campaign: None,
    });
    let outcome = run_readback_guard(
        acc.as_ref(),
        vec![ObjectId("only".into())],
        &FlagOp::Add(set(&["\\Seen"])),
    )
    .await
    .expect("guard ok");
    assert_eq!(outcome.skipped, 1);
    assert_eq!(outcome.still_failed, 0);
}

/// Long enough that an un-delayed resubmission is unambiguous.
const HINTED_RETRY_SECS: u64 = 600;

fn campaign_account(state: Arc<CampaignState>) -> Arc<FlagsAccount> {
    Arc::new(FlagsAccount {
        caps: caps(),
        flag_table: std::collections::HashMap::new(),
        campaign: Some(state),
    })
}

fn vendor() -> IdempotencyVendor {
    IdempotencyVendor::with_run_id(
        bifrost_types::RunId("boundary-test".into()),
        Box::new(|_| bifrost_types::ProtocolSalt::Imap),
    )
}

#[tokio::test(start_paused = true)]
async fn paused_campaign_resumes_once_without_losing_retry_accounting() {
    let state = Arc::new(CampaignState::default());
    let engine = Arc::new(SyncEngine::builder().build().expect("engine"));
    let account_id = AccountId("mutation-boundary".into());
    let control = engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");
    let campaign_engine = Arc::clone(&engine);
    let campaign_id = account_id.clone();
    let campaign = tokio::spawn(async move {
        campaign_engine
            .bulk_set_flags(
                &campaign_id,
                vec![ObjectId("one".into())],
                FlagOp::Add(set(&["\\Seen"])),
                &vendor(),
                ProtocolKind::Imap,
            )
            .await
    });

    state.submitted.notified().await;
    control.pause().await.expect("pause");
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(10)).await;
    tokio::task::yield_now().await;
    assert_eq!(state.submissions.load(Ordering::SeqCst), 1);

    control.resume();
    let counters = campaign.await.expect("campaign task").expect("campaign");
    assert_eq!(counters.applied, 1);
    assert_eq!(counters.pending_retry, 0);
    assert_eq!(state.submissions.load(Ordering::SeqCst), 2);
    {
        let keys = state.keys.lock().expect("keys lock");
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].run_id, keys[1].run_id);
        assert_eq!(keys[0].sequence, keys[1].sequence);
        assert_eq!(keys[0].protocol_salt, keys[1].protocol_salt);
    }
    engine.detach(&account_id).await.expect("detach");
}

/// `MutationConfig::retry_queue_cap` bounds how wide ONE resubmission may be,
/// and the targets it defers are reported rather than dropped.
///
/// The first submission terminates retryably with no per-item outcome, so all
/// five targets become retry candidates. With the cap at two, the second
/// submission may carry only two of them - and the other three must still be
/// accounted for, because an id merely truncated out of the resubmission is no
/// longer among the campaign's outstanding targets and no later attempt will
/// resolve it. Before the cap was wired, the field was never read and the
/// second submission carried all five.
#[tokio::test(start_paused = true)]
async fn retry_queue_cap_bounds_a_resubmission_without_losing_the_excess() {
    let state = Arc::new(CampaignState::default());
    let mut config = bifrost_sync::EngineConfig::default();
    config.mutation.retry_queue_cap = 2;
    let engine = Arc::new(
        SyncEngine::builder()
            .config(config)
            .build()
            .expect("engine"),
    );
    let account_id = AccountId("retry-queue-cap".into());
    engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");

    let counters = engine
        .bulk_set_flags(
            &account_id,
            (0..5)
                .map(|n| ObjectId(format!("id-{n}")))
                .collect::<Vec<_>>(),
            FlagOp::Add(set(&["\\Seen"])),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .expect("campaign");

    {
        let widths = state.widths.lock().expect("widths lock");
        assert_eq!(
            widths.as_slice(),
            &[5, 2],
            "the resubmission must be capped at retry_queue_cap"
        );
    }
    // Two were retried and applied. The three the cap deferred are marked
    // pending and then run through the read-back guard, which resolves them
    // against real server state - so they land in the skipped or still-failed
    // lane rather than staying nominally outstanding. Which lane is the
    // guard's business; what this pins is that all five are still accounted
    // for. Truncating the resubmission WITHOUT marking the excess drops them
    // from every lane and this total falls to two.
    assert_eq!(counters.applied, 2);
    let accounted = counters.applied
        + counters.skipped
        + counters.failed_terminal
        + counters.blocked_by_engine
        + counters.pending_retry;
    assert_eq!(
        accounted, 5,
        "every submitted target must land in exactly one lane: {counters:?}"
    );
    engine.detach(&account_id).await.expect("detach");
}

#[tokio::test]
async fn per_item_engine_block_accounts_for_unseen_campaign_ids() {
    let state = Arc::new(CampaignState::default());
    state.engine_block_first.store(true, Ordering::SeqCst);
    let engine = SyncEngine::builder().build().expect("engine");
    let account_id = AccountId("per-item-engine-block".into());
    engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(state))),
        )
        .await
        .expect("attach");

    let counters = engine
        .bulk_set_flags(
            &account_id,
            vec![
                ObjectId("one".into()),
                ObjectId("two".into()),
                ObjectId("three".into()),
            ],
            FlagOp::Add(set(&["\\Seen"])),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .expect("campaign");

    assert_eq!(counters.blocked_by_engine, 3);
    engine.detach(&account_id).await.expect("detach");
}

/// An engine directive raised by one item must not erase a SIBLING item's
/// read-back protection.
///
/// The read-back lane exists so a write that may already have landed is
/// verified against observed state instead of replayed or guessed at. The
/// engine-directive sweep used to overwrite every outcome that was not already
/// terminal, `PendingReadback` included, which emptied the read-back set,
/// skipped the guard entirely, and reported a mutation that had actually
/// applied as `blocked_by_engine`.
#[tokio::test]
async fn an_engine_directive_leaves_a_sibling_read_back_alone() {
    let state = Arc::new(CampaignState::default());
    state
        .engine_block_beside_uncertain
        .store(true, Ordering::SeqCst);
    let account = Arc::new(FlagsAccount {
        caps: caps(),
        flag_table: std::collections::HashMap::from([(ObjectId("two".into()), set(&["\\Seen"]))]),
        campaign: Some(Arc::clone(&state)),
    });
    let engine = SyncEngine::builder().build().expect("engine");
    let account_id = AccountId("engine-block-beside-readback".into());
    engine
        .attach(account_id.clone(), Arc::new(FlagsFactory(account)))
        .await
        .expect("attach");

    let counters = engine
        .bulk_set_flags(
            &account_id,
            vec![ObjectId("one".into()), ObjectId("two".into())],
            FlagOp::Add(set(&["\\Seen"])),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .expect("campaign");

    assert_eq!(
        counters.blocked_by_engine, 1,
        "only the id that actually raised the directive is engine-blocked: {counters:?}"
    );
    assert_eq!(
        counters.skipped, 1,
        "the uncertain sibling must still be read back and reconciled - observed state \
         already carries the flag, which the guard downgrades to skipped: {counters:?}"
    );
    engine.detach(&account_id).await.expect("detach");
}

/// A stream that ends `Done` after emitting per-item retryable failures must
/// still honor the delay before resubmitting.
///
/// `retry_advice` used to be set only from a STREAM-LEVEL `Retry` termination,
/// so this shape looped straight into the next attempt with no sleep at all -
/// `mutation_max_retries` back-to-back resubmissions against a provider that
/// had just named a deadline, while every other retry path in the engine
/// honors the hint.
#[tokio::test(start_paused = true)]
async fn a_per_item_retry_delays_the_resubmission() {
    let state = Arc::new(CampaignState::default());
    state.per_item_retry_then_done.store(true, Ordering::SeqCst);
    let engine = SyncEngine::builder().build().expect("engine");
    let account_id = AccountId("per-item-retry-delay".into());
    engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");

    let started = tokio::time::Instant::now();
    let counters = engine
        .bulk_set_flags(
            &account_id,
            vec![ObjectId("one".into())],
            FlagOp::Add(set(&["\\Seen"])),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .expect("campaign");
    let elapsed = started.elapsed();

    assert!(
        state.submissions.load(Ordering::SeqCst) >= 2,
        "the campaign must resubmit the retryable id"
    );
    assert!(
        elapsed >= std::time::Duration::from_secs(HINTED_RETRY_SECS),
        "the resubmission must honor the per-item provider hint (elapsed {elapsed:?}, \
         counters {counters:?})"
    );
    engine.detach(&account_id).await.expect("detach");
}

/// A campaign waits out a hold recorded under a budget its TARGETS declare,
/// even when another account recorded it and this campaign never failed.
///
/// One account's per-item 429 stamps budget X with a long hint. A second
/// account's campaign, whose targets declare X, must not submit until that
/// deadline passes. Nothing local to the second campaign can explain the
/// wait: it has no retry advice before its first attempt, and its own
/// account key holds nothing.
#[tokio::test(start_paused = true)]
async fn a_campaign_waits_for_a_budget_its_targets_declare() {
    // This remains a single-cohort case: every target declares the held
    // budget. Splitting campaigns must not bypass a hold on that cohort.
    let budget = bifrost_types::ThrottleBudget::new("test|mailbox:shared@example.com");
    let engine = SyncEngine::builder().build().expect("engine");

    let loud = Arc::new(CampaignState::default());
    loud.per_item_retry_then_done.store(true, Ordering::SeqCst);
    *loud.stamp_budget.lock().expect("stamp lock") = Some(budget.clone());
    let loud_id = AccountId("budget-loud-campaign".into());
    engine
        .attach(
            loud_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&loud)))),
        )
        .await
        .expect("attach loud");

    let quiet = Arc::new(CampaignState::default());
    *quiet.object_keys.lock().expect("object keys lock") =
        vec![bifrost_types::ThrottleKey::Budget(budget)];
    let quiet_id = AccountId("budget-quiet-campaign".into());
    engine
        .attach(
            quiet_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&quiet)))),
        )
        .await
        .expect("attach quiet");

    // The loud campaign records the hold on its first attempt, then sleeps
    // out its own hint before resubmitting; run it alongside.
    let loud_engine = Arc::new(engine);
    let loud_run = {
        let engine = Arc::clone(&loud_engine);
        let loud_id = loud_id.clone();
        tokio::spawn(async move {
            engine
                .bulk_set_flags(
                    &loud_id,
                    vec![ObjectId("one".into())],
                    FlagOp::Add(set(&["\\Seen"])),
                    &vendor(),
                    ProtocolKind::Imap,
                )
                .await
        })
    };
    while loud.submissions.load(Ordering::SeqCst) == 0 {
        loud.submitted.notified().await;
    }
    // Let the per-item failure be classified and recorded.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let started = tokio::time::Instant::now();
    loud_engine
        .bulk_set_flags(
            &quiet_id,
            vec![ObjectId("two".into())],
            FlagOp::Add(set(&["\\Seen"])),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .expect("quiet campaign");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_secs(HINTED_RETRY_SECS - 2),
        "the campaign submitted through a budget hold its targets declare \
         (elapsed {elapsed:?})"
    );

    loud_run.await.expect("loud task").expect("loud campaign");
    loud_engine.detach(&loud_id).await.expect("detach loud");
    loud_engine.detach(&quiet_id).await.expect("detach quiet");
}

#[tokio::test]
async fn mutation_campaigns_use_engine_admission_on_the_real_path() {
    let state = Arc::new(CampaignState::default());
    state.gate_first_campaign.store(true, Ordering::SeqCst);
    let config = bifrost_sync::EngineConfig {
        budget: bifrost_sync::ConcurrencyBudget {
            per_account: 2,
            global: 2,
            mutation_share_num: 1,
            mutation_share_den: 2,
        },
        ..bifrost_sync::EngineConfig::default()
    };
    let engine = Arc::new(
        SyncEngine::builder()
            .config(config)
            .build()
            .expect("engine"),
    );
    let account_id = AccountId("mutation-admission".into());
    engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");

    let spawn_campaign = |id: &'static str| {
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move {
            engine
                .bulk_set_flags(
                    &account_id,
                    vec![ObjectId(id.into())],
                    FlagOp::Add(set(&["\\Seen"])),
                    &vendor(),
                    ProtocolKind::Imap,
                )
                .await
        })
    };
    let first = spawn_campaign("one");
    state.submitted.notified().await;
    let second = spawn_campaign("two");
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(state.submissions.load(Ordering::SeqCst), 1);

    state.release_first_campaign.notify_one();
    first.await.expect("first task").expect("first campaign");
    second.await.expect("second task").expect("second campaign");
    assert_eq!(state.submissions.load(Ordering::SeqCst), 2);
    engine.detach(&account_id).await.expect("detach");
}

/// The read-back guard hydrates over the wire, and it runs AFTER the
/// attempt loop released the campaign's admission permit. Left unadmitted
/// it is protocol work outside both the per-account and the global cap -
/// on exactly the path (an oversized campaign's unresolved tail) where the
/// fan-out is widest.
///
/// The observable is a second campaign: with one mutation permit for the
/// account, it can only submit while the first campaign holds nothing. If
/// the read-back is admitted, its submission waits for the read-back to
/// finish.
#[tokio::test]
async fn a_read_back_holds_admission_against_a_second_campaign() {
    let state = Arc::new(CampaignState::default());
    state.gate_readback.store(true, Ordering::SeqCst);
    let config = bifrost_sync::EngineConfig {
        budget: bifrost_sync::ConcurrencyBudget {
            per_account: 2,
            global: 8,
            mutation_share_num: 1,
            mutation_share_den: 2,
        },
        // One attempt only, so the campaign's single target ends in the
        // pending-retry lane and reaches the read-back guard.
        mutation_max_retries: 0,
        ..bifrost_sync::EngineConfig::default()
    };
    let engine = Arc::new(
        SyncEngine::builder()
            .config(config)
            .build()
            .expect("engine"),
    );
    let account_id = AccountId("readback-admission".into());
    engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");

    let spawn_campaign = |id: &'static str| {
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        tokio::spawn(async move {
            engine
                .bulk_set_flags(
                    &account_id,
                    vec![ObjectId(id.into())],
                    FlagOp::Add(set(&["\\Seen"])),
                    &vendor(),
                    ProtocolKind::Imap,
                )
                .await
        })
    };

    let first = spawn_campaign("one");
    state.readback_started.notified().await;
    assert_eq!(
        state.submissions.load(Ordering::SeqCst),
        1,
        "the first campaign submitted once and is now in its read-back"
    );

    let second = spawn_campaign("two");
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        state.submissions.load(Ordering::SeqCst),
        1,
        "a second campaign must not reach the wire while the in-flight \
         read-back holds the account's only mutation permit"
    );

    state.release_readback.notify_one();
    first.await.expect("first task").expect("first campaign");
    second.await.expect("second task").expect("second campaign");
    assert_eq!(state.submissions.load(Ordering::SeqCst), 2);
    engine.detach(&account_id).await.expect("detach");
}

#[tokio::test(start_paused = true)]
async fn detach_abandons_a_parked_campaign() {
    let state = Arc::new(CampaignState::default());
    let engine = Arc::new(SyncEngine::builder().build().expect("engine"));
    let account_id = AccountId("mutation-shutdown".into());
    let control = engine
        .attach(
            account_id.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(&state)))),
        )
        .await
        .expect("attach");
    let campaign_engine = Arc::clone(&engine);
    let campaign_id = account_id.clone();
    let campaign = tokio::spawn(async move {
        campaign_engine
            .bulk_set_flags(
                &campaign_id,
                vec![ObjectId("one".into())],
                FlagOp::Add(set(&["\\Seen"])),
                &vendor(),
                ProtocolKind::Imap,
            )
            .await
    });

    state.submitted.notified().await;
    control.pause().await.expect("pause");
    engine.detach(&account_id).await.expect("detach");
    let counters = campaign.await.expect("campaign task").expect("accounting");
    assert_eq!(counters.pending_retry, 1);
    assert!(matches!(
        counters.error.as_deref(),
        Some(Error::ShuttingDown)
    ));
    assert_eq!(state.submissions.load(Ordering::SeqCst), 1);
}

fn cohort_state(mode: usize) -> Arc<CampaignState> {
    let state = Arc::new(CampaignState::default());
    state.cohort_mode.store(mode, Ordering::SeqCst);
    for id in ["a", "b", "c", "d"] {
        state.target_keys.lock().expect("keys").insert(
            ObjectId(id.into()),
            vec![bifrost_types::ThrottleKey::Budget(
                bifrost_types::ThrottleBudget::new(format!("cohort|{id}")),
            )],
        );
    }
    state
}

async fn cohort_engine(state: &Arc<CampaignState>, cap: usize) -> (Arc<SyncEngine>, AccountId) {
    let mut config = bifrost_sync::EngineConfig::default();
    config.mutation.retry_queue_cap = cap;
    let engine = Arc::new(
        SyncEngine::builder()
            .config(config)
            .build()
            .expect("engine"),
    );
    let account = AccountId("cohorts".into());
    engine
        .attach(
            account.clone(),
            Arc::new(FlagsFactory(campaign_account(Arc::clone(state)))),
        )
        .await
        .expect("attach");
    (engine, account)
}

async fn cohort_run(
    engine: &SyncEngine,
    account: &AccountId,
    ids: &[&str],
) -> Result<bifrost_sync::MutationCounters, Error> {
    engine
        .bulk_set_flags(
            account,
            ids.iter().map(|id| ObjectId((*id).into())).collect(),
            FlagOp::Add(set(&["\\Seen"])),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
}

#[tokio::test(start_paused = true)]
async fn cohort_free_mailbox_finishes_while_sibling_is_held() {
    let state = cohort_state(COHORT_RETRY_ONCE);
    let (engine, account) = cohort_engine(&state, 10).await;
    // First create a mailbox-specific hold using a separate campaign.
    let held = {
        let engine = Arc::clone(&engine);
        let account = account.clone();
        tokio::spawn(async move { cohort_run(&engine, &account, &["a"]).await })
    };
    while state.calls.lock().expect("calls").is_empty() {
        tokio::task::yield_now().await;
    }
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    state.cohort_mode.store(COHORT_APPLY, Ordering::SeqCst);
    let started = tokio::time::Instant::now();
    let run = {
        let engine = Arc::clone(&engine);
        let account = account.clone();
        tokio::spawn(async move { cohort_run(&engine, &account, &["a", "b"]).await })
    };
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    {
        let calls = state.calls.lock().expect("calls");
        assert!(
            calls
                .iter()
                .any(|(ids, _, at)| ids == &[ObjectId("b".into())] && *at == started),
            "free mailbox must finish before held mailbox becomes ready: {calls:?}"
        );
        assert_eq!(calls.len(), 2, "held cohort must still be waiting");
    }
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    run.await.expect("task").expect("campaign");
    held.await.expect("held task").expect("held campaign");
    engine.detach(&account).await.expect("detach");
}

#[tokio::test(start_paused = true)]
async fn cohort_retry_hold_yields_to_a_sibling_readback() {
    let state = cohort_state(COHORT_RETRY_BESIDE_UNCERTAIN);
    let (engine, account) = cohort_engine(&state, 10).await;
    let run = {
        let engine = Arc::clone(&engine);
        let account = account.clone();
        tokio::spawn(async move { cohort_run(&engine, &account, &["a", "b"]).await })
    };
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        *state.observations.lock().expect("reads"),
        vec![vec![ObjectId("b".into())]],
        "one cohort's 429 must not postpone another cohort's ready read-back"
    );
    assert_eq!(state.calls.lock().expect("calls").len(), 2);
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    run.await.expect("task").expect("campaign");
    engine.detach(&account).await.expect("detach");
}

#[tokio::test(start_paused = true)]
async fn cohort_deduplicates_targets_into_one_submission() {
    let state = cohort_state(COHORT_APPLY);
    let (engine, account) = cohort_engine(&state, 10).await;
    let counters = cohort_run(&engine, &account, &["a", "b", "a", "b"])
        .await
        .expect("campaign");
    let submitted: Vec<_> = state
        .calls
        .lock()
        .expect("calls")
        .iter()
        .flat_map(|(ids, _, _)| ids.clone())
        .collect();
    assert_eq!(submitted, vec![ObjectId("a".into()), ObjectId("b".into())]);
    assert_eq!(counters.applied, 2);
    engine.detach(&account).await.expect("detach");
}

#[tokio::test(start_paused = true)]
async fn cohort_frozen_keys_are_vended_once_per_cohort() {
    let state = cohort_state(COHORT_RETRY_ONCE);
    let (engine, account) = cohort_engine(&state, 10).await;
    cohort_run(&engine, &account, &["a", "b"])
        .await
        .expect("campaign");
    {
        let calls = state.calls.lock().expect("calls");
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0].0, calls[2].0);
        assert_eq!(calls[1].0, calls[3].0);
        assert_eq!(key_parts(&calls[0].1), key_parts(&calls[2].1));
        assert_eq!(key_parts(&calls[1].1), key_parts(&calls[3].1));
        assert_ne!(
            key_parts(&calls[0].1),
            key_parts(&calls[1].1),
            "cohorts must each vend their own key"
        );
    }
    engine.detach(&account).await.expect("detach");
}

/// Equal singleton declarations form one cohort even when the account returns
/// the keys in a different order or repeats keys. Both objects retry together
/// under the single key allocated for their shared execution identity.
#[tokio::test(start_paused = true)]
async fn cohort_equal_declarations_share_submission_and_frozen_key() {
    let state = cohort_state(COHORT_RETRY_ONCE);
    let first = bifrost_types::ThrottleKey::Budget(bifrost_types::ThrottleBudget::new(
        "cohort|shared-first",
    ));
    let second = bifrost_types::ThrottleKey::Budget(bifrost_types::ThrottleBudget::new(
        "cohort|shared-second",
    ));
    {
        let mut keys = state.target_keys.lock().expect("keys");
        keys.insert(ObjectId("a".into()), vec![first.clone(), second.clone()]);
        keys.insert(
            ObjectId("b".into()),
            vec![second.clone(), first.clone(), second, first],
        );
    }
    let (engine, account) = cohort_engine(&state, 10).await;
    let counters = cohort_run(&engine, &account, &["a", "b"])
        .await
        .expect("campaign");
    {
        let calls = state.calls.lock().expect("calls");
        assert_eq!(
            calls.len(),
            2,
            "equal declarations require one initial submission and one retry"
        );
        let targets = vec![ObjectId("a".into()), ObjectId("b".into())];
        assert_eq!(
            calls[0].0, targets,
            "distinct targets must share the initial submission"
        );
        assert_eq!(calls[1].0, targets, "both targets retry in the same cohort");
        assert_eq!(
            key_parts(&calls[0].1),
            key_parts(&calls[1].1),
            "the shared cohort key must remain frozen"
        );
        assert_eq!(state.keys.lock().expect("keys").len(), 2);
    }
    assert_eq!(counters.applied, 2);
    assert_eq!(counters.pending_retry, 0);
    engine.detach(&account).await.expect("detach");
}

#[tokio::test(start_paused = true)]
async fn cohort_retry_allowance_is_shared_campaign_wide() {
    let state = cohort_state(COHORT_RETRY_ONCE);
    let (engine, account) = cohort_engine(&state, 1).await;
    let counters = cohort_run(&engine, &account, &["a", "b", "c"])
        .await
        .expect("campaign");
    {
        let calls = state.calls.lock().expect("calls");
        assert_eq!(
            calls.len(),
            4,
            "three singleton initial submissions and ONE retry total"
        );
        assert!(calls.iter().all(|(ids, _, _)| ids.len() == 1));
    }
    assert_eq!(counters.applied, 1);
    assert_eq!(counters.failed_terminal, 0);
    assert_eq!(counters.pending_retry, 2);
    engine.detach(&account).await.expect("detach");
}

#[tokio::test(start_paused = true)]
async fn cohort_error_observes_submitted_siblings_and_excludes_unsent() {
    let state = cohort_state(COHORT_ERROR_IN_SECOND);
    let (engine, account) = cohort_engine(&state, 10).await;
    let counters = cohort_run(&engine, &account, &["a", "b", "c"])
        .await
        .expect("campaign accounting");
    assert!(matches!(counters.error.as_deref(), Some(Error::Account(_))));
    let mut reads = state.observations.lock().expect("reads").clone();
    reads.sort_by_key(|ids| ids[0].0.clone());
    assert_eq!(
        reads,
        vec![vec![ObjectId("a".into())], vec![ObjectId("b".into())]],
        "both submitted cohorts get one observation; known-unsent c must not be hydrated"
    );
    assert_eq!(
        state.calls.lock().expect("calls").len(),
        2,
        "c stays known unsent"
    );
    engine.detach(&account).await.expect("detach");
}

#[tokio::test(start_paused = true)]
async fn cohort_counters_derive_from_per_object_observations() {
    let state = cohort_state(COHORT_UNCERTAIN);
    state.duplicate_observations.store(true, Ordering::SeqCst);
    let (engine, account) = cohort_engine(&state, 10).await;
    let counters = cohort_run(&engine, &account, &["a", "b", "c"])
        .await
        .expect("campaign");
    assert_eq!(
        counters.pending_retry, 3,
        "duplicate observations cannot inflate accounting: {counters:?}"
    );
    assert_eq!(
        counters.failed_terminal
            + counters.applied
            + counters.skipped
            + counters.blocked_by_engine
            + counters.downgraded,
        0
    );
    engine.detach(&account).await.expect("detach");
}

fn key_parts(key: &IdempotencyKey) -> (&bifrost_types::RunId, u64, &bifrost_types::ProtocolSalt) {
    (&key.run_id, key.sequence, &key.protocol_salt)
}

struct JudgingAccount {
    inner: FlagsAccount,
}
impl JudgingAccount {
    fn hydrate(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        self.inner.get_stream(ids, projection)
    }
    fn judge(
        &self,
        _target: &ObjectId,
        _observed: &HashSet<String>,
        _op: &FlagOp,
    ) -> bifrost_types::FlagTargetState {
        bifrost_types::FlagTargetState::Satisfied
    }
}
impl Account for JudgingAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.inner.caps
    }
    fn set_priority(&self, _priority: Priority) {
        self.inner.set_priority(_priority);
    }
    fn set_bandwidth_cap(&self, _bps: Option<u64>) {
        self.inner.set_bandwidth_cap(_bps);
    }
    fn throttle_keys(
        &self,
        _account: &AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        self.inner.throttle_keys(_account, work)
    }
    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        self.inner.describe_cursor(_cursor)
    }
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        self.inner.discover_cursor_scopes()
    }
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        self.inner.discover_memberships()
    }
    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        self.inner.scope_lifecycle_stream()
    }
    fn establish_initial_cursor(
        &self,
        _scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        self.inner.establish_initial_cursor(_scope)
    }
    fn inventory_stream(&self, _scope: CursorScope) -> AccountStream<InventoryEvent> {
        self.inner.inventory_stream(_scope)
    }
    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        self.hydrate(ids, projection)
    }
    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        self.inner.changes_stream(_cursor)
    }
    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        self.inner.push_subscribe(_scopes)
    }
    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.push_unsubscribe(_handle)
    }
    fn push_stream(&self) -> AccountStream<WatchEvent> {
        self.inner.push_stream()
    }
    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_blob(_handle)
    }
    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_blob_range(_handle, _range)
    }
    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_raw_rfc822(_message)
    }
    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        _op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.inner.bulk_set_flags(targets, _op, key)
    }
    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.inner.bulk_move(_targets, destination, _key)
    }
    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.inner.bulk_destroy(_targets, _key)
    }
    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        self.inner.close()
    }
    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.add_to_container(_target, _container)
    }
    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.remove_from_container(_target, _container)
    }
    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_keyword(_target, _keyword, _value)
    }
    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_label_membership(_target, _label, _value)
    }
    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_category(_target, _category, _value)
    }
    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner
            .set_extended_property(_target, _property_id, _value)
    }
    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_is_read(_target, _is_read)
    }
    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_importance(_target, _level)
    }
    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        self.inner.send_message(_request)
    }
    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        self.inner.attachment_upload(_bytes, _mime)
    }
    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        self.inner.host_attachment(_bytes, _meta)
    }
    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        self.inner.draft_create(_patch)
    }
    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.draft_update(_draft, _patch)
    }
    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        self.inner.draft_discard(_draft)
    }
    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        self.inner.draft_send(_draft)
    }
    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.cancel_scheduled_send(_handle)
    }
    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        self.inner.reschedule_send(_handle, _scheduled)
    }
    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        self.inner.search(_request)
    }
    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        self.inner.search_messages(_request)
    }
    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
        self.inner.containers_list()
    }
    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        self.inner.container_create(_kind, _name, _parent, _style)
    }
    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_rename(_container, _name, _style)
    }
    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_move(_container, _new_parent)
    }
    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_delete(_container)
    }
    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        self.inner.identities_list()
    }
    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.identity_update(_identity, _patch)
    }
    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        self.inner.vacation_get()
    }
    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        self.inner.vacation_set(_config)
    }
    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        self.inner.quota_get()
    }
    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        self.inner.filters_list()
    }
    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        self.inner.filter_create(_filter)
    }
    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.filter_update(_filter, _patch)
    }
    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.filter_delete(_filter)
    }
    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        self.inner.filter_validate(_filter)
    }
    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        self.inner.address_books_list()
    }
    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        self.inner.contacts_list(_address_book, _page_cursor)
    }
    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        self.inner.contact_get(_contact)
    }
    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        self.inner.contact_create(_contact)
    }
    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.contact_update(_contact, _patch)
    }
    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.contact_delete(_contact)
    }
    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        self.inner.contact_search(_request)
    }
    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryCard>, AccountError>> {
        self.inner.directory_search(_query, _limit, _page_cursor)
    }
    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroup>, AccountError>> {
        self.inner.directory_groups_list(_page_cursor)
    }
    fn directory_group_expand(
        &self,
        _group: bifrost_types::DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroupMember>, AccountError>> {
        self.inner.directory_group_expand(_group, _page_cursor)
    }
    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        self.inner.calendars_list()
    }
    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        self.inner.events_in_range(_range)
    }
    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        self.inner.event_get(_event)
    }
    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        self.inner.event_create(_event)
    }
    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_update(_event, _patch)
    }
    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_delete(_event)
    }
    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_rsvp(_event, _status)
    }
    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        self.inner.event_search(_request)
    }
    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        self.inner.thread_hydrate(_thread)
    }
    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        self.inner.message_hydrate(_message, _projection)
    }
    fn flag_target(
        &self,
        target: &ObjectId,
        observed: &HashSet<String>,
        op: &FlagOp,
    ) -> bifrost_types::FlagTargetState {
        self.judge(target, observed, op)
    }
}
#[tokio::test]
async fn readback_guard_judges_through_the_account() {
    let account = JudgingAccount {
        inner: FlagsAccount {
            caps: caps(),
            flag_table: Default::default(),
            campaign: None,
        },
    };
    let result = run_readback_guard(
        &account,
        vec![ObjectId("a".into())],
        &FlagOp::Add(set(&["\\Seen"])),
    )
    .await
    .unwrap();
    assert_eq!(result.skipped, 1);
    assert_eq!(result.still_failed, 0);
}
#[tokio::test]
async fn readback_guard_folds_flag_case() {
    let account = FlagsAccount {
        caps: caps(),
        flag_table: [(ObjectId("a".into()), set(&["\\seen"]))]
            .into_iter()
            .collect(),
        campaign: None,
    };
    let result = run_readback_guard(
        &account,
        vec![ObjectId("a".into())],
        &FlagOp::Add(set(&["\\Seen"])),
    )
    .await
    .unwrap();
    assert_eq!(result.skipped, 1);
}

#[derive(Clone)]
enum ItemScript {
    Applied,
    Uncertain(AccountError),
    Failed(AccountError),
    Omit,
}
#[derive(Clone)]
enum Observation {
    Flags(HashSet<String>),
    Membership(Vec<MembershipScope>),
    NotFound,
    Fail(AccountError),
}
#[derive(Default)]
struct RoundScript {
    items: std::collections::HashMap<ObjectId, ItemScript>,
    termination: Option<AccountError>,
    then: Option<(ObjectId, Observation)>,
    park: Option<Arc<tokio::sync::Notify>>,
}
struct DoorCall {
    targets: Vec<bifrost_types::MailMutationTarget>,
    key: IdempotencyKey,
    versioned: bool,
}
#[derive(Default)]
struct ReassertState {
    rounds: std::sync::Mutex<std::collections::VecDeque<RoundScript>>,
    observations: std::sync::Mutex<std::collections::HashMap<ObjectId, Observation>>,
    readback_termination: std::sync::Mutex<Option<AccountError>>,
    keys: std::sync::Mutex<std::collections::HashMap<ObjectId, bifrost_types::ThrottleKey>>,
    doors: std::sync::Mutex<Vec<DoorCall>>,
    reads: std::sync::Mutex<Vec<Vec<ObjectId>>>,
    door_entered: tokio::sync::Notify,
    control: std::sync::Mutex<Option<bifrost_sync::SyncControl>>,
    /// Every `establish_initial_cursor` scope. Discovery is empty, so attach
    /// establishes nothing; an entry is a `RestartScope` repair the reopen
    /// listener ran for a forwarded recovery request, and since establishment
    /// answers `Unsupported` (terminal), each request makes exactly one call.
    establishes: std::sync::Mutex<Vec<CursorScope>>,
}
struct ReassertAccount {
    inner: FlagsAccount,
    state: Arc<ReassertState>,
}
impl ReassertAccount {
    fn new(state: Arc<ReassertState>, declared: bool) -> Self {
        let mut capabilities = caps();
        capabilities.mutation.reassertion = bifrost_types::MutationReassertion {
            set_flags: declared,
            destroy: declared,
        };
        Self {
            inner: FlagsAccount {
                caps: capabilities,
                flag_table: Default::default(),
                campaign: None,
            },
            state,
        }
    }
    fn hydrate(
        &self,
        ids: AccountStream<ObjectId>,
        _projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        let state = Arc::clone(&self.state);
        Box::pin(
            stream::once(async move {
                let ids = ids.collect::<Vec<_>>().await;
                state.reads.lock().unwrap().push(ids.clone());
                let observations = state.observations.lock().unwrap();
                let items = ids
                    .into_iter()
                    .filter_map(|id| {
                        let item = bifrost_types::BatchItemId(id.0.clone());
                        let kind = match observations.get(&id)? {
                            Observation::Flags(flags) => {
                                HydratedObjectKind::FlagsOnly(flags.clone())
                            }
                            Observation::Membership(memberships) => {
                                HydratedObjectKind::Metadata(bifrost_types::InventoryEntry {
                                    id: id.clone(),
                                    memberships: memberships.clone(),
                                    size: None,
                                    blob_id: None,
                                    fingerprint: bifrost_types::Fingerprint {
                                        server_version: bifrost_types::ServerVersion::Unavailable,
                                        size: None,
                                        flags_hash: bifrost_types::canonical_flags_hash(
                                            std::iter::empty::<&str>(),
                                        ),
                                    },
                                    thread_id: None,
                                    message_id: None,
                                    references: Vec::new(),
                                    in_reply_to: None,
                                })
                            }
                            Observation::NotFound => {
                                return Some(ItemOutcome::Failed(
                                    bifrost_types::BatchFailure::new(item, not_found_error()),
                                ));
                            }
                            Observation::Fail(error) => {
                                return Some(ItemOutcome::Failed(
                                    bifrost_types::BatchFailure::new(item, error.clone()),
                                ));
                            }
                        };
                        Some(ItemOutcome::Succeeded(bifrost_types::BatchSuccess::new(
                            item,
                            HydratedObject {
                                id,
                                kind,
                                blobs: Vec::new(),
                            },
                        )))
                    })
                    .collect();
                let batch = SyncEvent::Batch(Batch {
                    items,
                    page_boundary: PageBoundary::Page,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: None,
                });
                let end = state
                    .readback_termination
                    .lock()
                    .unwrap()
                    .take()
                    .map_or(SyncEvent::Done(None), SyncEvent::Terminated);
                stream::iter([batch, end])
            })
            .flatten(),
        )
    }
    fn submit(
        &self,
        ids: AccountStream<ObjectId>,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.submit_targets(
            Box::pin(ids.map(bifrost_types::MailMutationTarget::from)),
            key,
            false,
        )
    }
    fn submit_targets(
        &self,
        targets: AccountStream<bifrost_types::MailMutationTarget>,
        key: IdempotencyKey,
        versioned: bool,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        let state = Arc::clone(&self.state);
        Box::pin(
            stream::once(async move {
                let targets = targets.collect::<Vec<_>>().await;
                {
                    let mut log = state.doors.lock().unwrap();
                    assert!(log.len() < 8, "campaign exceeded the bounded test script");
                    log.push(DoorCall {
                        targets: targets.clone(),
                        key,
                        versioned,
                    });
                }
                state.door_entered.notify_one();
                let script = state.rounds.lock().unwrap().pop_front().unwrap_or_else(|| {
                    round(
                        &targets.iter().map(|t| t.id.0.as_str()).collect::<Vec<_>>(),
                        ItemScript::Uncertain(retryable_transport()),
                    )
                });
                if let Some(park) = script.park {
                    park.notified().await;
                }
                if let Some((id, observation)) = script.then {
                    state.observations.lock().unwrap().insert(id, observation);
                }
                let items = targets
                    .into_iter()
                    .filter_map(|target| {
                        let item = bifrost_types::BatchItemId(target.id.0.clone());
                        match script.items.get(&target.id).unwrap_or(&ItemScript::Omit) {
                            ItemScript::Applied => Some(ItemOutcome::Succeeded(
                                bifrost_types::BatchSuccess::new(item, MutationSuccess::Applied),
                            )),
                            ItemScript::Uncertain(error) => Some(ItemOutcome::Uncertain(
                                bifrost_types::BatchUncertain::new(item, error.clone()),
                            )),
                            ItemScript::Failed(error) => Some(ItemOutcome::Failed(
                                bifrost_types::BatchFailure::new(item, error.clone()),
                            )),
                            ItemScript::Omit => None,
                        }
                    })
                    .collect();
                stream::iter([
                    SyncEvent::Batch(Batch {
                        items,
                        page_boundary: PageBoundary::Page,
                        server_latency: std::time::Duration::ZERO,
                        bytes_in: 0,
                        checkpoint: None,
                    }),
                    script
                        .termination
                        .map_or(SyncEvent::Done(None), SyncEvent::Terminated),
                ])
            })
            .flatten(),
        )
    }
}
struct ReassertFactory(std::sync::Mutex<std::collections::VecDeque<Arc<ReassertAccount>>>);
impl AccountFactory for ReassertFactory {
    fn open(&self, _account: AccountId) -> AccountFuture<Result<OpenedAccount, AccountError>> {
        let mut accounts = self.0.lock().unwrap();
        let account = if accounts.len() > 1 {
            accounts.pop_front().unwrap()
        } else {
            Arc::clone(accounts.front().unwrap())
        };
        Box::pin(async move { Ok(OpenedAccount::complete(account)) })
    }
}
fn round(ids: &[&str], item: ItemScript) -> RoundScript {
    RoundScript {
        items: ids
            .iter()
            .map(|id| (ObjectId((*id).into()), item.clone()))
            .collect(),
        ..Default::default()
    }
}
fn reassert_state(ids: &[&str], rounds: Vec<RoundScript>) -> Arc<ReassertState> {
    Arc::new(ReassertState {
        rounds: std::sync::Mutex::new(rounds.into()),
        observations: std::sync::Mutex::new(
            ids.iter()
                .map(|id| (ObjectId((*id).into()), Observation::Flags(set(&[]))))
                .collect(),
        ),
        ..Default::default()
    })
}
async fn reassert_engine(
    state: &Arc<ReassertState>,
    retries: u32,
    cap: usize,
    declared: bool,
    replacement: Option<bool>,
) -> (Arc<SyncEngine>, AccountId) {
    let mut config = bifrost_sync::EngineConfig {
        mutation_max_retries: retries,
        ..Default::default()
    };
    config.mutation.retry_queue_cap = cap;
    let engine = Arc::new(SyncEngine::builder().config(config).build().unwrap());
    let id = AccountId("reassert".into());
    let mut accounts = vec![Arc::new(ReassertAccount::new(Arc::clone(state), declared))];
    if let Some(declaration) = replacement {
        accounts.push(Arc::new(ReassertAccount::new(
            Arc::clone(state),
            declaration,
        )));
    }
    let control = engine
        .attach(
            id.clone(),
            Arc::new(ReassertFactory(std::sync::Mutex::new(accounts.into()))),
        )
        .await
        .unwrap();
    *state.control.lock().unwrap() = Some(control);
    (engine, id)
}
fn conflict_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::ConcurrencyConflict,
        Cause::State(bifrost_types::StateCause::ConcurrencyConflict),
    )
    .try_build()
    .unwrap()
}
fn dedupe_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::PartialResponse),
        Cause::Wire(bifrost_types::WireCause::MalformedResponse {
            protocol: bifrost_types::Protocol::Graph,
            detail: None,
        }),
    )
    .operation(bifrost_types::AccountOperation::BulkDestroy)
    .try_build()
    .unwrap()
}
fn engine_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::CursorInvalid),
        Cause::State(bifrost_types::StateCause::CursorInvalid),
    )
    .scope(bifrost_types::ErrorScope::Cursor(CursorScope::Account))
    .try_build()
    .unwrap()
}
fn operator_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::OperatorOverrideNeeded),
        Cause::State(bifrost_types::StateCause::OperatorOverrideNeeded {
            reason: "test readback directive".into(),
        }),
    )
    .try_build()
    .unwrap()
}
fn not_found_error() -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message),
        Cause::Request(RequestCause::NotFound {
            what: bifrost_types::ResourceKind::Message,
            id: None,
        }),
    )
    .try_build()
    .unwrap()
}
fn held_error(budget: Option<&str>, seconds: u64) -> AccountError {
    let mut builder = AccountErrorBuilder::new(
        AccountErrorKind::Server(bifrost_types::ServerErrorKind::RateLimited),
        Cause::Server(bifrost_types::ServerCause::RateLimited {
            retry_hint: Some(bifrost_types::RetryHint::After(
                std::time::Duration::from_secs(seconds),
            )),
        }),
    )
    .operation(bifrost_types::AccountOperation::UpdateFlags);
    if let Some(budget) = budget {
        builder = builder.throttle_budget(bifrost_types::ThrottleBudget::new(budget));
    } else {
        builder = builder.throttle_scope(bifrost_types::ThrottleScope::CurrentOperation);
    }
    builder.try_build().unwrap()
}
fn call_ids(call: &DoorCall) -> Vec<&str> {
    call.targets.iter().map(|t| t.id.0.as_str()).collect()
}

impl Account for ReassertAccount {
    fn capabilities(&self) -> &AccountCapabilities {
        &self.inner.caps
    }
    fn set_priority(&self, _priority: Priority) {
        self.inner.set_priority(_priority);
    }
    fn set_bandwidth_cap(&self, _bps: Option<u64>) {
        self.inner.set_bandwidth_cap(_bps);
    }
    fn throttle_keys(
        &self,
        _account: &AccountId,
        work: bifrost_types::ThrottleWork<'_>,
    ) -> Vec<bifrost_types::ThrottleKey> {
        match work {
            bifrost_types::ThrottleWork::Objects(ids) => {
                let keys = self.state.keys.lock().unwrap();
                ids.iter().filter_map(|id| keys.get(id).cloned()).collect()
            }
            _ => Vec::new(),
        }
    }
    fn describe_cursor(&self, _cursor: &ChangeCursor) -> CursorDescriptor {
        self.inner.describe_cursor(_cursor)
    }
    fn discover_cursor_scopes(&self) -> AccountStream<SyncEvent<CursorScope>> {
        self.inner.discover_cursor_scopes()
    }
    fn discover_memberships(&self) -> AccountStream<SyncEvent<MembershipScope>> {
        self.inner.discover_memberships()
    }
    fn scope_lifecycle_stream(&self) -> AccountStream<ScopeLifecycleEvent> {
        self.inner.scope_lifecycle_stream()
    }
    fn establish_initial_cursor(
        &self,
        scope: CursorScope,
    ) -> AccountFuture<Result<CursorEstablishment, AccountError>> {
        self.state.establishes.lock().unwrap().push(scope.clone());
        self.inner.establish_initial_cursor(scope)
    }
    fn inventory_stream(&self, _scope: CursorScope) -> AccountStream<InventoryEvent> {
        self.inner.inventory_stream(_scope)
    }
    fn get_stream(
        &self,
        ids: AccountStream<ObjectId>,
        projection: Projection,
    ) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
        self.hydrate(ids, projection)
    }
    fn changes_stream(&self, _cursor: ChangeCursor) -> AccountStream<SyncEvent<Change>> {
        self.inner.changes_stream(_cursor)
    }
    fn push_subscribe(
        &self,
        _scopes: &[CursorScope],
    ) -> AccountFuture<Result<bifrost_types::PushSubscription, AccountError>> {
        self.inner.push_subscribe(_scopes)
    }
    fn push_unsubscribe(
        &self,
        _handle: SubscriptionHandle,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.push_unsubscribe(_handle)
    }
    fn push_stream(&self) -> AccountStream<WatchEvent> {
        self.inner.push_stream()
    }
    fn open_blob(&self, _handle: BlobHandle) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_blob(_handle)
    }
    fn open_blob_range(
        &self,
        _handle: BlobHandle,
        _range: ByteRange,
    ) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_blob_range(_handle, _range)
    }
    fn open_raw_rfc822(&self, _message: ObjectId) -> AccountStream<SyncEvent<Bytes>> {
        self.inner.open_raw_rfc822(_message)
    }
    fn bulk_set_flags(
        &self,
        targets: AccountStream<ObjectId>,
        _op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.submit(targets, key)
    }
    fn bulk_move(
        &self,
        _targets: AccountStream<ObjectId>,
        _destination: MembershipScope,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.submit(_targets, _key)
    }
    fn bulk_destroy(
        &self,
        _targets: AccountStream<ObjectId>,
        _key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.submit(_targets, _key)
    }
    fn close(&self) -> AccountFuture<Result<(), AccountError>> {
        self.inner.close()
    }
    fn add_to_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.add_to_container(_target, _container)
    }
    fn remove_from_container(
        &self,
        _target: MutationTarget,
        _container: ContainerId,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.remove_from_container(_target, _container)
    }
    fn set_keyword(
        &self,
        _target: MutationTarget,
        _keyword: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_keyword(_target, _keyword, _value)
    }
    fn set_label_membership(
        &self,
        _target: MutationTarget,
        _label: ContainerId,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_label_membership(_target, _label, _value)
    }
    fn set_category(
        &self,
        _target: MutationTarget,
        _category: String,
        _value: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_category(_target, _category, _value)
    }
    fn set_extended_property(
        &self,
        _target: MutationTarget,
        _property_id: String,
        _value: Option<String>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner
            .set_extended_property(_target, _property_id, _value)
    }
    fn set_is_read(
        &self,
        _target: MutationTarget,
        _is_read: bool,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_is_read(_target, _is_read)
    }
    fn set_importance(
        &self,
        _target: MutationTarget,
        _level: Importance,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.set_importance(_target, _level)
    }
    fn send_message(&self, _request: SendRequest) -> AccountFuture<Result<ObjectId, AccountError>> {
        self.inner.send_message(_request)
    }
    fn attachment_upload(
        &self,
        _bytes: AccountStream<Result<Bytes, AccountError>>,
        _mime: String,
    ) -> AccountFuture<Result<AttachmentHandle, AccountError>> {
        self.inner.attachment_upload(_bytes, _mime)
    }
    fn host_attachment(
        &self,
        _bytes: Bytes,
        _meta: CloudUploadMeta,
    ) -> AccountFuture<Result<HostedAttachment, AccountError>> {
        self.inner.host_attachment(_bytes, _meta)
    }
    fn draft_create(&self, _patch: DraftPatch) -> AccountFuture<Result<DraftHandle, AccountError>> {
        self.inner.draft_create(_patch)
    }
    fn draft_update(
        &self,
        _draft: DraftHandle,
        _patch: DraftPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.draft_update(_draft, _patch)
    }
    fn draft_discard(&self, _draft: DraftHandle) -> AccountFuture<Result<(), AccountError>> {
        self.inner.draft_discard(_draft)
    }
    fn draft_send(&self, _draft: DraftHandle) -> AccountFuture<Result<ObjectId, AccountError>> {
        self.inner.draft_send(_draft)
    }
    fn cancel_scheduled_send(&self, _handle: ObjectId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.cancel_scheduled_send(_handle)
    }
    fn reschedule_send(
        &self,
        _handle: ObjectId,
        _scheduled: std::time::SystemTime,
    ) -> AccountFuture<Result<ObjectId, AccountError>> {
        self.inner.reschedule_send(_handle, _scheduled)
    }
    fn search(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ThreadId>, AccountError>> {
        self.inner.search(_request)
    }
    fn search_messages(
        &self,
        _request: SearchRequest,
    ) -> AccountFuture<Result<Page<ObjectId>, AccountError>> {
        self.inner.search_messages(_request)
    }
    fn containers_list(&self) -> AccountFuture<Result<bifrost_types::ContainerList, AccountError>> {
        self.inner.containers_list()
    }
    fn container_create(
        &self,
        _kind: ContainerKind,
        _name: String,
        _parent: Option<ContainerId>,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<ContainerId, AccountError>> {
        self.inner.container_create(_kind, _name, _parent, _style)
    }
    fn container_rename(
        &self,
        _container: ContainerId,
        _name: String,
        _style: Option<bifrost_types::ContainerStyle>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_rename(_container, _name, _style)
    }
    fn container_move(
        &self,
        _container: ContainerId,
        _new_parent: Option<ContainerId>,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_move(_container, _new_parent)
    }
    fn container_delete(&self, _container: ContainerId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.container_delete(_container)
    }
    fn identities_list(&self) -> AccountFuture<Result<Vec<Identity>, AccountError>> {
        self.inner.identities_list()
    }
    fn identity_update(
        &self,
        _identity: IdentityId,
        _patch: IdentityPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.identity_update(_identity, _patch)
    }
    fn vacation_get(&self) -> AccountFuture<Result<Option<VacationConfig>, AccountError>> {
        self.inner.vacation_get()
    }
    fn vacation_set(&self, _config: VacationConfig) -> AccountFuture<Result<(), AccountError>> {
        self.inner.vacation_set(_config)
    }
    fn quota_get(&self) -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
        self.inner.quota_get()
    }
    fn filters_list(&self) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
        self.inner.filters_list()
    }
    fn filter_create(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<ServerFilterId, AccountError>> {
        self.inner.filter_create(_filter)
    }
    fn filter_update(
        &self,
        _filter: ServerFilterId,
        _patch: ServerFilterPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.filter_update(_filter, _patch)
    }
    fn filter_delete(&self, _filter: ServerFilterId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.filter_delete(_filter)
    }
    fn filter_validate(
        &self,
        _filter: ServerFilterCreate,
    ) -> AccountFuture<Result<FilterValidation, AccountError>> {
        self.inner.filter_validate(_filter)
    }
    fn address_books_list(&self) -> AccountFuture<Result<Vec<AddressBook>, AccountError>> {
        self.inner.address_books_list()
    }
    fn contacts_list(
        &self,
        _address_book: Option<AddressBookId>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        self.inner.contacts_list(_address_book, _page_cursor)
    }
    fn contact_get(&self, _contact: ContactId) -> AccountFuture<Result<ContactCard, AccountError>> {
        self.inner.contact_get(_contact)
    }
    fn contact_create(
        &self,
        _contact: ContactCreate,
    ) -> AccountFuture<Result<ContactId, AccountError>> {
        self.inner.contact_create(_contact)
    }
    fn contact_update(
        &self,
        _contact: ContactId,
        _patch: ContactPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.contact_update(_contact, _patch)
    }
    fn contact_delete(&self, _contact: ContactId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.contact_delete(_contact)
    }
    fn contact_search(
        &self,
        _request: ContactSearchRequest,
    ) -> AccountFuture<Result<Page<ContactCard>, AccountError>> {
        self.inner.contact_search(_request)
    }
    fn directory_search(
        &self,
        _query: String,
        _limit: Option<u32>,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryCard>, AccountError>> {
        self.inner.directory_search(_query, _limit, _page_cursor)
    }
    fn directory_groups_list(
        &self,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroup>, AccountError>> {
        self.inner.directory_groups_list(_page_cursor)
    }
    fn directory_group_expand(
        &self,
        _group: bifrost_types::DirectoryGroupId,
        _page_cursor: Option<Vec<u8>>,
    ) -> AccountFuture<Result<Page<bifrost_types::DirectoryGroupMember>, AccountError>> {
        self.inner.directory_group_expand(_group, _page_cursor)
    }
    fn calendars_list(&self) -> AccountFuture<Result<Vec<Calendar>, AccountError>> {
        self.inner.calendars_list()
    }
    fn events_in_range(
        &self,
        _range: EventRange,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        self.inner.events_in_range(_range)
    }
    fn event_get(&self, _event: EventId) -> AccountFuture<Result<CalendarEvent, AccountError>> {
        self.inner.event_get(_event)
    }
    fn event_create(&self, _event: EventCreate) -> AccountFuture<Result<EventId, AccountError>> {
        self.inner.event_create(_event)
    }
    fn event_update(
        &self,
        _event: EventId,
        _patch: EventPatch,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_update(_event, _patch)
    }
    fn event_delete(&self, _event: EventId) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_delete(_event)
    }
    fn event_rsvp(
        &self,
        _event: EventId,
        _status: RsvpStatus,
    ) -> AccountFuture<Result<(), AccountError>> {
        self.inner.event_rsvp(_event, _status)
    }
    fn event_search(
        &self,
        _request: EventSearchRequest,
    ) -> AccountFuture<Result<Page<CalendarEvent>, AccountError>> {
        self.inner.event_search(_request)
    }
    fn thread_hydrate(
        &self,
        _thread: ThreadId,
    ) -> AccountFuture<Result<ThreadHydration, AccountError>> {
        self.inner.thread_hydrate(_thread)
    }
    fn message_hydrate(
        &self,
        _message: ObjectId,
        _projection: HydrationProjection,
    ) -> AccountFuture<Result<Message, AccountError>> {
        self.inner.message_hydrate(_message, _projection)
    }
    fn bulk_set_flags_if_version(
        self: Arc<Self>,
        targets: AccountStream<bifrost_types::MailMutationTarget>,
        _op: FlagOp,
        key: IdempotencyKey,
    ) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
        self.submit_targets(targets, key, true)
    }
}

#[tokio::test(start_paused = true)]
async fn an_eligible_mismatch_is_resubmitted_under_its_frozen_key() {
    let state = reassert_state(
        &["a"],
        vec![
            round(&["a"], ItemScript::Uncertain(retryable_transport())),
            round(&["a"], ItemScript::Applied),
        ],
    );
    let (engine, id) = reassert_engine(&state, 2, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    let doors = state.doors.lock().unwrap();
    assert_eq!(doors.len(), 2);
    assert_eq!(key_parts(&doors[0].key), key_parts(&doors[1].key));
    assert_eq!(counters.applied, 1);
    assert_eq!(state.reads.lock().unwrap().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn a_case_folded_observation_is_satisfied_not_reasserted() {
    let state = reassert_state(
        &["a"],
        vec![round(&["a"], ItemScript::Uncertain(retryable_transport()))],
    );
    state
        .observations
        .lock()
        .unwrap()
        .insert(ObjectId("a".into()), Observation::Flags(set(&["\\seen"])));
    let (engine, id) = reassert_engine(&state, 2, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(counters.skipped, 1);
    assert_eq!(state.doors.lock().unwrap().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn reassertion_shares_the_cohort_attempt_budget() {
    let state = reassert_state(
        &["a"],
        vec![
            round(&["a"], ItemScript::Failed(retryable_transport())),
            round(&["a"], ItemScript::Uncertain(retryable_transport())),
        ],
    );
    let (engine, id) = reassert_engine(&state, 1, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(counters.pending_retry, 1);
    assert_eq!(state.doors.lock().unwrap().len(), 2);
}
#[tokio::test(start_paused = true)]
async fn reassertion_with_no_budget_stays_pending() {
    let state = reassert_state(
        &["a"],
        vec![round(&["a"], ItemScript::Uncertain(retryable_transport()))],
    );
    let (engine, id) = reassert_engine(&state, 0, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(counters.pending_retry, 1);
    assert_eq!(state.doors.lock().unwrap().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn reassertion_shares_the_retry_queue_cap() {
    let state = reassert_state(
        &["a", "b"],
        vec![
            round(&["a", "b"], ItemScript::Uncertain(retryable_transport())),
            round(&["a", "b"], ItemScript::Applied),
        ],
    );
    let (engine, id) = reassert_engine(&state, 2, 1, true, None).await;
    let counters = cohort_run(&engine, &id, &["a", "b"]).await.unwrap();
    let doors = state.doors.lock().unwrap();
    assert_eq!(doors.len(), 2);
    assert_eq!(call_ids(&doors[1]), vec!["a"]);
    assert_eq!(counters.applied, 1);
    assert_eq!(counters.pending_retry, 1);
}
#[tokio::test(start_paused = true)]
async fn an_undeclared_account_never_reasserts() {
    let state = reassert_state(
        &["a"],
        vec![round(&["a"], ItemScript::Uncertain(retryable_transport()))],
    );
    let (engine, id) = reassert_engine(&state, 2, 10, false, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(counters.pending_retry, 1);
    assert_eq!(state.doors.lock().unwrap().len(), 1);
}
fn guarded_targets() -> Vec<bifrost_types::MailMutationTarget> {
    vec![
        bifrost_types::MailMutationTarget {
            id: ObjectId("g".into()),
            expected_version: Some(bifrost_types::ServerVersion::ModSeq(7)),
        },
        ObjectId("u".into()).into(),
    ]
}
async fn guarded_run(engine: &SyncEngine, id: &AccountId) -> bifrost_sync::MutationCounters {
    engine
        .bulk_set_flags_if_version(
            id,
            guarded_targets(),
            FlagOp::Add(set(&["\\Seen"])),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .unwrap()
}
#[tokio::test(start_paused = true)]
async fn a_guarded_target_is_never_observed_or_reasserted() {
    let state = reassert_state(
        &["g", "u"],
        vec![
            round(&["g", "u"], ItemScript::Uncertain(retryable_transport())),
            round(&["u"], ItemScript::Applied),
        ],
    );
    let (engine, id) = reassert_engine(&state, 2, 10, true, None).await;
    let counters = guarded_run(&engine, &id).await;
    let doors = state.doors.lock().unwrap();
    assert_eq!(doors.len(), 2);
    assert!(doors[0].versioned);
    assert_eq!(call_ids(&doors[0]), vec!["g", "u"]);
    assert!(doors[0].targets[0].expected_version.is_some());
    assert!(doors[0].targets[1].expected_version.is_none());
    assert!(!doors[1].versioned);
    assert_eq!(call_ids(&doors[1]), vec!["u"]);
    assert_eq!(
        *state.reads.lock().unwrap(),
        vec![vec![ObjectId("u".into())]]
    );
    assert_eq!(counters.applied, 1);
    assert_eq!(counters.pending_retry, 1);
}
#[tokio::test(start_paused = true)]
async fn dedupe_guidance_is_observed_but_never_reasserted() {
    let mut third = round(&["c2"], ItemScript::Uncertain(retryable_transport()));
    third
        .items
        .insert(ObjectId("c3".into()), ItemScript::Failed(conflict_error()));
    third.termination = Some(dedupe_error());
    let state = reassert_state(
        &["a", "b", "c1", "c2", "c3"],
        vec![
            round(&["a"], ItemScript::Failed(dedupe_error())),
            round(&["b"], ItemScript::Uncertain(dedupe_error())),
            third,
        ],
    );
    for (target, budget) in [
        ("a", "A"),
        ("b", "B"),
        ("c1", "C"),
        ("c2", "C"),
        ("c3", "C"),
    ] {
        state.keys.lock().unwrap().insert(
            ObjectId(target.into()),
            bifrost_types::ThrottleKey::Budget(bifrost_types::ThrottleBudget::new(budget)),
        );
    }
    let (engine, id) = reassert_engine(&state, 5, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a", "b", "c1", "c2", "c3"])
        .await
        .unwrap();
    assert_eq!(state.doors.lock().unwrap().len(), 3);
    assert_eq!(counters.pending_retry, 5);
    let reads = state.reads.lock().unwrap();
    for target in ["a", "b", "c1", "c2", "c3"] {
        assert_eq!(
            reads.iter().flatten().filter(|id| id.0 == target).count(),
            1
        );
    }
}
#[tokio::test(start_paused = true)]
async fn an_uncertain_engine_or_terminal_outcome_is_never_reasserted() {
    let state = reassert_state(
        &["a", "b"],
        vec![
            round(&["a"], ItemScript::Uncertain(engine_error())),
            round(
                &["b"],
                ItemScript::Uncertain(unsupported(bifrost_types::AccountOperation::UpdateFlags)),
            ),
        ],
    );
    for target in ["a", "b"] {
        state.keys.lock().unwrap().insert(
            ObjectId(target.into()),
            bifrost_types::ThrottleKey::Budget(bifrost_types::ThrottleBudget::new(target)),
        );
    }
    let (engine, id) = reassert_engine(&state, 5, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a", "b"]).await.unwrap();
    assert_eq!(state.doors.lock().unwrap().len(), 2);
    assert_eq!(state.reads.lock().unwrap().len(), 2);
    assert_eq!(counters.pending_retry, 2);
    assert_eq!(counters.blocked_by_engine, 0);
    assert!(counters.error.is_none());
}
#[tokio::test(start_paused = true)]
async fn moves_never_reassert() {
    let state = reassert_state(
        &["a"],
        vec![round(&["a"], ItemScript::Uncertain(retryable_transport()))],
    );
    state.observations.lock().unwrap().insert(
        ObjectId("a".into()),
        Observation::Membership(vec![MembershipScope::Folder(bifrost_types::FolderId(
            "old".into(),
        ))]),
    );
    let (engine, id) = reassert_engine(&state, 5, 10, true, None).await;
    let counters = engine
        .bulk_move(
            &id,
            vec![ObjectId("a".into())],
            MembershipScope::Folder(bifrost_types::FolderId("new".into())),
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .unwrap();
    assert_eq!(counters.pending_retry, 1);
    assert_eq!(state.doors.lock().unwrap().len(), 1);
    assert_eq!(state.reads.lock().unwrap().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn a_reasserted_omission_is_read_back_again() {
    let state = reassert_state(
        &["a"],
        vec![
            round(&["a"], ItemScript::Uncertain(retryable_transport())),
            RoundScript::default(),
            round(&["a"], ItemScript::Applied),
        ],
    );
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(counters.applied, 1);
    assert_eq!(state.doors.lock().unwrap().len(), 3);
    assert_eq!(state.reads.lock().unwrap().len(), 2);
}
#[tokio::test(start_paused = true)]
async fn a_dedupe_termination_after_reassertion_protects_the_round() {
    let state = reassert_state(
        &["a"],
        vec![
            round(&["a"], ItemScript::Uncertain(retryable_transport())),
            RoundScript {
                termination: Some(dedupe_error()),
                ..Default::default()
            },
        ],
    );
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(counters.pending_retry, 1);
    assert_eq!(state.doors.lock().unwrap().len(), 2);
    assert_eq!(state.reads.lock().unwrap().len(), 2);
}
fn held_state(ending: Option<AccountError>) -> Arc<ReassertState> {
    let mut rounds = vec![round(
        &["a", "a2"],
        ItemScript::Uncertain(retryable_transport()),
    )];
    let mut ids = vec!["a", "a2"];
    if let Some(error) = ending {
        ids.push("b");
        rounds.push(round(&["b"], ItemScript::Failed(held_error(Some("B"), 10))));
        rounds.push(RoundScript {
            termination: Some(error),
            ..Default::default()
        });
    } else {
        rounds.push(round(&["a"], ItemScript::Applied));
    }
    let state = reassert_state(&ids, rounds);
    for (target, budget) in [("a", "A"), ("a2", "A"), ("b", "B")] {
        state.keys.lock().unwrap().insert(
            ObjectId(target.into()),
            bifrost_types::ThrottleKey::Budget(bifrost_types::ThrottleBudget::new(budget)),
        );
    }
    state.observations.lock().unwrap().insert(
        ObjectId("a2".into()),
        Observation::Fail(held_error(Some("A"), 60)),
    );
    state
}
async fn wait_for_reads(state: &ReassertState, count: usize) {
    while state.reads.lock().unwrap().len() < count {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
}
fn spawn_campaign(
    engine: &Arc<SyncEngine>,
    id: &AccountId,
    ids: &[&str],
) -> tokio::task::JoinHandle<bifrost_sync::MutationCounters> {
    let engine = Arc::clone(engine);
    let id = id.clone();
    let ids = ids.iter().map(|id| (*id).to_string()).collect::<Vec<_>>();
    tokio::spawn(async move {
        cohort_run(
            &engine,
            &id,
            &ids.iter().map(String::as_str).collect::<Vec<_>>(),
        )
        .await
        .unwrap()
    })
}
#[tokio::test(start_paused = true)]
async fn a_held_reassertion_finalizes_on_abort_without_reobservation() {
    let state = held_state(Some(unsupported(
        bifrost_types::AccountOperation::UpdateFlags,
    )));
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a", "a2", "b"]).await.unwrap();
    {
        let doors = state.doors.lock().unwrap();
        let calls = doors.iter().map(call_ids).collect::<Vec<_>>();
        assert_eq!(calls, vec![vec!["a", "a2"], vec!["b"], vec!["b"]]);
    }
    assert_eq!(counters.pending_retry, 3);
    assert!(matches!(
        counters.error.as_deref(),
        Some(Error::Account(error)) if matches!(error.kind(), AccountErrorKind::Unsupported(_))
    ));
    let reads = state.reads.lock().unwrap();
    for target in ["a", "b"] {
        assert_eq!(
            reads.iter().flatten().filter(|id| id.0 == target).count(),
            1
        );
    }
}
#[tokio::test(start_paused = true)]
async fn capability_loss_on_reopen_cancels_a_ready_reassertion() {
    let state = held_state(None);
    let (engine, id) = reassert_engine(&state, 3, 10, true, Some(false)).await;
    let task = spawn_campaign(&engine, &id, &["a", "a2"]);
    wait_for_reads(&state, 1).await;
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    engine.reattach(&id).await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(60)).await;
    let counters = task.await.unwrap();
    assert_eq!(state.doors.lock().unwrap().len(), 1);
    assert_eq!(state.reads.lock().unwrap().len(), 1);
    assert_eq!(counters.pending_retry, 2);
}
#[tokio::test(start_paused = true)]
async fn a_read_back_engine_failure_blocks_reassertion_and_forwards_once() {
    let state = reassert_state(
        &["a", "b", "c"],
        vec![round(
            &["a", "b", "c"],
            ItemScript::Uncertain(retryable_transport()),
        )],
    );
    for target in ["b", "c"] {
        state
            .observations
            .lock()
            .unwrap()
            .insert(ObjectId(target.into()), Observation::Fail(operator_error()));
    }
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let mut controls = engine.account_control_stream(&id).unwrap();
    let task = spawn_campaign(&engine, &id, &["a", "b", "c"]);
    let event = controls.recv().await.unwrap();
    assert!(matches!(
        event,
        bifrost_types::AccountControl::Pause(bifrost_types::PauseReason::OperatorOverrideRequired)
    ));
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    assert!(matches!(
        controls.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    engine.resume_account(&id).unwrap();
    let counters = task.await.unwrap();
    assert_eq!(state.doors.lock().unwrap().len(), 1);
    assert_eq!(counters.pending_retry, 3);
}
#[tokio::test(start_paused = true)]
async fn a_stream_state_refresh_termination_routes_to_read_back() {
    let mut second = round(&["a"], ItemScript::Applied);
    second.then = Some((ObjectId("a".into()), Observation::Flags(set(&["\\Seen"]))));
    let state = reassert_state(
        &["a"],
        vec![
            RoundScript {
                termination: Some(conflict_error()),
                ..Default::default()
            },
            second,
        ],
    );
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(counters.applied, 1);
    // A blind retry would have reached the Applied round with no read-back.
    assert_eq!(state.reads.lock().unwrap().len(), 1);
    assert_eq!(state.doors.lock().unwrap().len(), 2);
}
#[tokio::test(start_paused = true)]
async fn destroy_reasserts_only_a_present_object() {
    let state = reassert_state(
        &["a", "b"],
        vec![
            round(&["a", "b"], ItemScript::Uncertain(retryable_transport())),
            round(&["a"], ItemScript::Applied),
        ],
    );
    state
        .observations
        .lock()
        .unwrap()
        .insert(ObjectId("b".into()), Observation::NotFound);
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let counters = engine
        .bulk_destroy(
            &id,
            vec![ObjectId("a".into()), ObjectId("b".into())],
            &vendor(),
            ProtocolKind::Imap,
        )
        .await
        .unwrap();
    assert_eq!(counters.skipped, 1);
    assert_eq!(counters.applied, 1);
    let doors = state.doors.lock().unwrap();
    assert_eq!(doors.len(), 2);
    assert_eq!(call_ids(&doors[1]), vec!["a"]);
}
#[tokio::test(start_paused = true)]
async fn a_concurrent_change_between_rounds_is_judged_on_the_new_state() {
    let mut second = round(&["a"], ItemScript::Uncertain(retryable_transport()));
    second.then = Some((ObjectId("a".into()), Observation::Flags(set(&["\\seen"]))));
    let state = reassert_state(
        &["a"],
        vec![
            round(&["a"], ItemScript::Uncertain(retryable_transport())),
            second,
        ],
    );
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(counters.skipped, 1);
    assert_eq!(state.doors.lock().unwrap().len(), 2);
    assert_eq!(state.reads.lock().unwrap().len(), 2);
}
#[tokio::test(start_paused = true)]
async fn pause_parks_a_ready_reassertion_without_spending_an_attempt() {
    let state = held_state(None);
    let (engine, id) = reassert_engine(&state, 1, 10, true, None).await;
    let task = spawn_campaign(&engine, &id, &["a", "a2"]);
    wait_for_reads(&state, 1).await;
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    let control = state.control.lock().unwrap().clone().unwrap();
    control.pause().await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(85)).await;
    assert_eq!(state.doors.lock().unwrap().len(), 1);
    control.resume();
    let counters = task.await.unwrap();
    let doors = state.doors.lock().unwrap();
    assert_eq!(doors.len(), 2);
    assert_eq!(key_parts(&doors[0].key), key_parts(&doors[1].key));
    assert_eq!(counters.applied, 1);
}
#[tokio::test(start_paused = true)]
async fn shutdown_during_a_reassertion_round_leaves_the_target_unresolved() {
    let state = reassert_state(
        &["a"],
        vec![
            round(&["a"], ItemScript::Uncertain(retryable_transport())),
            RoundScript {
                park: Some(Arc::new(tokio::sync::Notify::new())),
                ..Default::default()
            },
        ],
    );
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let task = spawn_campaign(&engine, &id, &["a"]);
    loop {
        let entered = state.door_entered.notified();
        if state.doors.lock().unwrap().len() >= 2 {
            break;
        }
        entered.await;
    }
    engine.detach(&id).await.unwrap();
    let counters = task.await.unwrap();
    assert_eq!(counters.pending_retry, 1);
    assert!(matches!(
        counters.error.as_deref(),
        Some(Error::ShuttingDown)
    ));
    assert_eq!(state.doors.lock().unwrap().len(), 2);
    assert_eq!(state.reads.lock().unwrap().len(), 1);
}
#[tokio::test(start_paused = true)]
async fn repeated_reassertions_consume_the_attempt_budget() {
    let state = reassert_state(&["a"], vec![]);
    let (engine, id) = reassert_engine(&state, 2, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a"]).await.unwrap();
    assert_eq!(state.doors.lock().unwrap().len(), 3);
    assert_eq!(state.reads.lock().unwrap().len(), 3);
    assert_eq!(counters.pending_retry, 1);
}
#[tokio::test(start_paused = true)]
async fn an_engine_block_turns_a_ready_reassertion_into_blocked_by_engine() {
    let state = held_state(Some(engine_error()));
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a", "a2", "b"]).await.unwrap();
    {
        let doors = state.doors.lock().unwrap();
        let calls = doors.iter().map(call_ids).collect::<Vec<_>>();
        assert_eq!(calls, vec![vec!["a", "a2"], vec!["b"], vec!["b"]]);
    }
    assert_eq!(
        state
            .reads
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .filter(|id| id.0 == "a")
            .count(),
        1
    );
    assert_eq!(counters.blocked_by_engine, 2);
    assert_eq!(counters.pending_retry, 1);
    // The campaign awaited its `reopen_tx` sends before returning, so every
    // request it forwarded is already queued. Sleeping (not yielding) lets
    // paused time auto-advance through any repair backoff, so a second
    // request, or a retried establishment, would land before the assertion.
    tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
    assert_eq!(
        *state.establishes.lock().unwrap(),
        vec![CursorScope::Account],
        "exactly one recovery request forwarded"
    );
}
#[tokio::test(start_paused = true)]
async fn a_guarded_stream_state_refresh_termination_still_retries_with_frozen_versions() {
    let state = reassert_state(
        &["g", "u"],
        vec![
            RoundScript {
                termination: Some(conflict_error()),
                ..Default::default()
            },
            round(&["g"], ItemScript::Applied),
            round(&["u"], ItemScript::Applied),
        ],
    );
    let (engine, id) = reassert_engine(&state, 2, 10, true, None).await;
    let counters = guarded_run(&engine, &id).await;
    let doors = state.doors.lock().unwrap();
    assert_eq!(doors.len(), 3);
    assert!(doors[0].versioned && doors[1].versioned && !doors[2].versioned);
    assert_eq!(call_ids(&doors[1]), vec!["g"]);
    assert_eq!(call_ids(&doors[2]), vec!["u"]);
    assert_eq!(
        format!("{:?}", doors[0].targets[0].expected_version),
        format!("{:?}", doors[1].targets[0].expected_version)
    );
    assert_eq!(
        *state.reads.lock().unwrap(),
        vec![vec![ObjectId("u".into())]]
    );
    assert_eq!(counters.applied, 2);
}
#[tokio::test(start_paused = true)]
async fn a_read_back_termination_finalizes_observed_mismatches() {
    let state = reassert_state(
        &["a"],
        vec![round(
            &["a", "b"],
            ItemScript::Uncertain(retryable_transport()),
        )],
    );
    *state.readback_termination.lock().unwrap() =
        Some(unsupported(bifrost_types::AccountOperation::Hydrate));
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let counters = cohort_run(&engine, &id, &["a", "b"]).await.unwrap();
    assert_eq!(state.doors.lock().unwrap().len(), 1);
    assert_eq!(state.reads.lock().unwrap().len(), 1);
    assert_eq!(counters.pending_retry, 2);
    assert!(matches!(
        counters.error.as_deref(),
        Some(Error::Account(error)) if matches!(error.kind(), AccountErrorKind::Unsupported(_))
    ));
}
#[tokio::test(start_paused = true)]
async fn an_uncertain_operation_local_retry_hint_delays_the_read_back() {
    let state = reassert_state(
        &["a"],
        vec![
            round(&["a"], ItemScript::Uncertain(held_error(None, 30))),
            round(&["a"], ItemScript::Applied),
        ],
    );
    let (engine, id) = reassert_engine(&state, 3, 10, true, None).await;
    let task = spawn_campaign(&engine, &id, &["a"]);
    loop {
        let entered = state.door_entered.notified();
        if !state.doors.lock().unwrap().is_empty() {
            break;
        }
        entered.await;
    }
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(29)).await;
    tokio::task::yield_now().await;
    assert!(state.reads.lock().unwrap().is_empty());
    assert_eq!(state.doors.lock().unwrap().len(), 1);
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    let counters = task.await.unwrap();
    assert_eq!(state.reads.lock().unwrap().len(), 1);
    assert_eq!(state.doors.lock().unwrap().len(), 2);
    assert_eq!(counters.applied, 1);
}
