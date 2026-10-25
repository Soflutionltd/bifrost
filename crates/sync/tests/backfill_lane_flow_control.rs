//! End-to-end pins for the bounded backfill lane (`engine::lane`).
//!
//! Cold-start pages and live changes share one broadcast ring with no
//! producer-side backpressure of its own. Backfill now takes one lane permit per
//! published page and returns it once the consumer acknowledges that page (or
//! any later one) through the ack writer, so a slow consumer applies flow
//! control instead of losing pages to ring overflow and re-reading from the last
//! durable checkpoint.
//!
//! These drive the whole pipeline against `common::StubAccount` - attach, the
//! orchestrator's fixed-plan loop, the partition runner, the broadcast, the ack
//! writer - because every one of the properties below is a property of that
//! assembly rather than of the bound itself, which has its own unit tests.
//!
//! Every test here runs on the CURRENT-THREAD runtime with `start_paused`. That
//! is deliberate and is not a weaker choice: a current-thread runtime interleaves
//! tasks at every await, and paused time additionally advances only once every
//! task is idle - which turns "the walk stopped and stayed stopped" into a
//! deterministic observation instead of a wall-clock guess, and makes "the
//! spawned waiter has parked" a fact rather than a hope. There are no wall-clock
//! sleeps anywhere in this file; the `sleep` calls are virtual.
//!
//! None of these tests is a concurrency race test. The cross-task ordering
//! properties live in `multiplexer::tests`, on a multi-threaded runtime.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::{
    BackfillConfig, CheckpointStore, ConcurrencyBudget, EngineConfig, InMemoryCheckpointStore,
    MultiplexerConfig, MultiplexerEvent, SyncEngine, WorkKind,
};
use bifrost_types::{
    AccountFactory, AccountId, Batch, Change, Checkpoint, CoverageDomain, CursorScope, Fingerprint,
    InventoryBatch, InventoryCompletion, InventoryCoverageReport, InventoryEntry, InventoryEvent,
    InventoryPartition, InventoryPartitioning, ObjectId, PageBoundary, PartitionEnd, ServerVersion,
    SyncEvent, WarningKind,
};

/// Page width of every backfill partition in these tests, and how many of them
/// the plan holds.
const PAGE: u32 = 2;
const PARTITIONS: u32 = 40;
/// Well under `PARTITIONS`, so a bound that is not honoured is unmissable.
const LANE: usize = 2;
/// Deliberately smaller than `PARTITIONS * PAGE`: an unbounded producer
/// overruns it and the consumer sees a lag warning plus missing ids. That is
/// what gives `no_backfill_page_is_lost_or_duplicated_under_the_bound` its bite.
const RING: usize = 8;

fn entry(id: &str) -> InventoryEntry {
    InventoryEntry {
        id: ObjectId(id.into()),
        memberships: Vec::new(),
        size: None,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: ServerVersion::Unavailable,
            size: None,
            flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
}

/// Ids this partition's page carries.
///
/// The FIRST partition is deliberately fat and the rest are thin. A test that
/// observes a stall against uniform pages can pass against a producer with no
/// bound at all if the scheduling happens to line up; an asymmetric head makes
/// "how far did it get" a real measurement rather than a coincidence, and makes
/// the delivered-id set in the no-loss test asymmetric too, so an off-by-a-page
/// answer cannot look correct.
fn page_ids(from: u32) -> Vec<String> {
    let width = if from == 0 { 20 } else { PAGE };
    (0..width).map(|n| format!("p{from}-{n}")).collect()
}

fn expected_ids() -> HashSet<String> {
    (0..PARTITIONS)
        .flat_map(|partition| page_ids(partition * PAGE))
        .collect()
}

/// A stub whose fixed plan is `PARTITIONS` page windows, each yielding one
/// checkpoint-bearing batch and then `Done`.
fn paged_stub(scope: &CursorScope) -> common::StubAccount {
    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    stub.partitioning = InventoryPartitioning::PageCount {
        total: Some(PARTITIONS * PAGE),
        page_size: Some(PAGE),
    };
    stub.partition_hook = Some(Arc::new(move |scope, partition| {
        let from = match partition {
            InventoryPartition::Page { from, .. } => *from,
            other => panic!("the PageCount plan yields page partitions only: {other:?}"),
        };
        let items: Vec<InventoryEntry> = page_ids(from).iter().map(|id| entry(id)).collect();
        vec![
            InventoryEvent::Batch(
                InventoryBatch::try_new(
                    items,
                    PageBoundary::Final,
                    Duration::ZERO,
                    0,
                    None,
                    InventoryCoverageReport::complete(CoverageDomain::full(scope.clone())),
                )
                .expect("a Final page with no checkpoint is boundary-valid"),
            ),
            // A fixed plan walks every window whatever this says.
            InventoryEvent::Done(InventoryCompletion::complete(
                CoverageDomain::full(scope.clone()),
                None,
                PartitionEnd::MoreBeyond,
            )),
        ]
    }));
    stub
}

/// The same walk as `paged_stub`, but OPEN-ENDED: `PageCount { total: None }`,
/// which drives `BackfillPlan::OpenPages` and therefore the positional resume.
///
/// Uniform page widths on purpose. `get_backfill` returns the latest checkpoint
/// by `items_done`, breaking ties on the higher window end, so equal-width pages
/// make "the furthest window the consumer acknowledged" the resume position -
/// which is precisely the position a retry must NOT trust when the walk lost
/// pages behind it.
fn open_pages_stub(scope: &CursorScope) -> common::StubAccount {
    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    stub.partitioning = InventoryPartitioning::PageCount {
        total: None,
        page_size: Some(PAGE),
    };
    stub.partition_hook = Some(Arc::new(move |scope, partition| {
        let from = match partition {
            InventoryPartition::Page { from, .. } => *from,
            other => panic!("the PageCount plan yields page partitions only: {other:?}"),
        };
        // Past the end of the inventory the window comes back empty and
        // declares the listing dry, which is the only thing an open-ended walk
        // may terminate on.
        let past_the_end = from >= PARTITIONS * PAGE;
        let items: Vec<InventoryEntry> = if past_the_end {
            Vec::new()
        } else {
            (0..PAGE).map(|n| entry(&format!("o{from}-{n}"))).collect()
        };
        vec![
            InventoryEvent::Batch(
                InventoryBatch::try_new(
                    items,
                    PageBoundary::Final,
                    Duration::ZERO,
                    0,
                    None,
                    InventoryCoverageReport::complete(CoverageDomain::full(scope.clone())),
                )
                .expect("a Final page with no checkpoint is boundary-valid"),
            ),
            InventoryEvent::Done(InventoryCompletion::complete(
                CoverageDomain::full(scope.clone()),
                None,
                if past_the_end {
                    PartitionEnd::Exhausted
                } else {
                    PartitionEnd::MoreBeyond
                },
            )),
        ]
    }));
    stub
}

fn config(poll: Option<Duration>) -> EngineConfig {
    let mut multiplexer = MultiplexerConfig {
        changes_capacity: RING,
        ..MultiplexerConfig::default()
    };
    if let Some(poll) = poll {
        multiplexer.poll_initial = poll;
        multiplexer.poll_min = poll;
        multiplexer.poll_max = poll;
    }
    EngineConfig {
        multiplexer,
        backfill: BackfillConfig {
            lane_capacity: LANE,
            ..BackfillConfig::default()
        },
        ..EngineConfig::default()
    }
}

/// An `InMemoryCheckpointStore` that RECORDS which scopes `delete_backfill` was
/// called for, in order.
///
/// The discard requests are engine-internal, so "whose request did this drain
/// take" is only observable as the deletes the writer performs. Recording them
/// turns the call site into something a test can assert on rather than infer.
struct RecordingDeleteStore {
    inner: InMemoryCheckpointStore,
    deleted: std::sync::Mutex<Vec<CursorScope>>,
}

impl RecordingDeleteStore {
    fn new() -> Self {
        Self {
            inner: InMemoryCheckpointStore::default(),
            deleted: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn deleted(&self) -> Vec<CursorScope> {
        self.deleted.lock().expect("delete log").clone()
    }
}

impl CheckpointStore for RecordingDeleteStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.clear_account_checkpoints(account, ledger)
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: bifrost_sync::CheckpointTransition,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.apply_transition(account, transition)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::ChangeCursor>, bifrost_sync::Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::BackfillCheckpoint>, bifrost_sync::Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: bifrost_sync::DebtLedger,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<bifrost_sync::DebtLedger, bifrost_sync::Error>>
                + Send
                + 'a,
        >,
    > {
        self.inner.get_ledger(account)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.deleted.lock().expect("delete log").push(scope.clone());
        self.inner.delete_backfill(account, scope)
    }
}

/// An `InMemoryCheckpointStore` whose `delete_backfill` always FAILS.
///
/// A seam, not a curiosity: the durable discard and the in-memory
/// restart-from-scratch note are two answers to the same finding, and a
/// successful delete produces a fresh walk on its own - so a test that leaves the
/// delete working cannot tell which of the two did the work. With the delete
/// failing, only the in-memory override can produce it.
struct FailingDeleteStore {
    inner: InMemoryCheckpointStore,
}

impl FailingDeleteStore {
    fn new() -> Self {
        Self {
            inner: InMemoryCheckpointStore::default(),
        }
    }
}

impl CheckpointStore for FailingDeleteStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: bifrost_sync::cursor::DebtLedger,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        let _ = (account, ledger);
        Box::pin(async {
            Err(bifrost_sync::Error::CheckpointStore(
                "delete refused".into(),
            ))
        })
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: bifrost_sync::CheckpointTransition,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.apply_transition(account, transition)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::ChangeCursor>, bifrost_sync::Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::BackfillCheckpoint>, bifrost_sync::Error>,
                > + Send
                + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: bifrost_sync::DebtLedger,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<bifrost_sync::DebtLedger, bifrost_sync::Error>>
                + Send
                + 'a,
        >,
    > {
        self.inner.get_ledger(account)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        _account: &'a AccountId,
        _scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), bifrost_sync::Error>> + Send + 'a>,
    > {
        Box::pin(async {
            Err(bifrost_sync::Error::CheckpointStore(
                "delete_backfill is unavailable in this test".into(),
            ))
        })
    }
}

async fn attach(
    account_id: &AccountId,
    stub: Arc<common::StubAccount>,
    store: Arc<InMemoryCheckpointStore>,
    config: EngineConfig,
    budget: Option<ConcurrencyBudget>,
) -> SyncEngine {
    attach_over(
        account_id,
        stub,
        store as Arc<dyn CheckpointStore>,
        config,
        budget,
    )
    .await
}

async fn attach_over(
    account_id: &AccountId,
    stub: Arc<common::StubAccount>,
    store: Arc<dyn CheckpointStore>,
    mut config: EngineConfig,
    budget: Option<ConcurrencyBudget>,
) -> SyncEngine {
    if let Some(budget) = budget {
        config.budget = budget;
    }
    let engine = SyncEngine::builder()
        .config(config)
        .checkpoints(store)
        .build()
        .expect("engine config is valid");
    let factory: Arc<dyn AccountFactory> = Arc::new(common::StubFactory::queue(vec![stub]));
    engine
        .attach(account_id.clone(), factory)
        .await
        .expect("attach succeeds");
    engine
}

fn backfill_checkpoint(event: &MultiplexerEvent) -> Option<&bifrost_types::BackfillCheckpoint> {
    match &event.checkpoint {
        Some(Checkpoint::Backfill(checkpoint)) => Some(checkpoint),
        _ => None,
    }
}

fn batch_ids(event: &MultiplexerEvent) -> Vec<String> {
    match event.event.as_ref() {
        SyncEvent::Batch(batch) => batch
            .items
            .iter()
            .filter_map(|change| match change {
                Change::ObjectChange(object) => Some(object.id.0.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The partition count once the walk has demonstrably STOPPED advancing.
///
/// "Read `LANE` pages" only proves the producer got that far; a test that then
/// asserts something about a parked producer has to establish it is parked, or
/// it is asserting about a producer that was simply between partitions.
///
/// Under `start_paused` this is deterministic rather than a wall-clock guess:
/// tokio only advances virtual time once every task is idle, so a `sleep` that
/// returns is proof that nothing was still running - including the orchestrator's
/// own 1s rescan tick, which has had its chance too.
async fn settled_partition_count(stub: &Arc<common::StubAccount>) -> usize {
    loop {
        let before = stub.walked_partitions().len();
        tokio::time::sleep(Duration::from_millis(200)).await;
        if stub.walked_partitions().len() == before {
            return before;
        }
    }
}

/// Establish that the producer is parked at the bound, holding its pages
/// UNREAD.
///
/// The bound is a receipt bound: a page leaves the ring's pressure when the
/// consumer READS it, not when the consumer acknowledges it. So the staging for
/// a parked producer is a subscriber that does not read - the caller subscribes
/// (the walk needs a subscriber to start at all) and then simply leaves the
/// pages sitting in the ring. Reading `LANE` pages, which is what these tests
/// used to do, now RELEASES the producer for `LANE` more.
///
/// Returns the settled walk length, which the caller may compare against later
/// readings.
async fn park_the_producer_unread(stub: &Arc<common::StubAccount>) -> usize {
    let parked = settled_partition_count(stub).await;
    assert!(
        parked <= LANE + 1,
        "the producer must be parked at the bound with its pages unread; walked {parked}"
    );
    parked
}

/// Read `LANE` backfill pages and acknowledge NONE of them, which is what makes
/// them strandable when this receiver departs.
///
/// This is NOT a park. Under the receipt bound the read is the permit, so by the
/// time this returns the producer has been freed for `LANE` further pages and
/// is parked one bound further along - which is why the callers that assert on
/// the walk length allow `2 * LANE + 1` rather than `LANE + 1`. What the helper
/// stages is the hole: pages one receiver took delivery of and never answered
/// for, which a replacement joining at the ring's tail can never see.
async fn read_without_acknowledging(
    events: &mut bifrost_sync::ChangesReceiver,
) -> Vec<(CursorScope, Checkpoint, bifrost_sync::PublicationId)> {
    let mut held = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        while held.len() < LANE {
            let event = events.recv().await.expect("the broadcast stays open");
            assert!(
                !matches!(event.event.as_ref(), SyncEvent::Warning(w)
                    if w.kind == WarningKind::ChangeStreamLagged),
                "a bounded backfill lane must not lag its own consumer"
            );
            if let Some(checkpoint) = backfill_checkpoint(&event) {
                held.push((
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone().expect("checkpointed publication"),
                ));
            }
        }
    })
    .await
    .expect("the first pages must reach the consumer");
    held
}

/// THE property, and the whole of the receipt bound in one test. A consumer that
/// does not READ stops the producer: no further partition is even requested from
/// the account until a page is taken out of the ring. A consumer that reads
/// releases exactly what it read. And an acknowledgement of a page already read
/// releases nothing further, because the read already did.
///
/// That last assertion is the one that separates the two contracts. Under the
/// acknowledgement bound it fails: the ack was the permit, so it bought a page of
/// headroom and the walk moved on.
#[tokio::test(start_paused = true)]
async fn a_consumer_that_holds_pages_unread_stalls_the_backfill_producer() {
    let account_id = AccountId("lane-stall".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // The consumer subscribes - the walk needs a subscriber to start at all -
    // and then reads nothing. The bound is enforced BEFORE the wire call, not
    // merely at the broadcast, so at most one page is in the producer's hand
    // past the bound.
    let parked_at = park_the_producer_unread(&stub).await;

    // One read, one page of headroom: the READ is the permit signal.
    let page = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = events.recv().await.expect("the broadcast stays open");
            if let Some(checkpoint) = backfill_checkpoint(&event) {
                return (
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone().expect("checkpointed publication"),
                );
            }
        }
    })
    .await
    .expect("the pages the producer already published are there to be read");
    let after_one_read = settled_partition_count(&stub).await;
    assert!(
        after_one_read > parked_at,
        "reading a page must release the producer for another; it stayed at {parked_at}"
    );
    assert!(
        after_one_read <= parked_at + 2,
        "and for ONE more, not for the rest of the walk; it ran to {after_one_read}. \
         The mechanism frees exactly one page per read; the second unit of slack is \
         the SAMPLING, not the bound. `parked_at` is read while the producer may \
         have one page in hand past the bound (`park_the_producer_unread` allows \
         `LANE + 1` for that reason), so a reading taken at `LANE` and one taken \
         after the freed page settled differ by two"
    );

    // And the acknowledgement of that same page releases nothing further. It is
    // a promise about the consumer's own durability, made on whatever schedule
    // the consumer likes; the ring pressure it would have relieved was already
    // relieved by the read.
    engine
        .ack_checkpoint(&account_id, page.0, page.1, Some(page.2))
        .await
        .expect("the ack persists");
    assert_eq!(
        settled_partition_count(&stub).await,
        after_one_read,
        "an acknowledgement is not the permit: the producer must stay parked behind \
         the pages the consumer has still not read"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The fairness rule, stated as a test: the lane bound is a producer-side gate
/// on backfill and nothing else consults it, so live changes keep flowing at
/// full rate while cold start is parked.
#[tokio::test(start_paused = true)]
async fn live_changes_still_flow_while_the_backfill_producer_is_parked() {
    let account_id = AccountId("lane-live".to_owned());
    let scope = CursorScope::Account;
    let mut stub = paged_stub(&scope);
    // A live batch on every poll, carrying no checkpoint so it is unambiguously
    // distinguishable from a backfill page and takes no lane permit.
    stub.changes_hook = Some(Arc::new(|_cursor| {
        vec![SyncEvent::Batch(Batch {
            items: vec![Change::ObjectChange(bifrost_types::ObjectChange {
                id: ObjectId("live".into()),
                kind: bifrost_types::ObjectChangeKind::Updated,
            })],
            page_boundary: PageBoundary::Final,
            server_latency: Duration::ZERO,
            bytes_in: 0,
            checkpoint: None,
        })]
    }));
    let stub = Arc::new(stub);
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(Some(Duration::from_millis(50))),
        // FINDING 3's bite. `per_account = 2` with the default 1/4 mutation
        // share leaves the account exactly ONE sync permit, and `global = 1`
        // makes it the whole engine's. A backfill that parks while holding it
        // starves the live lane on the very mechanism built to protect it. The
        // default budget has spare permits and cannot observe this at all.
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let walked_at_the_bound = park_the_producer_unread(&stub).await;

    // Now, with no acknowledgement at all, live changes must keep arriving. The
    // one receiver reads both lanes, so draining live batches unavoidably reads
    // backfill pages too and hands the producer that many permits back - which
    // is exactly the accounting the assertion below is written in. What must NOT
    // happen is the backfill lane running away regardless.
    let mut live = 0_usize;
    let mut pages_read = 0_usize;
    tokio::time::timeout(Duration::from_secs(10), async {
        while live < 3 {
            let event = events.recv().await.expect("the broadcast stays open");
            if backfill_checkpoint(&event).is_some() {
                pages_read += 1;
            } else if batch_ids(&event) == vec!["live".to_owned()] {
                live += 1;
            }
        }
    })
    .await
    .expect("a parked backfill must not stall the live lane");

    let walked = stub.walked_partitions().len();
    assert!(
        walked <= walked_at_the_bound + pages_read + 1,
        "the backfill producer must stay within the bound the consumer's reads bought \
         it: parked at {walked_at_the_bound}, {pages_read} pages read, walked {walked}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The whole point of the change: with the bound in place and the consumer
/// acknowledging as it reads, every inventory page reaches the consumer exactly
/// once through a ring far smaller than the walk.
///
/// Before the lane, this walk overran a 8-slot ring, the receiver reported
/// `ChangeStreamLagged`, the account's outstanding publications were abandoned,
/// and the lost pages' ids never arrived at all.
#[tokio::test(start_paused = true)]
async fn no_backfill_page_is_lost_or_duplicated_under_the_bound() {
    let account_id = AccountId("lane-no-loss".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let mut seen: Vec<String> = Vec::new();
    let mut pages = 0_usize;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = events.recv().await.expect("the broadcast stays open");
            assert!(
                !matches!(event.event.as_ref(), SyncEvent::Warning(w)
                    if w.kind == WarningKind::ChangeStreamLagged),
                "the bound exists precisely so this walk never lags its consumer"
            );
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            pages += 1;
            seen.extend(batch_ids(&event));
            // A DELIBERATELY slow consumer. Without it the reader keeps up with
            // the producer by accident on a fast machine, and the test measures
            // nothing: the ring is 8 slots against a 40-partition walk, so an
            // unbounded producer has to be given the chance to run away before
            // "it did not" means anything.
            tokio::time::sleep(Duration::from_millis(5)).await;
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                break;
            }
        }
    })
    .await
    .expect("the walk must run to its completion sentinel");

    let unique: HashSet<String> = seen.iter().cloned().collect();
    assert_eq!(
        seen.len(),
        unique.len(),
        "no page may be delivered twice under the bound"
    );
    assert_eq!(unique, expected_ids(), "and none may be lost");
    assert_eq!(
        pages,
        PARTITIONS as usize + 1,
        "one checkpoint-bearing batch per partition, plus the completion sentinel"
    );
    assert_eq!(
        stub.walked_partitions().len(),
        PARTITIONS as usize,
        "and every partition was walked exactly once"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// A retained receiver must see the channel CLOSE after detach, not wait for
/// ever on a channel it is itself keeping open.
///
/// The receiver's handle back to the account's delivery gate is weak for exactly
/// this reason: a strong one kept the broadcast sender alive, so a consumer
/// draining the ring after `detach` would sit in `recv()` indefinitely with no
/// producer left and no way to learn that.
#[tokio::test(start_paused = true)]
async fn a_retained_receiver_sees_the_channel_close_after_detach() {
    let account_id = AccountId("lane-close".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    park_the_producer_unread(&stub).await;

    engine.detach(&account_id).await.expect("detach succeeds");
    drop(engine);

    // Drain whatever is still buffered, then the channel must report Closed.
    let closed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match events.recv().await {
                Ok(_) => {}
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("a detached account's stream must not leave its consumer waiting");
    assert!(
        matches!(closed, tokio::sync::broadcast::error::RecvError::Closed),
        "expected Closed after detach, got {closed:?}"
    );
}

/// FINDING 3. A walk whose pages reached NOBODY must not settle the scope.
///
/// The receiver-drop sweep wakes the parked producer; if the consumer that left
/// was the only one, every page that follows is retired on send, the completion
/// sentinel with it, and the walk "completes" having delivered nothing. Settling
/// the incarnation on that would retire it for the life of the attachment - a
/// consumer that reattaches gets no inventory and no retry, silently.
#[tokio::test(start_paused = true)]
async fn a_walk_that_reached_nobody_is_not_settled() {
    let account_id = AccountId("lane-nobody".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;

    let events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    park_the_producer_unread(&stub).await;

    // The only consumer leaves. The sweep frees the bound and the producer is
    // free to run - into nobody.
    drop(events);
    let ran_on = settled_partition_count(&stub).await;

    // Now a consumer comes back. The scope must still be eligible, and the walk
    // must be offered again rather than having been settled while nobody was
    // listening.
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let resumed = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            if backfill_checkpoint(&event).is_some() {
                return;
            }
        }
    })
    .await;
    assert!(
        resumed.is_ok(),
        "a scope whose walk reached no consumer must be re-walked for the next one; \
         it stopped at {ran_on} of {PARTITIONS} partitions and offered nothing after"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The mid-walk receiver replacement, end to end. A subscriber present when the
/// walk finishes proves nothing about the middle of it.
///
/// A fills the lane and leaves without acknowledging: the departure sweep retires
/// those pages as undelivered, because a replacement joins at the ring's TAIL and
/// can never take delivery of them. B subscribes immediately - the sweep runs
/// inline in `drop`, so no producer poll can interleave - and then receives and
/// acknowledges every remaining page plus the completion marker. Every
/// point-in-time check is satisfied: a subscriber was there at the end, the walk
/// ran to exhaustion, the marker was acknowledged. And the scope still has a hole
/// in the middle of it, which is exactly what the walk-wide undelivered watermark
/// exists to catch.
///
/// The bite is one layer up from the settle decision: the completion MARKER is
/// durable. Emitting it on a walk that left a hole lets B acknowledge it into the
/// checkpoint store, and the next rescan then reads it back, answers
/// `ScopeResume::Skip`, and settles the incarnation anyway - so declining to
/// settle in memory buys nothing. The marker is therefore withheld on the same
/// condition.
#[tokio::test(start_paused = true)]
async fn a_mid_walk_receiver_replacement_does_not_settle_the_scope() {
    let account_id = AccountId("lane-midwalk".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let held = read_without_acknowledging(&mut first).await;
    let parked_at = settled_partition_count(&stub).await;
    assert!(
        parked_at <= 2 * LANE + 1,
        "A's reads bought the producer one further bound and no more; walked {parked_at}"
    );
    let stranded: HashSet<String> = held
        .iter()
        .map(|(_, checkpoint, _)| match checkpoint {
            Checkpoint::Backfill(backfill) => String::from_utf8_lossy(&backfill.partition.0).into(),
            other => panic!("backfill pages carry backfill checkpoints: {other:?}"),
        })
        .collect();

    // A leaves with those pages unacknowledged, and B takes its place before the
    // producer can be polled: `drop` performs the departure sweep inline on this
    // task, so the replacement is staged with certainty rather than by timing.
    drop(held);
    drop(first);
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // B behaves impeccably: it acknowledges every page it is given, and the
    // completion marker with it. The walked count at the moment the marker
    // arrives is the measurement: a marker that a single pass earned comes back
    // at `PARTITIONS`, and one that only a re-walk earned comes back beyond it.
    let mut first_page: Option<String> = None;
    let mut walked_at_completion = 0_usize;
    let completion = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let partition: String = String::from_utf8_lossy(&checkpoint.partition.0).into();
            let completion = checkpoint.partition.0 == b"complete";
            if first_page.is_none() {
                first_page = Some(partition);
            }
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            if completion {
                walked_at_completion = stub.walked_partitions().len();
                return;
            }
        }
    })
    .await;

    // The staging is only meaningful if the replacement really did start BEYOND
    // the pages its predecessor stranded. A broadcast receiver joins at the ring
    // tail, so it cannot have them - assert it rather than assume it.
    let first_page = first_page.expect("the replacement receives pages at all");
    assert!(
        !stranded.contains(&first_page),
        "the replacement must start beyond the pages its predecessor stranded, or there \
         is no hole and this test stages nothing: stranded {stranded:?}, first page \
         received {first_page}"
    );
    assert!(
        completion.is_ok(),
        "the walk must reach a completion marker eventually; it stalled at {} of \
         {PARTITIONS} partitions",
        stub.walked_partitions().len()
    );
    // THE assertion. Every point-in-time check was satisfied at the end of the
    // first pass - a subscriber was present, the walk ran to exhaustion - and it
    // still left a hole, so no durable completion may be recorded for it. The
    // only marker allowed is one a second, whole walk earned.
    assert!(
        walked_at_completion > PARTITIONS as usize,
        "a walk with a hole in the middle must not record a durable completion: the \
         marker arrived after only {walked_at_completion} of {PARTITIONS} partitions, so \
         the scope was settled with the pages the departed consumer stranded never \
         re-offered to anyone"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The same hole, on an OPEN-ENDED plan, where withholding the completion marker
/// is not enough on its own.
///
/// `PageCount { total: None }` resumes positionally: `BackfillPlan::resume` reads
/// the furthest durably acknowledged window and starts the next walk after it. So
/// a retry scheduled because the walk lost pages resumes PAST the pages it lost -
/// the replacement consumer legitimately acknowledged the later windows, and
/// those sit beyond the hole - walks one empty probe, reaches exhaustion, and
/// settles the incarnation having never re-offered the lost pages to anyone. The
/// attempt has to give up the stored position and restart the walk whole.
#[tokio::test(start_paused = true)]
async fn a_retry_after_lost_pages_restarts_an_open_ended_walk_from_its_beginning() {
    let account_id = AccountId("lane-openpages-retry".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(open_pages_stub(&scope));
    // The durable discard is DISABLED here, deliberately. It and the in-memory
    // restart note are two answers to the same finding, and a successful delete
    // forces a fresh walk on its own - so with the delete working this test would
    // pass against the in-memory override being deleted. The failing store leaves
    // the misleading rows in place, so only the override can produce a walk that
    // starts from the beginning.
    let engine = attach_over(
        &account_id,
        Arc::clone(&stub),
        Arc::new(FailingDeleteStore::new()) as Arc<dyn CheckpointStore>,
        config(None),
        None,
    )
    .await;

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let held = read_without_acknowledging(&mut first).await;
    let parked_at = settled_partition_count(&stub).await;
    assert!(
        parked_at <= 2 * LANE + 1,
        "A's reads bought the producer one further bound and no more; walked {parked_at}"
    );
    let stranded: HashSet<String> = held
        .iter()
        .map(|(_, checkpoint, _)| match checkpoint {
            Checkpoint::Backfill(backfill) => String::from_utf8_lossy(&backfill.partition.0).into(),
            other => panic!("backfill pages carry backfill checkpoints: {other:?}"),
        })
        .collect();

    // A leaves those pages unacknowledged; B replaces it in the same task step.
    drop(held);
    drop(first);
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // B acknowledges everything it is offered, which is what installs the
    // dangerous resume position: windows past the hole, durably acked.
    let mut partitions_seen: HashSet<String> = HashSet::new();
    let completed = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            partitions_seen.insert(String::from_utf8_lossy(&checkpoint.partition.0).into());
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            if completion {
                return;
            }
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "the scope must reach a completion marker eventually; it stalled at {} windows",
        stub.walked_partitions().len()
    );

    // THE assertion: by the time a completion marker is allowed to stand, the
    // windows the departed consumer stranded must have been offered again.
    assert!(
        stranded.is_subset(&partitions_seen),
        "a retry after lost pages must restart the walk rather than resume past them: \
         stranded {stranded:?}, offered {partitions_seen:?}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// Acknowledge a backfill page, permitting exactly one failure: the refusal a
/// page of a DISCARDED attempt gets.
///
/// A walk that lost pages has its backfill publications fenced, so a page of it
/// still in the consumer's hand when the discard lands is refused - deliberately,
/// since honouring it would rebuild the resume position the discard removed. That
/// is the only tolerated outcome; any other error still fails the test, and a
/// live-lane acknowledgement is not affected by the fence at all, which is why
/// these tests never tolerate a failure there.
async fn ack_page_or_fenced(
    engine: &SyncEngine,
    account_id: &AccountId,
    event: &MultiplexerEvent,
    checkpoint: &bifrost_types::BackfillCheckpoint,
) {
    let outcome = engine
        .ack_checkpoint(
            account_id,
            event.scope.clone(),
            Checkpoint::Backfill(checkpoint.clone()),
            event.publication.clone(),
        )
        .await;
    if let Err(error) = outcome {
        assert!(
            matches!(&error, bifrost_sync::Error::CheckpointStore(message)
                if message.contains("unknown publication")),
            "the only acknowledgement a consumer may be refused here is one naming a \
             publication of a discarded backfill attempt; got {error:?}"
        );
    }
}

/// Whether the store holds a durable completion marker for this scope.
async fn completion_recorded(
    store: &Arc<InMemoryCheckpointStore>,
    account_id: &AccountId,
    scope: &CursorScope,
) -> bool {
    store
        .get_backfill(account_id, scope)
        .await
        .expect("the in-memory store reads back")
        .is_some_and(|checkpoint| checkpoint.partition.0 == b"complete")
}

/// The partition key of a backfill page event.
fn partition_key(event: &MultiplexerEvent) -> Option<String> {
    backfill_checkpoint(event).map(|c| String::from_utf8_lossy(&c.partition.0).into())
}

/// A page's loss can happen AFTER its walk's completion marker was published,
/// and the marker is durable.
///
/// With spare capacity the marker goes out to every receiver at once, so the
/// interleaving needs no gap at all: A holds an unacknowledged page, B subscribes
/// behind it, both are sent the rest of the walk and the marker, and only then
/// does A depart. The sweep retires A's page - B joined the ring after it and can
/// never acknowledge it - while the marker is already sitting in B's ring, and B
/// acknowledges it in good faith. Every check made before that point passed.
///
/// Nothing else the writer looks at can see this: the debt ledger is asked about
/// COVERAGE, and a page that was clean and simply never persisted owes nothing.
/// So the marker carries the walk's watermark and the writer compares it again.
#[tokio::test(start_paused = true)]
async fn a_page_lost_after_the_marker_was_published_is_refused_at_ack_time() {
    let account_id = AccountId("lane-late-loss".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let store = Arc::new(InMemoryCheckpointStore::default());
    // SPARE capacity is the whole point of this interleaving: the marker has to
    // be published while a page is still outstanding, which a bound of `LANE`
    // cannot do - the page A never answers for would park the walk instead. A
    // wider ring for the same reason: nothing here is a slow-consumer test, and B
    // must not lag while it acknowledges.
    let mut spacious = config(None);
    spacious.backfill.lane_capacity = 16;
    spacious.multiplexer.changes_capacity = 128;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store),
        spacious.clone(),
        // One sync permit for the whole engine, so the test can stop the walk
        // between partitions and stage the subscribe precisely.
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let scheduler = engine.scheduler();

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    // One page for A, unacknowledged: the loss this test is about. A is the only
    // receiver that will ever have had it.
    let stranded = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = first.recv().await.expect("the broadcast stays open");
            if let Some(key) = partition_key(&event) {
                return key;
            }
        }
    })
    .await
    .expect("the first page reaches the first consumer");
    // Now stop the walk between partitions, and answer for everything else it
    // published, so exactly ONE charge is outstanding when A eventually leaves.
    let permit = scheduler
        .admit(
            account_id.clone(),
            bifrost_sync::Priority::Normal,
            WorkKind::Sync,
        )
        .await
        .expect("the engine's sync permit is obtainable");
    while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(50), first.recv()).await {
        let Some(checkpoint) = backfill_checkpoint(&event) else {
            continue;
        };
        engine
            .ack_checkpoint(
                &account_id,
                event.scope.clone(),
                Checkpoint::Backfill(checkpoint.clone()),
                event.publication.clone(),
            )
            .await
            .expect("the ack persists");
    }

    // B joins BEHIND that page and carries the walk from here, acknowledging
    // every page it is given, so the walk runs to its completion marker.
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    drop(permit);
    let marker = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            assert_ne!(
                String::from_utf8_lossy(&checkpoint.partition.0),
                stranded,
                "a receiver that joined after a page must not be handed it, or this test \
                 stages no loss at all"
            );
            if checkpoint.partition.0 == b"complete" {
                return (
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                );
            }
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "the walk must reach its completion marker; walked {}",
            stub.walked_partitions().len()
        )
    });

    // Only NOW does A leave, with the marker already in B's hands. The sweep
    // retires the page A never acknowledged; the marker survives, because B is
    // numbered below it and really did receive it.
    drop(first);
    // And B acknowledges the marker it was legitimately given.
    engine
        .ack_checkpoint(&account_id, marker.0, marker.1, marker.2)
        .await
        .expect("the consumer's acknowledgement is honoured; only the marker is withheld");

    assert!(
        !completion_recorded(&store, &account_id, &scope).await,
        "a completion marker acknowledged after one of its walk's pages was lost must \
         not become durable: the next attach would skip the scope, and the page nobody \
         received is then invisible for ever"
    );

    engine.detach(&account_id).await.expect("detach succeeds");

    // And the durable refusal is what a later attach acts on: it must walk again.
    let replacement = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&replacement),
        Arc::clone(&store),
        spacious,
        None,
    )
    .await;
    let mut third = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let rewalked = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = third.recv().await.expect("the broadcast stays open");
            if backfill_checkpoint(&event).is_some() {
                return;
            }
        }
    })
    .await;
    assert!(
        rewalked.is_ok(),
        "the re-attach must re-walk the scope; it walked {} partitions",
        replacement.walked_partitions().len()
    );
    engine.detach(&account_id).await.expect("detach succeeds");
}

/// An ack-time refusal has to REOPEN the incarnation, not merely decline to
/// write a row.
///
/// The orchestrator settles the scope in the same step that publishes the
/// marker, and the loss that voids both lands afterwards. So when the writer
/// refuses the marker there is a scope that is `Completed` in the registry,
/// settled in the rescan, and carrying rows that point past a hole - and nothing
/// in memory heard the refusal. The scope is never walked again this attachment,
/// and an `OpenPages` re-attach resumes beyond the hole.
///
/// Both halves are asserted: the walk happens AGAIN inside the attachment (the
/// rescan compares the watermark it settled under), and a fresh attach over the
/// same store starts from the beginning (the writer dropped the rows and fenced
/// the attempt that wrote them).
#[tokio::test(start_paused = true)]
async fn an_ack_time_refusal_reopens_the_incarnation_and_the_next_attach() {
    let account_id = AccountId("lane-reopen".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(open_pages_stub(&scope));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let mut spacious = config(None);
    spacious.backfill.lane_capacity = 16;
    spacious.multiplexer.changes_capacity = 128;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store),
        spacious.clone(),
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let scheduler = engine.scheduler();

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let stranded = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = first.recv().await.expect("the broadcast stays open");
            if let Some(key) = partition_key(&event) {
                return key;
            }
        }
    })
    .await
    .expect("the first page reaches the first consumer");
    let permit = scheduler
        .admit(
            account_id.clone(),
            bifrost_sync::Priority::Normal,
            WorkKind::Sync,
        )
        .await
        .expect("the engine's sync permit is obtainable");
    while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(50), first.recv()).await {
        if let Some(checkpoint) = backfill_checkpoint(&event) {
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    }

    // B joins behind the stranded page and carries the walk to its marker,
    // acknowledging every window - which installs the resume position past the
    // hole that the reopen has to undo.
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    drop(permit);
    let marker = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            if checkpoint.partition.0 == b"complete" {
                return (
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                );
            }
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    })
    .await
    .expect("the walk reaches its completion marker");
    let walked_at_marker = stub.walked_partitions().len();

    // The loss lands after the publication, and B acknowledges the marker in good
    // faith. The writer refuses it - and that refusal has to travel.
    drop(first);
    engine
        .ack_checkpoint(&account_id, marker.0, marker.1, marker.2)
        .await
        .expect("the consumer's acknowledgement is honoured");
    assert!(
        !completion_recorded(&store, &account_id, &scope).await,
        "the marker must not be durable"
    );

    // Half one: the scope is walked AGAIN, inside this attachment.
    let rewalked = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            if backfill_checkpoint(&event).is_some()
                && stub.walked_partitions().len() > walked_at_marker
            {
                return;
            }
        }
    })
    .await;
    assert!(
        rewalked.is_ok(),
        "an incarnation whose marker was refused must be reopened and re-walked; it \
         stopped at {} windows",
        stub.walked_partitions().len()
    );
    // Nothing of the retry is acknowledged, so the durable footprint under test
    // below is the refused walk's alone.
    engine.detach(&account_id).await.expect("detach succeeds");
    drop(second);

    // Half two: a fresh attachment starts from the beginning rather than past the
    // hole, because the writer dropped the rows the refused walk left.
    let resumed = Arc::new(open_pages_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&resumed),
        Arc::clone(&store),
        spacious,
        None,
    )
    .await;
    let mut third = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let mut offered: HashSet<String> = HashSet::new();
    let _ = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = third.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            offered.insert(String::from_utf8_lossy(&checkpoint.partition.0).into());
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                return;
            }
        }
    })
    .await;
    assert!(
        offered.contains(&stranded),
        "and the next attach must re-offer the window nobody received: offered {offered:?}, \
         stranded {stranded}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The abandonment path records its losses too.
///
/// The bound stops a cold start overrunning the ring on its own, but both lanes
/// share that ring and the live lane has no bound: a burst can still overrun a
/// consumer holding backfill pages it has received and not yet acknowledged, and
/// destroy the acknowledgeability of every registration on the account. When it
/// does, `ChangesReceiver::recv` reports the lag
/// and `abandon_checkpoints` drains the account's registrations - pages the
/// consumer will never answer for, which is the same fact the drop sweep and the
/// retire-on-sentinel-only path record. It was the one producer of that fact that
/// did not record it, so a walk could lose pages this way and still hand its
/// completion marker to the store.
#[tokio::test(start_paused = true)]
async fn pages_lost_to_a_lag_withhold_the_completion_marker() {
    let account_id = AccountId("lane-lag-withholds".to_owned());
    let scope = CursorScope::Account;
    let mut stub = paged_stub(&scope);
    // A live batch on every poll: the burst that overruns the ring past the
    // backfill pages the consumer received and never acknowledged.
    stub.changes_hook = Some(Arc::new(|_cursor| {
        vec![SyncEvent::Batch(Batch {
            items: vec![Change::ObjectChange(bifrost_types::ObjectChange {
                id: ObjectId("live".into()),
                kind: bifrost_types::ObjectChangeKind::Updated,
            })],
            page_boundary: PageBoundary::Final,
            server_latency: Duration::ZERO,
            bytes_in: 0,
            checkpoint: None,
        })]
    }));
    let stub = Arc::new(stub);
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store),
        config(Some(Duration::from_millis(50))),
        None,
    )
    .await;

    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    // The consumer RECEIVES `LANE` pages and answers for none of them. They are
    // read, not unread, and deliberately so: what makes them strandable is the
    // missing acknowledgement, and the live burst below is what takes away any
    // chance of one. Under the receipt bound the reading also frees the producer
    // for a further `LANE` pages, which is immaterial here - the walk is not
    // what this test measures.
    let held = read_without_acknowledging(&mut events).await;
    let stranded: HashSet<String> = held
        .iter()
        .map(|(_, checkpoint, _)| match checkpoint {
            Checkpoint::Backfill(backfill) => String::from_utf8_lossy(&backfill.partition.0).into(),
            other => panic!("backfill pages carry backfill checkpoints: {other:?}"),
        })
        .collect();
    drop(held);

    // Now it stops reading entirely while the live lane keeps publishing. Paused
    // time makes this exact rather than hopeful: each tick is one live batch, and
    // the ring is `RING` slots.
    tokio::time::sleep(Duration::from_millis(50 * (RING as u64 + 4))).await;

    // Reading again reports the lag, which is what abandons the outstanding
    // backfill registrations - the stranded pages among them.
    let mut lagged = false;
    let mut walked_at_completion = 0_usize;
    let completed = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = events.recv().await.expect("the broadcast stays open");
            if matches!(event.event.as_ref(), SyncEvent::Warning(w)
                if w.kind == WarningKind::ChangeStreamLagged)
            {
                lagged = true;
                continue;
            }
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            if completion {
                walked_at_completion = stub.walked_partitions().len();
                return;
            }
        }
    })
    .await;
    assert!(
        lagged,
        "the live burst must actually overrun the ring, or this test stages nothing"
    );
    assert!(
        completed.is_ok(),
        "the scope must reach a completion marker eventually; it walked {} partitions",
        stub.walked_partitions().len()
    );

    // The measurement is the same one the other loss tests use: a marker earned
    // by a single pass arrives at `PARTITIONS`, one earned by a re-walk beyond it.
    // The pages this walk lost were lost to the ABANDONMENT, which is the one
    // producer of that fact that used to stay silent.
    assert!(
        walked_at_completion > PARTITIONS as usize,
        "pages abandoned to a lag reached nobody, so the walk that published them may not \
         record a durable completion: the marker arrived after only {walked_at_completion} \
         of {PARTITIONS} partitions, with {stranded:?} never re-offered"
    );
    assert!(
        completion_recorded(&store, &account_id, &scope).await,
        "and the re-walk that did deliver everything may record one"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The LIVE driver refuses a backfill checkpoint too, exactly as the fusion path
/// does.
///
/// Same structural reason in both: the driver registers the publication and then
/// sends on the raw sender, so the entry is never stamped with a delivery - and
/// an unsent stamp is deliberately never swept, leaving a `Lane::Backfill` entry
/// charged against the account's backfill bound until an acknowledgement, a lag
/// or a reset frees it. Worse for the completion partition specifically: nothing
/// ever stamped that publication with a walk reading, so its acknowledgement
/// would reach `Unvouchable` in this very attachment. Latent - no provider here
/// emits one - and refused because refusing costs nothing and the alternative
/// narrows every later cold start.
#[tokio::test(start_paused = true)]
async fn the_live_driver_refuses_a_backfill_checkpoint() {
    let account_id = AccountId("lane-live-backfill-cp".to_owned());
    let scope = CursorScope::Account;
    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    let hook_scope = scope.clone();
    stub.changes_hook = Some(Arc::new(move |_cursor| {
        vec![SyncEvent::Batch(Batch {
            items: Vec::new(),
            page_boundary: PageBoundary::Final,
            server_latency: Duration::ZERO,
            bytes_in: 0,
            checkpoint: Some(Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
                scope: hook_scope.clone(),
                partition: bifrost_types::Partition(b"complete".to_vec()),
                progress_marker: None,
                progress: bifrost_types::BackfillProgress::default(),
                envelope_version: 1,
            })),
        })]
    }));
    let engine = attach(
        &account_id,
        Arc::new(stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(Some(Duration::from_millis(50))),
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let terminated = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let event = events.recv().await.expect("the broadcast stays open");
            match event.event.as_ref() {
                SyncEvent::Terminated(error) => {
                    // The MESSAGE, not merely the classification. The two contract
                    // violations share a kind and a terminal outcome, so nothing
                    // but the detail distinguishes them - and they were silently
                    // swapped for a round, sending whoever read the log after a
                    // boundary problem that did not exist.
                    let chain = format!("{:?}", error.chain());
                    assert!(
                        chain.contains("backfill checkpoint"),
                        "the refusal must say what actually happened: {chain}"
                    );
                    return;
                }
                SyncEvent::Batch(batch) => assert!(
                    batch.checkpoint.is_none(),
                    "a backfill checkpoint must never be published from the live driver: \
                     it is charged against the bound and nothing will ever sweep it"
                ),
                _ => {}
            }
        }
    })
    .await;
    assert!(
        terminated.is_ok(),
        "the scope must be terminated rather than the batch published"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// One walk's end drains ONE scope's request, observed at the CALL SITE.
///
/// The ledger method being per scope is not the same claim as the walk-end call
/// site using it: reinstating a drain of every outstanding request there leaves a
/// method-level test green. What is observable from outside the engine is the
/// DELETE the writer performs, so this records them.
///
/// The staging is the one the finding describes: a consumer holds ONE page of
/// each scope unanswered while acknowledging the rest, so when it departs the
/// sweep strands a page of each - two outstanding requests, one of them belonging
/// to a scope whose walk has already ended. The next walk end must delete its own
/// scope's rows and nobody else's. No virtual time is allowed to pass between
/// that walk end and the assertion, so the other scope's own rescan cannot have
/// run: any second delete at that point came from the drain taking a request that
/// was not its to take.
#[tokio::test(start_paused = true)]
async fn a_walk_end_drain_deletes_only_its_own_scopes_rows() {
    let account_id = AccountId("lane-two-scope-drain".to_owned());
    let first_scope = CursorScope::Account;
    let second_scope = CursorScope::Type(bifrost_types::ObjectType::Email);
    let mut stub = paged_stub(&first_scope);
    stub.scopes = vec![first_scope.clone(), second_scope.clone()];
    // The default plan is long (`PARTITIONS` windows per scope), which is what
    // leaves the SECOND scope's walk still in flight when the consumer departs.
    let stub = Arc::new(stub);
    let store = Arc::new(RecordingDeleteStore::new());
    let mut config = config(None);
    config.backfill.lane_capacity = 4;
    config.multiplexer.changes_capacity = 128;
    let engine = attach_over(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store) as Arc<dyn CheckpointStore>,
        config,
        None,
    )
    .await;

    // Hold the first page of each scope, answer for everything else, and run
    // until both scopes have published - by which point the first scope's walk
    // has run to its end.
    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let mut held: Vec<String> = Vec::new();
    let mut acked_on_the_second = 0_usize;
    let _ = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = first.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let key = format!("{:?}", event.scope);
            if !held.contains(&key) {
                // The first page of this scope: left unanswered, so the departure
                // sweep has something to strand for it.
                held.push(key);
                continue;
            }
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            // Stop a few pages into the SECOND scope's walk, so that walk is still
            // in flight when the consumer leaves.
            if held.len() == 2 && key == held[1] {
                acked_on_the_second += 1;
                if acked_on_the_second >= 3 {
                    return;
                }
            }
        }
    })
    .await;
    assert_eq!(held.len(), 2, "both scopes must have published pages");
    assert!(
        store.deleted().is_empty(),
        "nothing has been lost yet, so nothing may have been discarded"
    );

    // The consumer departs, stranding a page of EACH scope, and a replacement
    // takes over so the walk in flight can finish.
    drop(first);
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // Drive to the first walk-end drain WITHOUT letting virtual time pass, so no
    // rescan can reopen the other scope and discard on its own account.
    let mut reached = false;
    for _ in 0..4000 {
        while let Ok(event) = second.try_recv() {
            if let Some(checkpoint) = backfill_checkpoint(&event) {
                ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            }
        }
        if !store.deleted().is_empty() {
            reached = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        reached,
        "a walk must reach its end and drain its own request; walked {}",
        stub.walked_partitions().len()
    );
    // Keep going for a few rounds past the first delete. A drain that took every
    // outstanding request issues its second delete on a later poll, so stopping
    // at the first one makes the count below depend on scheduling order rather
    // than on the rule. Still no virtual time: the settled scope's own rescan
    // cannot run in here.
    for _ in 0..64 {
        while let Ok(event) = second.try_recv() {
            if let Some(checkpoint) = backfill_checkpoint(&event) {
                ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            }
        }
        tokio::task::yield_now().await;
    }

    let deletes = store.deleted();
    assert_eq!(
        deletes.len(),
        1,
        "exactly the ending walk's own scope is discarded; the other scope's request \
         is left for its own repair or for teardown. deletes: {deletes:?}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// A backfill PAGE acknowledgement replayed across a detach must not rebuild the
/// resume position the discard removed.
///
/// `Unvouchable` covers the completion marker; a page took the ordinary route -
/// claim lookup, receipt fallback, `apply_transition`, row written. That row is
/// not a position to re-read from but the position the next WALK starts at, so
/// replaying one from a prior attachment hands the fresh walk a place to begin
/// beyond a hole: an `OpenPages` plan then takes its empty end probe, lands a
/// marker, and never offers the stranded window again.
///
/// The interleaving needs nothing unusual. The consumer holds an unflushed page
/// acknowledgement when the account detaches; on reattach the orchestrator parks
/// on the subscriber gate, which is exactly when a returning consumer subscribes
/// and flushes what it never got to send - before `get_backfill` has been read.
#[tokio::test(start_paused = true)]
async fn a_page_ack_replayed_across_a_detach_does_not_rebuild_the_resume_position() {
    let account_id = AccountId("lane-page-replay".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(open_pages_stub(&scope));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store),
        config(None),
        None,
    )
    .await;

    // A consumer takes the first windows and keeps ONE far-along page
    // unacknowledged - the ack it will replay after the reattach.
    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let held = read_without_acknowledging(&mut first).await;
    let stranded: HashSet<String> = held
        .iter()
        .map(|(_, checkpoint, _)| match checkpoint {
            Checkpoint::Backfill(backfill) => String::from_utf8_lossy(&backfill.partition.0).into(),
            other => panic!("backfill pages carry backfill checkpoints: {other:?}"),
        })
        .collect();
    drop(held);
    drop(first);

    // A replacement carries the walk on and acknowledges a few windows past the
    // hole; the last one it receives is kept back, unflushed.
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let mut unflushed: Option<(CursorScope, Checkpoint, bifrost_sync::PublicationId)> = None;
    let mut acked = 0_usize;
    tokio::time::timeout(Duration::from_secs(60), async {
        while acked < 6 {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            acked += 1;
            if acked == 6 {
                unflushed = Some((
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone().expect("checkpointed publication"),
                ));
                return;
            }
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
        }
    })
    .await
    .expect("the walk carries on for the replacement");
    let unflushed = unflushed.expect("one page is kept back");

    engine.detach(&account_id).await.expect("detach succeeds");
    drop(second);

    // A fresh attachment. The returning consumer flushes the acknowledgement it
    // was holding while the orchestrator is still parked on the subscriber gate.
    let resumed = Arc::new(open_pages_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&resumed),
        Arc::clone(&store),
        config(None),
        None,
    )
    .await;
    engine
        .ack_checkpoint(&account_id, unflushed.0, unflushed.1, Some(unflushed.2))
        .await
        .expect("the consumer's replayed acknowledgement is honoured");
    let mut third = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let mut offered: HashSet<String> = HashSet::new();
    let _ = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = third.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            offered.insert(String::from_utf8_lossy(&checkpoint.partition.0).into());
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            if completion {
                return;
            }
        }
    })
    .await;
    assert!(
        stranded.is_subset(&offered),
        "a page acknowledgement from a prior attachment must not become the fresh walk's \
         starting position: stranded {stranded:?}, offered {offered:?}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// A loss observed while NO walk is running is settled by `detach` itself.
///
/// This is the case the teardown drain exists for, and the only one that reaches
/// it. The walk has already ended - marker durable, scope settled - when the
/// consumer holding an earlier page departs, so there is no walk-end drain left
/// to run, and the rescan that would reopen the incarnation never gets to run
/// either: the detach lands first. The orchestrator is idle between rescans and
/// simply returns on the shutdown token, which is exactly the shape the drain was
/// written for and exactly what `a_detach_between_the_loss_and_the_walks_end_still_drops_the_rows`
/// does NOT exercise.
///
/// Without the drain the store keeps a durable completion marker over a page
/// nobody received, and every later attach answers `Skip`.
#[tokio::test(start_paused = true)]
async fn a_loss_with_no_walk_running_is_settled_by_detach() {
    let account_id = AccountId("lane-idle-loss-detach".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(open_pages_stub(&scope));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let mut spacious = config(None);
    spacious.backfill.lane_capacity = 16;
    spacious.multiplexer.changes_capacity = 128;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store),
        spacious.clone(),
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let scheduler = engine.scheduler();

    // A takes one page and never answers for it.
    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let stranded = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = first.recv().await.expect("the broadcast stays open");
            if let Some(key) = partition_key(&event) {
                return key;
            }
        }
    })
    .await
    .expect("the first page reaches the first consumer");
    let permit = scheduler
        .admit(
            account_id.clone(),
            bifrost_sync::Priority::Normal,
            WorkKind::Sync,
        )
        .await
        .expect("the engine's sync permit is obtainable");
    while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(50), first.recv()).await {
        if let Some(checkpoint) = backfill_checkpoint(&event) {
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    }

    // B carries the walk to its end and acknowledges everything, so the marker is
    // legitimately durable and the walk is over.
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    drop(permit);
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                return;
            }
        }
    })
    .await
    .expect("the walk reaches its marker");
    assert!(
        completion_recorded(&store, &account_id, &scope).await,
        "the marker is legitimately durable at this point"
    );

    // NOW the loss, with no walk running - and the detach immediately after it,
    // before the rescan can reopen the incarnation. `drop` sweeps inline, so no
    // orchestrator poll comes between the two.
    drop(first);
    engine.detach(&account_id).await.expect("detach succeeds");
    drop(second);

    assert!(
        !completion_recorded(&store, &account_id, &scope).await,
        "teardown must settle the outstanding discard: the marker stands over a page \
         nobody received, and every later attach would answer Skip"
    );

    // And the next attachment really does walk it again.
    let resumed = Arc::new(open_pages_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&resumed),
        Arc::clone(&store),
        spacious,
        None,
    )
    .await;
    let mut third = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let mut offered: HashSet<String> = HashSet::new();
    let _ = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = third.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            offered.insert(String::from_utf8_lossy(&checkpoint.partition.0).into());
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            if completion {
                return;
            }
        }
    })
    .await;
    assert!(
        offered.contains(&stranded),
        "the fresh attach must re-offer the stranded window: offered {offered:?}, \
         stranded {stranded}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The MIRROR of the test above, and the one that pins the ORDERING rather than
/// the flag: the same departure, moved from just before `detach` to inside it,
/// records nothing.
///
/// `a_departure_during_teardown_records_no_loss` (in `multiplexer::tests`) calls
/// `ChangeDelivery::begin_teardown` directly, so it pins what the sweep does
/// once the flag is set and says nothing about where the flag gets set. Deleting
/// the `begin_teardown` call from `detach_inner`, or moving it below the worker
/// awaits, leaves that test green. This one drives the real `SyncEngine::detach`
/// and drops the receiver while the call is in flight.
///
/// How the window is held open, since detach otherwise runs to completion in one
/// scheduling pass: a second scope establishes via inventory, and the membership
/// refresh that follows the establishment PARKS instead of ending
/// (`memberships_stall`). The deferred-inventory worker is one of the workers
/// `detach` joins and that provider poll has no shutdown arm, so `detach` cannot
/// leave its worker await until the test releases the gate. The departure is
/// therefore inside the window by construction rather than by timing luck, and
/// the release keeps the teardown drain's own budget intact - which matters,
/// because the drain is what settles the discard under the ablation. The
/// `is_finished` assertion below is the guard on that staging, and it is not
/// decorative: a first version of this test parked the CHANGES stream instead,
/// which the multiplexer's own per-scope task owns rather than `detach`, and the
/// detach had already RETURNED by the time the receiver was dropped. It passed
/// both ablations. A parked INVENTORY stream, which a later version used, stopped
/// holding the window when the fusion walk gained its shutdown arm.
///
/// How the window is ENTERED deterministically: `detach_inner` removes the slot,
/// calls `begin_teardown`, publishes `Stop` and cancels the token with NO await
/// between them, so the slot's absence from `engine.accounts` is proof that the
/// flag is already set - under the current ordering. Under either
/// ablation it is proof of the opposite, which is exactly the discrimination
/// this test needs.
///
/// Ablation (both directions): delete the `begin_teardown` call, or move it
/// below the worker awaits, and the departure records the loss and its discard
/// request, the teardown drain settles it against a writer that is still alive,
/// and the durable completion marker is gone - the assertion below fails. The
/// positive control that the staging really can destroy the marker is
/// `a_loss_with_no_walk_running_is_settled_by_detach` above: identical setup,
/// with the drop one step EARLIER.
/// `open_pages_stub`'s walk, cut to `windows` pages.
///
/// The full-length stub needs a lever to hold the walk still while the first
/// consumer finishes draining, and every such lever in this file is the
/// scheduler permit. A SHORT walk removes the need for one: the pages the first
/// consumer leaves unread never reach `lane_capacity`, so the producer runs to
/// its marker with no gating at all.
fn short_open_pages_stub(scope: &CursorScope, windows: u32) -> common::StubAccount {
    let mut stub = common::StubAccount::new(vec![scope.clone()]);
    stub.partitioning = InventoryPartitioning::PageCount {
        total: None,
        page_size: Some(PAGE),
    };
    let end = windows * PAGE;
    stub.partition_hook = Some(Arc::new(move |scope, partition| {
        let from = match partition {
            InventoryPartition::Page { from, .. } => *from,
            other => panic!("the PageCount plan yields page partitions only: {other:?}"),
        };
        let items: Vec<InventoryEntry> = if from >= end {
            Vec::new()
        } else {
            (0..PAGE).map(|n| entry(&format!("s{from}-{n}"))).collect()
        };
        let declared = if from >= end {
            PartitionEnd::Exhausted
        } else {
            PartitionEnd::MoreBeyond
        };
        vec![
            InventoryEvent::Batch(
                InventoryBatch::try_new(
                    items,
                    PageBoundary::Final,
                    Duration::ZERO,
                    0,
                    None,
                    InventoryCoverageReport::complete(CoverageDomain::full(scope.clone())),
                )
                .expect("a Final page with no checkpoint is boundary-valid"),
            ),
            InventoryEvent::Done(InventoryCompletion::complete(
                CoverageDomain::full(scope.clone()),
                None,
                declared,
            )),
        ]
    }));
    stub
}

#[tokio::test(start_paused = true)]
async fn a_departure_inside_a_live_detach_records_no_loss() {
    let account_id = AccountId("lane-departure-in-detach".to_owned());
    let scope = CursorScope::Account;
    let mut stub = short_open_pages_stub(&scope, 4);
    // A SECOND scope, established via inventory, whose follow-up membership
    // refresh parks. That is what holds `detach` inside its worker await: the
    // deferred inventory worker is one of the workers detach joins, and the
    // refresh's provider poll has no shutdown arm. Releasing it right after the
    // departure keeps the teardown drain's own budget intact - which matters,
    // because the drain is what settles the discard under the ablation.
    //
    // A parked CHANGES stream will NOT do, and this was established by
    // measurement, not by reading: the multiplexer owns its per-scope poll
    // tasks and aborts them itself, so they are not among the workers detach
    // waits on, and a first version of this test staged that way found the
    // detach already FINISHED by the time it dropped the receiver - it passed
    // with `begin_teardown` deleted. Nor will a parked inventory stream, which
    // the fusion walk now polls under the slot's shutdown token.
    let parked_scope = CursorScope::Type(bifrost_types::ObjectType::Contact);
    stub.scopes.push(parked_scope.clone());
    stub.establishment = |scope| match scope {
        CursorScope::Type(_) => bifrost_types::CursorEstablishment::EstablishViaInventory,
        other => {
            bifrost_types::CursorEstablishment::Ready(common::cursor_for(other, b"stub-ready"))
        }
    };
    stub.inventory_hook = Some(common::establishing_inventory());
    let stub = Arc::new(stub);
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = Arc::new(
        attach(
            &account_id,
            Arc::clone(&stub),
            Arc::clone(&store),
            // The file's ordinary `LANE`-bounded config. The bound is what holds
            // the walk still while B subscribes: A reads one page and stops, and
            // the producer parks `LANE` publications later with nobody eligible
            // to free it but A.
            config(None),
            // The engine default, deliberately. The neighbouring tests run
            // `global: 1` and hold a permit of their own to keep the walk still;
            // this one needs no such lever, because the short stub replaces it.
            None,
        )
        .await,
    );
    // Armed after attach, which walks memberships itself, and before A
    // subscribes, since the deferred worker walks nothing until a real
    // subscriber exists.
    let stall = Arc::new(tokio::sync::Notify::new());
    let (probe, parked, _destroyed) = common::StallProbe::install();
    *stub.memberships_stall.lock().expect("stall lock") = Some((Arc::clone(&stall), Some(probe)));

    // A takes one page and ACKNOWLEDGES NOTHING, here or later - every page it
    // reads is a strandable hole, which is all this test needs of it. Stopping
    // here parks the producer at the bound, which is what keeps the walk from
    // running past B's subscription: B joins at the ring's tail, so a walk that
    // finished first would leave it nothing to receive at all.
    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = first.recv().await.expect("the broadcast stays open");
            if partition_key(&event).is_some() {
                return;
            }
        }
    })
    .await
    .expect("the first page reaches the first consumer");

    // B joins at the ring's tail - so the page A is holding is unreachable to it
    // for ever - and carries the walk to a legitimately durable completion
    // marker. A is drained (never acknowledged) on each pass, because A is the
    // only receiver eligible to free the pages the producer is parked on; B's
    // reads cannot, having joined behind them.
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            while first.try_recv().is_ok() {}
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                return;
            }
        }
    })
    .await
    .expect("the walk reaches its marker");
    assert!(
        completion_recorded(&store, &account_id, &scope).await,
        "the marker is legitimately durable at this point"
    );

    // The parked worker has to BE parked, or detach has nothing to wait on.
    tokio::time::timeout(Duration::from_secs(120), parked)
        .await
        .expect("the deferred inventory worker parks in its membership refresh")
        .expect("the parked signal is not dropped");

    // Detach, and drop A once the call is demonstrably past the slot removal.
    let detaching = tokio::spawn({
        let engine = Arc::clone(&engine);
        let account_id = account_id.clone();
        async move { engine.detach(&account_id).await }
    });
    // Probed through `open_skipped_scopes` rather than `account_changes_stream`:
    // both answer off `engine.accounts`, but subscribing would add and then drop
    // a numbered receiver on every poll, which is itself a departure and would
    // perturb the ledger this test is reading.
    tokio::time::timeout(Duration::from_secs(5), async {
        while engine.open_skipped_scopes(&account_id).is_ok() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the detach reaches its teardown window");
    assert!(
        !detaching.is_finished(),
        "the whole test rests on detach still being IN its teardown window here; a \
         staging that lets it finish first observes nothing at all"
    );
    // No await between the observation and the drop: the sweep runs inline on
    // this task, so it cannot slip past the drain.
    drop(first);
    // Now let the parked worker go, so the rest of the teardown - the drain in
    // particular - runs with its budget intact.
    stall.notify_waiters();

    tokio::time::timeout(Duration::from_secs(60), detaching)
        .await
        .expect("the detach is clamped, not hung")
        .expect("the detach task does not panic")
        .expect("detach succeeds");
    drop(second);

    assert!(
        completion_recorded(&store, &account_id, &scope).await,
        "a page in a consumer's hands when detach began is the consumer's, exactly as \
         at a crash: the departure sweep is inert from the top of detach, so nothing \
         records a loss and the marker stands"
    );
}

/// A DETACH between the loss and the end of the walk still leaves the rows gone.
///
/// NOTE on what this does and does not pin: it does NOT exercise the teardown
/// drain in `detach`. The producer here is parked in
/// `wait_for_capacity_holding_admission`, so the cancel returns
/// `WaitFailed::ShuttingDown`, the orchestrator treats it as a failed partition,
/// the driver ends the walk normally and the WALK-END drain runs while the
/// orchestrator still holds its own writer sender. What it pins is the request
/// being recorded at the loss rather than sent only at walk end. The teardown
/// drain is pinned by `a_loss_with_no_walk_running_is_settled_by_detach`, where
/// no walk is left to run one.
///
/// All three discard triggers used to run in the attachment that observed the
/// loss, and the orchestrator returns on shutdown from inside its partition loop
/// - so a detach landing between a loss and the walk's end reached none of them.
/// What survived was a set of rows pointing PAST the hole with no marker above
/// them: harmless for a `Fixed` plan, which re-walks everything regardless, and a
/// permanent hole for `OpenPages`, which resumes from exactly those rows. The
/// loss itself therefore REQUESTS the discard, and `detach` settles what is
/// outstanding while the writer is still alive.
#[tokio::test(start_paused = true)]
async fn a_detach_between_the_loss_and_the_walks_end_still_drops_the_rows() {
    let account_id = AccountId("lane-loss-then-detach".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(open_pages_stub(&scope));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store),
        config(None),
        None,
    )
    .await;

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let held = read_without_acknowledging(&mut first).await;
    let parked_at = settled_partition_count(&stub).await;
    assert!(
        parked_at <= 2 * LANE + 1,
        "parked one bound past the pages it read; {parked_at}"
    );
    let stranded: HashSet<String> = held
        .iter()
        .map(|(_, checkpoint, _)| match checkpoint {
            Checkpoint::Backfill(backfill) => String::from_utf8_lossy(&backfill.partition.0).into(),
            other => panic!("backfill pages carry backfill checkpoints: {other:?}"),
        })
        .collect();

    // The consumer departs, stranding those pages, and a replacement takes over
    // and acknowledges the windows that follow - installing the resume position
    // past the hole.
    drop(held);
    drop(first);
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let mut acked = 0_usize;
    tokio::time::timeout(Duration::from_secs(60), async {
        while acked < 4 {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            acked += 1;
        }
    })
    .await
    .expect("the walk carries on for the replacement");

    // And the detach lands mid-walk, before any walk-end discard could run.
    engine.detach(&account_id).await.expect("detach succeeds");
    drop(second);

    let resumed = Arc::new(open_pages_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&resumed),
        Arc::clone(&store),
        config(None),
        None,
    )
    .await;
    let mut third = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let mut offered: HashSet<String> = HashSet::new();
    let _ = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = third.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            offered.insert(String::from_utf8_lossy(&checkpoint.partition.0).into());
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            if completion {
                return;
            }
        }
    })
    .await;
    assert!(
        stranded.is_subset(&offered),
        "the fresh attach must walk from the beginning: the rows the interrupted walk \
         left point past the hole. stranded {stranded:?}, offered {offered:?}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// A loss can land after the marker is not merely published but DURABLE, and the
/// in-memory repair alone does not survive a detach.
///
/// Every check passes honestly here: the walk delivered every page it published,
/// the marker is acknowledged, the row is written. Only afterwards does the
/// consumer holding the first page depart, and the sweep strands it. The rescan
/// reopens the incarnation - that part works - but the ack that would have
/// discarded the rows has already happened, so nothing removes the durable
/// marker; a detach before the retry lands its own leaves the next attach reading
/// that marker back, answering `Skip`, and never re-offering the stranded page.
///
/// The retry therefore discards BEFORE it walks, which is also what makes the
/// reopen robust when the writer's own discard failed.
#[tokio::test(start_paused = true)]
async fn a_loss_after_the_marker_is_durable_still_forces_a_fresh_walk() {
    let account_id = AccountId("lane-late-durable-loss".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(open_pages_stub(&scope));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let mut spacious = config(None);
    spacious.backfill.lane_capacity = 16;
    spacious.multiplexer.changes_capacity = 128;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store),
        spacious.clone(),
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let scheduler = engine.scheduler();

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let stranded = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = first.recv().await.expect("the broadcast stays open");
            if let Some(key) = partition_key(&event) {
                return key;
            }
        }
    })
    .await
    .expect("the first page reaches the first consumer");
    let permit = scheduler
        .admit(
            account_id.clone(),
            bifrost_sync::Priority::Normal,
            WorkKind::Sync,
        )
        .await
        .expect("the engine's sync permit is obtainable");
    while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(50), first.recv()).await {
        if let Some(checkpoint) = backfill_checkpoint(&event) {
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    }

    // B carries the walk to the end and acknowledges everything, the marker
    // included - all of it before any loss, so every check passes and the marker
    // legitimately becomes durable.
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    drop(permit);
    let completed = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                return;
            }
        }
    })
    .await;
    assert!(completed.is_ok(), "the walk must reach its marker");
    assert!(
        completion_recorded(&store, &account_id, &scope).await,
        "and the marker is legitimately durable at this point: nothing had gone wrong yet"
    );
    let walked_at_marker = stub.walked_partitions().len();

    // NOW the first consumer departs, stranding the page it never acknowledged.
    drop(first);

    // The reopen still happens in memory...
    let rewalked = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            if backfill_checkpoint(&event).is_some()
                && stub.walked_partitions().len() > walked_at_marker
            {
                return;
            }
        }
    })
    .await;
    assert!(
        rewalked.is_ok(),
        "the incarnation must be reopened; it stopped at {} windows",
        stub.walked_partitions().len()
    );
    // ...and the retry must have cleared the stale durable marker before walking,
    // or a detach here freezes the scope for every later attach.
    engine.detach(&account_id).await.expect("detach succeeds");
    drop(second);

    let resumed = Arc::new(open_pages_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&resumed),
        Arc::clone(&store),
        spacious,
        None,
    )
    .await;
    let mut third = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let mut offered: HashSet<String> = HashSet::new();
    let _ = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = third.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            offered.insert(String::from_utf8_lossy(&checkpoint.partition.0).into());
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            if completion {
                return;
            }
        }
    })
    .await;
    assert!(
        offered.contains(&stranded),
        "the next attach must re-offer the stranded window rather than reading back a \
         completion marker the loss invalidated: offered {offered:?}, stranded {stranded}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The requirement to restart an incomplete positional walk has to SURVIVE
/// DETACH.
///
/// The in-memory note dies with the attachment while the misleading rows do not:
/// the windows the replacement consumer acknowledged sit past the hole, so a
/// fresh attach resumes beyond it, takes one empty end probe, and completes
/// without ever replaying the missing windows. The rows are therefore dropped
/// durably when a walk loses pages.
#[tokio::test(start_paused = true)]
async fn an_incomplete_positional_walk_restarts_after_a_detach() {
    let account_id = AccountId("lane-openpages-detach".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(open_pages_stub(&scope));
    let store = Arc::new(InMemoryCheckpointStore::default());
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::clone(&store),
        config(None),
        None,
    )
    .await;

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let held = read_without_acknowledging(&mut first).await;
    let parked_at = settled_partition_count(&stub).await;
    assert!(
        parked_at <= 2 * LANE + 1,
        "parked one bound past the pages it read; {parked_at}"
    );
    let stranded: HashSet<String> = held
        .iter()
        .map(|(_, checkpoint, _)| match checkpoint {
            Checkpoint::Backfill(backfill) => String::from_utf8_lossy(&backfill.partition.0).into(),
            other => panic!("backfill pages carry backfill checkpoints: {other:?}"),
        })
        .collect();
    drop(held);
    drop(first);
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // B acknowledges the FIRST walk's remaining windows - installing exactly the
    // dangerous resume position - and stops acknowledging once a retry begins, so
    // the durable rows under test are the first walk's alone.
    let drain = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            if stub.walked_partitions().len() > PARTITIONS as usize {
                // The retry has started; the first walk's durable footprint is
                // whatever it is now.
                return;
            }
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    })
    .await;
    assert!(drain.is_ok(), "the first walk must run out and be retried");

    engine.detach(&account_id).await.expect("detach succeeds");
    drop(second);

    // A fresh attachment over the SAME store. Nothing in memory survives; only
    // the rows do.
    let resumed = Arc::new(open_pages_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&resumed),
        Arc::clone(&store),
        config(None),
        None,
    )
    .await;
    let mut third = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let mut offered: HashSet<String> = HashSet::new();
    let _ = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = third.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            offered.insert(String::from_utf8_lossy(&checkpoint.partition.0).into());
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                return;
            }
        }
    })
    .await;
    assert!(
        stranded.is_subset(&offered),
        "a fresh attach after a walk that lost pages must restart it, not resume past \
         the hole: stranded {stranded:?}, offered {offered:?}"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// A withheld marker must not cost the account its remaining scopes.
///
/// The orchestrator walks the account's scopes in one loop, so an early return on
/// a withheld marker abandons every LATER scope and every later rescan until the
/// account is reattached. The assertion is on BOTH scopes rather than on the
/// second one specifically, because the scan order is not the test's to choose:
/// whichever scope loses pages is re-walked and completes for the consumer that
/// is still there, and whichever does not completes on its first pass. An
/// orchestrator that returns delivers one of them and then nothing.
#[tokio::test(start_paused = true)]
async fn a_withheld_marker_does_not_abandon_the_accounts_other_scopes() {
    let account_id = AccountId("lane-withheld-continues".to_owned());
    let first_scope = CursorScope::Account;
    let second_scope = CursorScope::Type(bifrost_types::ObjectType::Email);
    let mut stub = paged_stub(&first_scope);
    stub.scopes = vec![first_scope.clone(), second_scope.clone()];
    // Two partitions per scope, so a walk reaches its marker at once.
    stub.partitioning = InventoryPartitioning::PageCount {
        total: Some(2 * PAGE),
        page_size: Some(PAGE),
    };
    let stub = Arc::new(stub);
    let mut config = config(None);
    // A bound of ONE makes the marker PARK behind the walk's last page, which is
    // the only way to reach `MarkerOutcome::Withheld` at all: a loss that happens
    // before the emission is caught by the pre-emission reading instead, and the
    // marker is then never emitted.
    config.backfill.lane_capacity = 1;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config,
        None,
    )
    .await;

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    // Read and answer for every page of the first walk but its LAST, which is
    // left UNREAD - the capacity the marker then parks behind. Under the receipt
    // bound an unacknowledged page that the consumer has read frees the lane, so
    // an unread page is what makes the marker park at all.
    let mut pages = 0_usize;
    tokio::time::timeout(Duration::from_secs(20), async {
        while pages < 1 {
            let event = first.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            pages += 1;
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    })
    .await
    .expect("the first walk delivers its pages");
    let parked_at = settled_partition_count(&stub).await;
    assert_eq!(
        parked_at, 2,
        "the walk must have stopped with its marker parked behind the last page rather \
         than moved on, or the withheld arm is never reached"
    );

    // The consumer leaves - the sweep retires that last page and wakes the parked
    // marker, which must now be WITHHELD - and is replaced in the same task step,
    // so the orchestrator has a subscriber to carry on with.
    drop(first);
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let mut completed: HashSet<String> = HashSet::new();
    let both = tokio::time::timeout(Duration::from_secs(120), async {
        while completed.len() < 2 {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            if checkpoint.partition.0 == b"complete" {
                completed.insert(format!("{:?}", checkpoint.scope));
            }
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    })
    .await;
    assert!(
        both.is_ok(),
        "a withheld completion marker must leave the orchestrator RUNNING: the scope \
         that lost a page is re-walked, and the account's other scope is walked at all. \
         completed: {completed:?}, walked: {}",
        stub.walked_partitions().len()
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The RUNNER's retire-on-sentinel-only recording, isolated from the sweep's.
///
/// Both paths bump the watermark, and a staging where a consumer departs holding
/// unacknowledged pages exercises the sweep - so it passes with the runner's call
/// deleted. Here the departing consumer has acknowledged everything it received,
/// so the sweep finds nothing to retire and records nothing; the only pages that
/// reach nobody are the ones the producer publishes into the gap, and only the
/// runner can record those.
///
/// The gap is opened with the SCHEDULER, not with timing: the test takes the
/// account's single sync permit, which the producer needs for each partition, so
/// the walk stops between partitions at a point the test chooses. That is what
/// lets a consumer arrive after the loss and before the completion marker - the
/// interleaving in which an unrecorded loss becomes a durable completion.
#[tokio::test(start_paused = true)]
async fn the_runner_records_pages_that_reached_only_the_sentinel() {
    let account_id = AccountId("lane-runner-records".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        // One sync permit for the whole engine, so taking it stops the walk.
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let scheduler = engine.scheduler();

    let mut first = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    // A acknowledges everything it receives, so it leaves nothing behind for the
    // departure sweep to retire.
    let mut acked = 0_usize;
    tokio::time::timeout(Duration::from_secs(20), async {
        while acked < 3 {
            let event = first.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            acked += 1;
        }
    })
    .await
    .expect("the first consumer takes and answers for several pages");

    // Stop the walk between partitions by taking the permit it needs.
    let permit = scheduler
        .admit(
            account_id.clone(),
            bifrost_sync::Priority::Normal,
            WorkKind::Sync,
        )
        .await
        .expect("the engine's sync permit is obtainable");
    // Drain and answer for anything published while we were queueing, so that the
    // departure below really does sweep nothing.
    while let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(50), first.recv()).await {
        if let Some(checkpoint) = backfill_checkpoint(&event) {
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    }
    let walked_before_the_gap = stub.walked_partitions().len();
    drop(first);

    // The gap: hand the permit back, let the producer take it, and take it again.
    // The yields are what make this deterministic rather than timed - on the
    // current-thread runtime each one hands the producer a step, and the loop
    // stops as soon as it has walked into a channel with no real subscriber at
    // all.
    drop(permit);
    for _ in 0..64 {
        if stub.walked_partitions().len() > walked_before_the_gap {
            break;
        }
        tokio::task::yield_now().await;
    }
    let permit = scheduler
        .admit(
            account_id.clone(),
            bifrost_sync::Priority::Normal,
            WorkKind::Sync,
        )
        .await
        .expect("the permit comes back");
    assert!(
        stub.walked_partitions().len() > walked_before_the_gap,
        "the producer must have walked into the gap, or there is nothing for the runner \
         to record"
    );

    // A consumer arrives AFTER the loss and before the walk ends.
    let mut second = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    drop(permit);

    let mut walked_at_completion = 0_usize;
    let completion = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let event = second.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            ack_page_or_fenced(&engine, &account_id, &event, checkpoint).await;
            if completion {
                walked_at_completion = stub.walked_partitions().len();
                return;
            }
        }
    })
    .await;
    assert!(
        completion.is_ok(),
        "the walk must reach a marker eventually"
    );
    assert!(
        walked_at_completion > PARTITIONS as usize,
        "pages that reached only the sentinel must be recorded BY THE RUNNER: the sweep \
         had nothing to retire here, so an unrecorded loss lets the marker stand at \
         {walked_at_completion} of {PARTITIONS} partitions"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// FINDING 1, end to end. The completion sentinel waits on the same bound as a
/// page, and a version of that wait which re-acquired scheduler admission on the
/// wake left the account's only sync permit held for the rest of the attachment.
///
/// The bite is the SECOND scope. With one sync permit for the whole engine, a
/// leaked admission means the second scope's barrier query and partitions can
/// never be admitted, so its walk simply never happens - silently, with the
/// first scope looking perfectly healthy.
#[tokio::test(start_paused = true)]
async fn a_completed_walk_does_not_strand_the_accounts_admission() {
    let account_id = AccountId("lane-sentinel-admission".to_owned());
    let first = CursorScope::Account;
    let second = CursorScope::Type(bifrost_types::ObjectType::Email);
    let mut stub = paged_stub(&first);
    stub.scopes = vec![first.clone(), second.clone()];
    // Two partitions per scope, so each walk reaches its sentinel quickly.
    stub.partitioning = InventoryPartitioning::PageCount {
        total: Some(2 * PAGE),
        page_size: Some(PAGE),
    };
    let stub = Arc::new(stub);
    let mut config = config(None);
    // A bound of ONE forces the sentinel to PARK: it is emitted after the last
    // page of the walk, which is still unacknowledged at that moment, so the
    // sentinel's wait is a real wait rather than a fast path. Without that this
    // test never exercises the wake at all, and the leak it exists to catch
    // happens on the wake.
    config.backfill.lane_capacity = 1;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config,
        // One sync permit for the entire engine: a stranded admission is fatal
        // rather than merely wasteful.
        Some(ConcurrencyBudget {
            per_account: 2,
            global: 1,
            mutation_share_num: 1,
            mutation_share_den: 4,
        }),
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let mut completed: HashSet<String> = HashSet::new();
    let both = tokio::time::timeout(Duration::from_secs(20), async {
        while completed.len() < 2 {
            let event = events.recv().await.expect("the broadcast stays open");
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            if checkpoint.partition.0 == b"complete" {
                completed.insert(format!("{:?}", checkpoint.scope));
            }
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
        }
    })
    .await;
    assert!(
        both.is_ok(),
        "both scopes must complete; a sentinel that kept the account's only sync \
         permit stops the second one dead. completed: {completed:?}, walked: {}",
        stub.walked_partitions().len()
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// Teardown, first half. A producer parked on lane capacity must not cost
/// `detach` its whole `detach_timeout` and then be aborted mid-partition.
#[tokio::test(start_paused = true)]
async fn detach_does_not_wait_out_a_producer_parked_on_capacity() {
    let account_id = AccountId("lane-detach".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    // Held, never read: the subscriber gate needs a receiver for the walk to
    // start at all, and the unread pages are what park the producer.
    let _events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    // Establish that the producer has actually REACHED the capacity wait rather
    // than merely being between partitions: the walk must have stopped and
    // stayed stopped. Without this the timing assertion below passes against a
    // producer that was never parked at all, which is what it is meant to prove
    // something about. The consumer holds its pages unread, which is what parks
    // it under the receipt bound.
    let _parked_at = park_the_producer_unread(&stub).await;

    let started = tokio::time::Instant::now();
    engine.detach(&account_id).await.expect("detach succeeds");
    let elapsed = started.elapsed();

    // The timeout IS the boundary between draining and aborting: `detach` awaits
    // each worker until the deadline and only then aborts it, so finishing well
    // inside the deadline is what "the parked producer was woken and drained"
    // looks like from outside. Half the deadline, so the assertion is not one
    // slow scheduling tick away from meaningless.
    assert!(
        elapsed * 2 < EngineConfig::default().detach_timeout,
        "detach took {elapsed:?} against a {:?} timeout, which means the parked producer \
         was awaited to the deadline and aborted rather than woken",
        EngineConfig::default().detach_timeout
    );
    // Corroboration that the workers really finished rather than being abandoned
    // mid-flight: teardown reached `Account::close()`, and nothing kept walking
    // afterwards.
    assert_eq!(
        stub.closed.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "teardown must reach the account close"
    );
    let after_detach = stub.walked_partitions().len();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        stub.walked_partitions().len(),
        after_detach,
        "no worker may still be walking partitions after detach returned"
    );
}

/// FINDING 4, end to end. A subscriber that received pages and then dropped can
/// never acknowledge them - and neither can its REPLACEMENT, which joins at the
/// ring's tail and so starts behind those pages.
///
/// The bite is the resubscribe, and it is immediate: it lands well inside the
/// parked producer's one-second liveness tick, so a rule that asks "is anyone
/// subscribed" never observes a gap and releases nothing, forever. The
/// assertions are the two halves that matter - the walk resumes, AND the
/// replacement receiver actually gets the remaining pages rather than the
/// producer running dry into a channel it is not reading.
#[tokio::test(start_paused = true)]
async fn replacing_the_stream_while_the_producer_is_parked_releases_it() {
    let account_id = AccountId("lane-replace".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config(None),
        None,
    )
    .await;
    let events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");
    let parked_at = park_the_producer_unread(&stub).await;

    // Drop and immediately replace, with nothing read and nothing acknowledged.
    drop(events);
    let mut replacement = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    // The replacement must receive real pages, which can only happen if the
    // producer was released.
    // `LANE` pages is what the released producer publishes before parking again,
    // and the replacement's own reads then carry it on from there. Asking for
    // `LANE` is asking only for the release itself, which is the property under
    // test.
    let mut received = 0_usize;
    let delivered = tokio::time::timeout(Duration::from_secs(10), async {
        while received < LANE {
            let event = replacement.recv().await.expect("the broadcast stays open");
            if backfill_checkpoint(&event).is_some() {
                received += 1;
            }
        }
    })
    .await;
    assert!(
        delivered.is_ok(),
        "a producer parked behind a consumer that has been replaced must be released and \
         deliver to the replacement; it stopped at {} of {PARTITIONS} partitions after \
         receiving {received}",
        stub.walked_partitions().len()
    );
    assert!(
        stub.walked_partitions().len() > parked_at,
        "and the walk itself must have moved on"
    );

    engine.detach(&account_id).await.expect("detach succeeds");
}

/// The bound of ONE case: forty consecutive pages, each read and then
/// acknowledged the instant it arrives, so every page in the walk has to free
/// its own capacity before the next one can be published. With a bound of one it
/// is the READ that has to land the release - a page whose receipt goes
/// unrecorded stalls the whole walk immediately.
///
/// SEQUENTIAL, and described as such. It runs on the current-thread runtime under
/// paused time like the rest of this file, so there is no cross-thread race here
/// and this test never claimed to exercise one - it exercises the tightest
/// possible ack-then-publish chain, where a single page whose capacity is not
/// returned stalls the whole walk immediately rather than eventually.
#[tokio::test(start_paused = true)]
async fn an_eagerly_acknowledging_consumer_never_strands_lane_capacity() {
    let account_id = AccountId("lane-eager".to_owned());
    let scope = CursorScope::Account;
    let stub = Arc::new(paged_stub(&scope));
    let mut config = config(None);
    config.backfill.lane_capacity = 1;
    let engine = attach(
        &account_id,
        Arc::clone(&stub),
        Arc::new(InMemoryCheckpointStore::default()),
        config,
        None,
    )
    .await;
    let mut events = engine
        .account_changes_stream(&account_id)
        .expect("attached account has a change stream");

    let mut pages = 0_usize;
    let mut seen: Vec<String> = Vec::new();
    let completed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let event = events.recv().await.expect("the broadcast stays open");
            assert!(
                !matches!(event.event.as_ref(), SyncEvent::Warning(w)
                    if w.kind == WarningKind::ChangeStreamLagged),
                "a bound of one cannot lag an 8-slot ring; a warning here means the \
                 producer escaped the bound rather than that the consumer was slow"
            );
            let Some(checkpoint) = backfill_checkpoint(&event) else {
                continue;
            };
            let completion = checkpoint.partition.0 == b"complete";
            pages += 1;
            seen.extend(batch_ids(&event));
            engine
                .ack_checkpoint(
                    &account_id,
                    event.scope.clone(),
                    Checkpoint::Backfill(checkpoint.clone()),
                    event.publication.clone(),
                )
                .await
                .expect("the ack persists");
            if completion {
                break;
            }
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "a bound of one must not strand capacity against an eager acker; the walk stalled \
         after {pages} pages at {} of {PARTITIONS} partitions",
        stub.walked_partitions().len()
    );

    // A walk that merely "finished" proves little: it has to have delivered
    // everything, once each, and asked the account for each partition once.
    let unique: HashSet<String> = seen.iter().cloned().collect();
    assert_eq!(seen.len(), unique.len(), "no page delivered twice");
    assert_eq!(unique, expected_ids(), "and none lost");
    assert_eq!(pages, PARTITIONS as usize + 1);
    assert_eq!(stub.walked_partitions().len(), PARTITIONS as usize);

    engine.detach(&account_id).await.expect("detach succeeds");
}
