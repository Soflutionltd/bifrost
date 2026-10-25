//! The backfill runner's account-boundary checks on what a partition stream
//! hands it.
//!
//! The runner mints its own positional checkpoints and never takes the
//! account's, but a page or completion whose `PageCheckpoint` contradicts its
//! coverage is an `Account` implementation breaking its stream contract, and
//! nothing else it says - its entries, its `PartitionEnd` - is trustworthy. The
//! partition is terminated as a classified `Internal(AccountContract)` rather
//! than read as an ordinary barrier stop or a clean page.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bifrost_sync::Error;
use bifrost_sync::backfill::{BackfillRunner, LiveSupersedes};
use bifrost_sync::multiplexer::ChangeDelivery;
use bifrost_types::{
    Account, AccountErrorBuilder, AccountErrorKind, Cause, Checkpoint, CoverageDomain, CursorScope,
    DiagnosticText, InternalErrorKind, InventoryBatch, InventoryCompletion,
    InventoryCoverageReport, InventoryEvent, InventoryObligation, InventoryPartition,
    ObligationKey, PageBoundary, PageCheckpoint, PartitionEnd, RegionRecovery, RequestCause,
    RequestErrorKind,
};

use common::{StubAccount, cursor_for};

fn barrier(scope: &CursorScope) -> InventoryCoverageReport {
    let error = AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only("unidentifiable value"),
        }),
    )
    .try_build()
    .expect("valid account error classification");
    InventoryCoverageReport::degraded(
        CoverageDomain::full(scope.clone()),
        vec![InventoryObligation::Region {
            key: ObligationKey(b"blocked-page".to_vec()),
            failure_label: "unidentifiable-value".into(),
            error,
            recovery: RegionRecovery::barrier(),
        }],
    )
}

/// Run one partition against a stub whose partition stream yields `events`.
async fn run(
    events: Vec<InventoryEvent>,
) -> Result<bifrost_sync::backfill::BackfillPartitionOutcome, Error> {
    let scope = CursorScope::Account;
    let mut stub = StubAccount::new(vec![scope.clone()]);
    let events = Arc::new(std::sync::Mutex::new(Some(events)));
    stub.partition_hook = Some(Arc::new(move |_, _| {
        events
            .lock()
            .expect("events lock")
            .take()
            .expect("one partition")
    }));
    let account: Arc<dyn Account> = Arc::new(stub);
    let (tx, _rx) = tokio::sync::broadcast::channel(16);
    let delivery = ChangeDelivery::new(tx);
    let live = LiveSupersedes::new();
    BackfillRunner::run_partition(
        account.as_ref(),
        scope,
        InventoryPartition::Full,
        &live,
        None,
        1,
        None,
        None,
        None,
        0,
        None,
        0,
        &delivery,
    )
    .await
}

/// Run one `Full` partition with a change stream attached, returning the
/// outcome and every warning the runner published.
async fn run_observed(
    events: Vec<InventoryEvent>,
) -> (
    bifrost_sync::backfill::BackfillPartitionOutcome,
    Vec<bifrost_types::Warning>,
) {
    let scope = CursorScope::Account;
    let mut stub = StubAccount::new(vec![scope.clone()]);
    let events = Arc::new(std::sync::Mutex::new(Some(events)));
    stub.partition_hook = Some(Arc::new(move |_, _| {
        events
            .lock()
            .expect("events lock")
            .take()
            .expect("one partition")
    }));
    let account: Arc<dyn Account> = Arc::new(stub);
    let (tx, mut rx) = tokio::sync::broadcast::channel(16);
    let delivery = ChangeDelivery::new(tx.clone());
    let live = LiveSupersedes::new();
    let outcome = BackfillRunner::run_partition(
        account.as_ref(),
        scope,
        InventoryPartition::Full,
        &live,
        Some(tx),
        1,
        None,
        None,
        None,
        0,
        None,
        0,
        &delivery,
    )
    .await
    .expect("the partition runs");
    let mut warnings = Vec::new();
    while let Ok(event) = rx.try_recv() {
        if let bifrost_types::SyncEvent::Warning(warning) = event.event.as_ref() {
            warnings.push(warning.clone());
        }
    }
    (outcome, warnings)
}

/// A fixed-plan partition whose stream ends with neither `Done` nor
/// `Terminated` proved nothing about how far the enumeration got. It must not
/// read as complete - that wrote the scope's completion marker over ground
/// nothing covered, and the next attach skipped the walk for good - and the
/// reason is surfaced as a typed warning.
#[tokio::test]
async fn a_fixed_partition_that_ends_silently_is_incomplete_and_says_so() {
    let (outcome, warnings) = run_observed(Vec::new()).await;
    assert!(
        !outcome.complete,
        "a silent end must not complete the partition"
    );
    assert_eq!(outcome.end, None);
    assert!(
        warnings
            .iter()
            .any(|w| w.kind == bifrost_types::WarningKind::InventoryEndedUnannounced),
        "{warnings:?}"
    );
}

/// The control: a partition that ends in `Done` completes, and says nothing.
#[tokio::test]
async fn a_fixed_partition_that_ends_in_done_completes_without_the_warning() {
    let scope = CursorScope::Account;
    let done = InventoryEvent::Done(InventoryCompletion::complete(
        CoverageDomain::full(scope),
        None,
        PartitionEnd::Exhausted,
    ));
    let (outcome, warnings) = run_observed(vec![done]).await;
    assert!(outcome.complete);
    assert!(
        warnings
            .iter()
            .all(|w| w.kind != bifrost_types::WarningKind::InventoryEndedUnannounced),
        "{warnings:?}"
    );
}

fn assert_contract_violation(
    outcome: Result<bifrost_sync::backfill::BackfillPartitionOutcome, Error>,
    what: &str,
) {
    match outcome {
        Err(Error::Account(error)) => assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::AccountContract),
            "{what}"
        ),
        other => panic!("{what}: expected a classified contract violation, got {other:?}"),
    }
}

/// A page labelled `Advance` over coverage holding a barrier.
#[tokio::test]
async fn a_page_whose_checkpoint_disagrees_with_its_coverage_fails_the_partition() {
    let scope = CursorScope::Account;
    let page = InventoryEvent::Batch(InventoryBatch {
        removed: Vec::new(),
        items: Vec::new(),
        page_boundary: PageBoundary::Page,
        server_latency: Duration::ZERO,
        bytes_in: 0,
        checkpoint: PageCheckpoint::Advance(Checkpoint::Change(cursor_for(&scope, b"page"))),
        coverage: barrier(&scope),
    });
    assert_contract_violation(run(vec![page]).await, "a mislabelled page");
}

/// A completion labelled `Withheld` over clean coverage. Its `end`
/// declaration is not read: a completion that contradicts itself has said
/// nothing the walk can act on.
#[tokio::test]
async fn a_completion_whose_checkpoint_disagrees_with_its_coverage_fails_the_partition() {
    let scope = CursorScope::Account;
    let done = InventoryEvent::Done(InventoryCompletion {
        checkpoint: PageCheckpoint::Withheld(None),
        coverage: InventoryCoverageReport::complete(CoverageDomain::full(scope)),
        end: PartitionEnd::Exhausted,
    });
    assert_contract_violation(run(vec![done]).await, "a mislabelled completion");
}
