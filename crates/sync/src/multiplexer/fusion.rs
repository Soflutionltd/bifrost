//! Inventory-fusion path.
//!
//! For `EstablishViaInventory` scopes (Graph all scopes,
//! IMAP-Basic / IMAP-CONDSTORE-only folders) the inventory walk IS the
//! cursor establishment. The multiplexer consumes the inventory stream
//! and registers the cursor exactly once, on the inventory's terminal
//! `Done` event. Mid-stream `Batch` checkpoints (if any) advance the
//! inventory progress marker only; the change cursor is established in
//! memory when the inventory's terminal `Done` arrives. Durable cursor
//! persistence still waits for the consumer ack of the terminal
//! checkpoint.
//!
//! Inventory data: every `Batch` is forwarded to the per-account
//! broadcast as an `ObjectChange::Created` signal before the cursor
//! is persisted, so cold-start inventory (Graph all scopes,
//! IMAP-Basic / CONDSTORE-only folders) does not vanish.

use std::sync::Arc;

use bifrost_types::{Account, AccountError, Checkpoint, CursorScope, SyncEvent};
use futures::stream::StreamExt;

#[cfg(test)]
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::delivery::InventoryPublication as Forwarded;
use crate::error::Error;
use crate::inventory_walk::{InventoryWalk, PageVerdict};

use super::{ChangeDelivery, EventScope, MultiplexerEvent};

/// Outcome of an inventory fusion pass.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FusionOutcome {
    /// Inventory completed and the in-memory cursor was established.
    Established,
    /// A reset or deletion of the scope closed while the walk ran; no
    /// checkpoint was published, no batch was forwarded and no cursor was
    /// installed after the walk observed it. (A checkpoint-less batch forwarded
    /// between the fence raise and the walk's next comparison did go out; it
    /// makes nothing durable.) The engine broadcasts a scoped warning.
    Invalidated,
    /// Inventory completed without yielding a cursor; the engine emits
    /// a Warning.
    NoCursor,
    /// A page reached no numbered receiver. No terminal cursor was installed;
    /// the owner must arrange another establishment, not treat this as Done.
    Undelivered,
    /// Inventory terminated with an account error. Carries the full
    /// `AccountError` so the caller dispatches through
    /// `error.recovery()`.
    Terminated(AccountError),
}

pub struct InventoryFusion {
    /// Snapshot before any await belonging to this walk.
    pub expected_fence: crate::cursor::ScopeFence,
    pub account_id: bifrost_types::AccountId,
    pub cursors: Arc<CursorRegistry>,
    pub publisher: super::PagePublisher,
    pub generation: crate::cursor::WalkGeneration,
}

impl InventoryFusion {
    /// Drive an `inventory_stream(scope)` to completion. On the
    /// terminal `Done`, register the cursor carried in its checkpoint.
    /// Every batch is forwarded through the required publisher; mid-stream
    /// checkpoints do not establish the live change cursor.
    pub async fn run(
        &self,
        account: &dyn Account,
        scope: CursorScope,
    ) -> Result<FusionOutcome, Error> {
        self.run_with_broadcast(account, scope).await
    }

    /// Same contract as `run`: each `Batch` of inventory entries
    /// is fanned out on the account's change delivery as
    /// `Change::ObjectChange::Created` events BEFORE the terminal cursor is
    /// acknowledged. This is the only path inventory data flows to consumers
    /// for `EstablishViaInventory` scopes - dropping it would lose every
    /// Graph object and every IMAP-Basic / CONDSTORE-only folder's
    /// cold-start contents.
    ///
    /// The delivery gate rather than the raw sender, because every
    /// checkpoint-bearing page this walk publishes registers a claim that only
    /// a NUMBERED receiver can answer for, and only the gate knows whether one
    /// took delivery.
    pub async fn run_with_broadcast(
        &self,
        account: &dyn Account,
        scope: CursorScope,
    ) -> Result<FusionOutcome, Error> {
        let stream = account.inventory_stream(scope.clone());
        self.run_stream(scope, stream).await
    }

    /// Resume a provider-owned inventory page cursor. The account returns
    /// `None` for a cursor it does not own, so a stale or cross-provider row
    /// cannot be accidentally reinterpreted as a fresh inventory walk.
    pub async fn run_resume_with_broadcast(
        &self,
        account: &dyn Account,
        cursor: bifrost_types::ChangeCursor,
    ) -> Result<FusionOutcome, Error> {
        let scope = cursor.scope.clone();
        let classified_as_inventory = account.is_inventory_cursor(&cursor);
        let stream = account.inventory_resume_stream(cursor);
        self.run_resume_stream_with_broadcast(scope, classified_as_inventory, stream)
            .await
    }

    /// Resume a cursor after its classifier and stream have already been
    /// queried by an establishment caller. Keeping the check here ensures an
    /// invocation routes either disagreement through scoped recovery.
    /// Callers that gate on the classifier alone can still bypass this guard
    /// when the stream hook returns `Some` for an unclassified cursor.
    pub(crate) async fn run_resume_stream_with_broadcast(
        &self,
        scope: CursorScope,
        classified_as_inventory: bool,
        stream: Option<bifrost_types::AccountStream<bifrost_types::InventoryEvent>>,
    ) -> Result<FusionOutcome, Error> {
        let stream =
            match inventory_resume_stream_checked(scope.clone(), classified_as_inventory, stream) {
                Ok(stream) => stream,
                Err(error) => {
                    // Route a bad resume cursor through the ordinary scoped
                    // recovery path. Returning `Err` here made the deferred attach
                    // worker log and exit with no registered cursor to poll again.
                    return Ok(forward_resume_contract_failure(
                        &scope,
                        error,
                        &self.publisher.delivery,
                    ));
                }
            };
        let Some(stream) = stream else {
            return Ok(FusionOutcome::NoCursor);
        };
        self.run_stream(scope, stream).await
    }

    async fn run_stream(
        &self,
        scope: CursorScope,
        mut stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent>,
    ) -> Result<FusionOutcome, Error> {
        let delivery = &self.publisher.delivery;
        let _activity = self
            .publisher
            .control
            .begin_activity()
            .ok_or(Error::Paused)?;
        // The last checkpoint this walk published. If a later page turns out to
        // be a barrier, this is the position a future walk resumes from - it
        // certifies a prefix ending before the barrier region begins.
        let mut walk = InventoryWalk::default();
        loop {
            // The shutdown arm wraps the provider poll and nothing else. Every
            // other await in this loop - barrier recording, waived-barrier
            // crossing - runs inside an arm, and a select cannot destroy a
            // branch mid-poll, so a page's claim registration and its publish,
            // which are synchronous, cannot be split. What the cut drops is the
            // provider stream and the activity guard, which is the point.
            //
            // An ERROR rather than `NoCursor`, for the same reason the backfill
            // runner returns one: `NoCursor` is a completed walk that yielded no
            // cursor, and callers treat it as a finished establishment.
            let next = tokio::select! {
                biased;
                () = self.publisher.shutdown.cancelled() => return Err(Error::ShuttingDown),
                next = stream.next() => next,
            };
            let Some(event) = next else { break };
            match event {
                bifrost_types::InventoryEvent::Done(completion) => {
                    // Same contract check as every page: a withheld state that
                    // contradicts the coverage beside it is an account breaking
                    // its stream contract, terminated as a classified violation.
                    if let Err(violation) = completion.validate() {
                        let error = crate::engine_errors::batch_boundary_violation(
                            violation,
                            completion.checkpoint.offered(),
                            bifrost_types::AccountOperation::SyncInventory,
                            &scope,
                        );
                        delivery
                            .publish_terminated(EventScope::Cursor(scope.clone()), error.clone());
                        return Ok(FusionOutcome::Terminated(error));
                    }
                    let verdict = walk
                        .judge(&completion.coverage, &self.publisher, self.generation)
                        .await?;
                    if verdict == PageVerdict::Stop {
                        walk.warn_coverage(delivery, &scope, &completion.coverage, verdict, true);
                        return Ok(FusionOutcome::NoCursor);
                    }
                    let checkpoint = completion.checkpoint.accepted(verdict.crossed()).cloned();
                    validate_checkpoint_envelope(checkpoint.as_ref())?;
                    walk.warn_coverage(delivery, &scope, &completion.coverage, verdict, true);
                    if let Some(cp) = checkpoint.clone() {
                        // Register the claim BEFORE publishing: a fast consumer
                        // can acknowledge between the send and a later
                        // registration, and the writer would then find no claim
                        // for a checkpoint that carried one.
                        let me = MultiplexerEvent {
                            scope: EventScope::Cursor(scope.clone()),
                            event: Arc::new(SyncEvent::Done(Some(cp.clone()))),
                            checkpoint: Some(cp),
                            publication: None,
                        };
                        match self.publisher.publish_page(
                            me,
                            crate::cursor::CoverageClaim::new(
                                completion.coverage.clone(),
                                self.generation,
                            ),
                            self.expected_fence,
                        ) {
                            Ok(true) => {}
                            // Retiring a publication does not authorize installing its
                            // cursor. A user must see this page before sync skips it.
                            Ok(false) => return Ok(FusionOutcome::Undelivered),
                            Err(claim) => return self.refused(&scope, delivery, claim).await,
                        }
                    }
                    let outcome = self.finalize(scope.clone(), checkpoint).await?;
                    if matches!(outcome, FusionOutcome::Invalidated) {
                        Self::warn_invalidated(&scope, delivery);
                    }
                    return Ok(outcome);
                }
                bifrost_types::InventoryEvent::Terminated(err) => {
                    delivery.publish_terminated(EventScope::Cursor(scope.clone()), err.clone());
                    return Ok(FusionOutcome::Terminated(err));
                }
                bifrost_types::InventoryEvent::Batch(batch) => {
                    // Same reasoning as the changes driver: a bare engine
                    // error would leave the establish path retrying an
                    // unchanged position with nothing on the stream to tell
                    // a consumer the walk stopped. Terminate the walk with a
                    // classified provider-contract violation instead.
                    if let Err(violation) = batch.validate_boundary() {
                        let error = crate::engine_errors::batch_boundary_violation(
                            violation,
                            batch.checkpoint.offered(),
                            bifrost_types::AccountOperation::SyncInventory,
                            &scope,
                        );
                        delivery
                            .publish_terminated(EventScope::Cursor(scope.clone()), error.clone());
                        return Ok(FusionOutcome::Terminated(error));
                    }
                    // A barrier TAINTS THE WALK. Refusing only this one
                    // checkpoint is not enough: the next page's checkpoint, or
                    // the terminal delta link, would simply leap over the same
                    // region. So the batch's items are still delivered - they
                    // were really seen - but its withheld checkpoint is not
                    // taken and the walk stops here. Checkpoints already
                    // accepted earlier in this walk stand: each of them
                    // certifies a prefix that ends before this region begins.
                    let verdict = walk
                        .judge(&batch.coverage, &self.publisher, self.generation)
                        .await?;
                    if verdict == PageVerdict::Stop {
                        match self.forward_inventory_batch(&scope, &batch, None) {
                            Ok(Forwarded::Published) => {}
                            Ok(Forwarded::Undelivered) => return Ok(FusionOutcome::Undelivered),
                            Ok(Forwarded::Refused(claim)) => {
                                return self.refused(&scope, delivery, claim).await;
                            }
                            Err(error) => return Ok(FusionOutcome::Terminated(error)),
                        }
                        walk.warn_coverage(delivery, &scope, &batch.coverage, verdict, false);
                        return Ok(FusionOutcome::NoCursor);
                    }
                    let checkpoint = batch.checkpoint.accepted(verdict.crossed()).cloned();
                    validate_checkpoint_envelope(checkpoint.as_ref())?;
                    walk.accept(checkpoint.clone());
                    match self.forward_inventory_batch(&scope, &batch, checkpoint) {
                        Ok(Forwarded::Published) => {}
                        Ok(Forwarded::Undelivered) => return Ok(FusionOutcome::Undelivered),
                        Ok(Forwarded::Refused(claim)) => {
                            return self.refused(&scope, delivery, claim).await;
                        }
                        Err(error) => return Ok(FusionOutcome::Terminated(error)),
                    }
                    walk.warn_coverage(delivery, &scope, &batch.coverage, verdict, false);
                }
                // A producer-emitted warning is forwarded, not absorbed. Graph
                // announces its public-folder over-cap degrade this way (past
                // the cap the folder silently becomes additions-only, so
                // deletions stop propagating), and IMAP announces a QRESYNC ->
                // CONDSTORE strategy downgrade. Dropping them here was worse
                // than losing a message in the IMAP case: that warning sits
                // behind a one-shot `AtomicBool` shared with the changes path,
                // so whichever lane reached it first consumed it, and the
                // other could never re-emit.
                bifrost_types::InventoryEvent::Warning(warning) => {
                    delivery.publish_warning(EventScope::Cursor(scope.clone()), warning);
                }
                bifrost_types::InventoryEvent::Progress(_) => {}
                _ => {}
            }
        }
        // The stream ended with neither `Done` nor `Terminated`: a producer
        // breaking its contract. No cursor is established, and the reason is
        // surfaced, the same way the backfill runner surfaces it.
        delivery.publish_warning(
            EventScope::Cursor(scope.clone()),
            crate::inventory_walk::unannounced_end_warning(),
        );
        Ok(FusionOutcome::NoCursor)
    }

    fn warn_invalidated(scope: &CursorScope, delivery: &Arc<ChangeDelivery>) {
        delivery.publish_warning(
            EventScope::Cursor(scope.clone()),
            bifrost_types::Warning::user_safe(
                bifrost_types::WarningKind::Engine(bifrost_types::EngineNotice::WalkSuperseded),
                "inventory walk superseded by a reset or deletion of its scope",
            ),
        );
    }

    async fn refused(
        &self,
        scope: &CursorScope,
        delivery: &Arc<ChangeDelivery>,
        claim: crate::cursor::CoverageClaim,
    ) -> Result<FusionOutcome, Error> {
        if let Err(error) =
            crate::inventory_walk::record_refused_debt(Some(&self.publisher.writer), claim).await
        {
            tracing::warn!(%error, "could not record refused inventory debt");
        }
        Self::warn_invalidated(scope, delivery);
        Ok(FusionOutcome::Invalidated)
    }

    /// Forward a page's entries, publishing `checkpoint` with them.
    ///
    /// `checkpoint` is the position the caller ACCEPTED, passed separately from
    /// the batch so nothing here can read the batch's own `PageCheckpoint` and
    /// take a withheld position.
    fn forward_inventory_batch(
        &self,
        scope: &CursorScope,
        batch: &bifrost_types::InventoryBatch,
        checkpoint: Option<Checkpoint>,
    ) -> Result<Forwarded, AccountError> {
        self.publisher.publish_inventory(
            scope,
            batch,
            batch.items.iter().map(|entry| &entry.id),
            batch.removed.iter(),
            checkpoint,
            self.generation,
            self.expected_fence,
        )
    }

    async fn finalize(
        &self,
        expected_scope: CursorScope,
        checkpoint: Option<Checkpoint>,
    ) -> Result<FusionOutcome, Error> {
        let Some(Checkpoint::Change(cursor)) = checkpoint else {
            return Ok(FusionOutcome::NoCursor);
        };
        if cursor.scope != expected_scope {
            return Err(Error::InventoryScopeMismatch {
                expected: expected_scope,
                actual: cursor.scope,
            });
        }
        if !self.cursors.put_if(cursor, || {
            self.publisher.coverage.scope_fence(&expected_scope) == self.expected_fence
        }) {
            return Ok(FusionOutcome::Invalidated);
        }
        Ok(FusionOutcome::Established)
    }
}

/// What a fusion batch's checkpoint may be, and what it may not.
///
/// A `Change` cursor is the point of the fusion path and is validated. A
/// `Backfill` checkpoint is REFUSED, and that refusal is the whole reason this
/// arm exists: fusion is a change-cursor establishment walk and has no backfill
/// admission. Sending a backfill checkpoint through the unstamped change path
/// would leave it without a delivery stamp. A `PublicationStream::Backfill` entry whose
/// `delivered_at` stays `None` for ever is charged against the account's backfill
/// bound and can be freed only by an acknowledgement, a lag or a reset - the
/// receiver-drop sweep will not touch it, because an unsent page is deliberately
/// never swept. A provider that attached one to an inventory batch would quietly
/// spend a permanent slot of the bound per scope.
///
/// No provider in this workspace emits one, so refusing costs nothing real, and
/// refusing is the safe direction: the inventory pass fails and is retried,
/// rather than the account's cold start narrowing by one page for the life of
/// the attachment.
fn validate_checkpoint_envelope(checkpoint: Option<&Checkpoint>) -> Result<(), Error> {
    match checkpoint {
        Some(Checkpoint::Change(cursor)) => cursor.validate_envelope().map_err(|_| {
            Error::Account(crate::engine_errors::fresh_cursor_contract_error(
                checkpoint,
                bifrost_types::AccountOperation::SyncInventory,
                &cursor.scope,
            ))
        }),
        Some(Checkpoint::Backfill(_)) => Err(Error::InventoryCheckpointUnsupported),
        None => Ok(()),
        Some(_) => Err(Error::InventoryCheckpointUnsupported),
    }
}

fn inventory_cursor_contract_error(scope: CursorScope) -> bifrost_types::AccountError {
    crate::engine_errors::inventory_resume_contract_failure(scope)
}

fn forward_resume_contract_failure(
    scope: &CursorScope,
    error: bifrost_types::AccountError,
    delivery: &Arc<ChangeDelivery>,
) -> FusionOutcome {
    delivery.publish_terminated(EventScope::Cursor(scope.clone()), error.clone());
    FusionOutcome::Terminated(error)
}

fn inventory_resume_stream_checked(
    scope: CursorScope,
    classified_as_inventory: bool,
    stream: Option<bifrost_types::AccountStream<bifrost_types::InventoryEvent>>,
) -> Result<
    Option<bifrost_types::AccountStream<bifrost_types::InventoryEvent>>,
    bifrost_types::AccountError,
> {
    if classified_as_inventory != stream.is_some() {
        Err(inventory_cursor_contract_error(scope))
    } else {
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The receiving halves of the control's channels. The caller holds them
    /// for the life of the test, so nothing the control sends goes to a closed
    /// channel, and they are dropped with the test rather than leaked.
    type ControlViews = (
        crate::cancel::BoundaryView,
        tokio::sync::watch::Receiver<bifrost_types::Priority>,
        tokio::sync::watch::Receiver<Option<bifrost_types::BandwidthCap>>,
    );

    fn guarded_fusion() -> (
        InventoryFusion,
        Arc<ChangeDelivery>,
        tokio::sync::broadcast::Receiver<MultiplexerEvent>,
        ControlViews,
    ) {
        let coverage = Arc::new(crate::cursor::PendingCoverage::new());
        let (boundary, view) = crate::cancel::Boundary::new();
        let (priority, p) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
        let (bandwidth, b) = tokio::sync::watch::channel(None);
        let control = SyncControl::new_with_publications(
            bifrost_types::AccountId("guarded".into()),
            boundary,
            priority,
            bandwidth,
            Arc::clone(&coverage),
        );
        let (tx, rx) = tokio::sync::broadcast::channel(16);
        let delivery = Arc::new(ChangeDelivery::new(tx));
        let (writer_tx, writer_rx) = tokio::sync::mpsc::channel(4);
        drop(writer_rx);
        (
            InventoryFusion {
                expected_fence: crate::cursor::PendingCoverage::initial_fence(),
                account_id: bifrost_types::AccountId("guarded".into()),
                cursors: Arc::new(CursorRegistry::new()),
                publisher: super::super::PagePublisher::new(
                    Arc::clone(&delivery),
                    control,
                    crate::engine::writer::WriterHandle::new(writer_tx),
                    tokio_util::sync::CancellationToken::new(),
                ),
                generation: crate::cursor::WalkGeneration::for_test(1),
            },
            delivery,
            rx,
            (view, p, b),
        )
    }
    #[tokio::test]
    async fn undelivered_done_does_not_install_a_cursor() {
        let (fusion, _delivery, _observer, _views) = guarded_fusion();
        let stream = Box::pin(futures::stream::iter(vec![clean_done()]));
        let outcome = fusion
            .run_stream(CursorScope::Account, stream)
            .await
            .expect("walk");
        assert!(matches!(outcome, FusionOutcome::Undelivered));
        assert!(fusion.cursors.snapshot(&CursorScope::Account).is_none());
        assert_eq!(fusion.publisher.coverage.pending_checkpoints(), 0);
    }

    #[tokio::test]
    async fn undelivered_prefix_stops_before_reading_done() {
        for checkpoint in [false, true] {
            let (fusion, _delivery, mut observer, _views) = guarded_fusion();
            let stream = Box::pin(futures::stream::iter(vec![
                page(checkpoint, false),
                clean_done(),
            ]));
            let outcome = fusion
                .run_stream(CursorScope::Account, stream)
                .await
                .expect("walk");
            assert!(matches!(outcome, FusionOutcome::Undelivered));
            assert!(fusion.cursors.snapshot(&CursorScope::Account).is_none());
            // An observer alone is not a consumer that can acknowledge. Done
            // must not even be read after the lost prefix, with or without a
            // page checkpoint.
            assert!(matches!(
                observer.try_recv().expect("page").event.as_ref(),
                SyncEvent::Batch(_)
            ));
            assert!(observer.try_recv().is_err());
        }
    }

    fn clean_done() -> bifrost_types::InventoryEvent {
        bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::complete(
            bifrost_types::CoverageDomain::full(CursorScope::Account),
            Some(Checkpoint::Change(crate::test_common::cursor_for(
                &CursorScope::Account,
                b"done",
            ))),
            bifrost_types::PartitionEnd::Exhausted,
        ))
    }
    fn page(checkpoint: bool, degraded: bool) -> bifrost_types::InventoryEvent {
        bifrost_types::InventoryEvent::Batch(bifrost_types::InventoryBatch {
            items: vec![bifrost_types::InventoryEntry {
                id: bifrost_types::ObjectId("observed".into()),
                memberships: Vec::new(),
                blob_id: None,
                fingerprint: bifrost_types::Fingerprint {
                    server_version: bifrost_types::ServerVersion::Unavailable,
                    size: None,
                    flags_hash: 0,
                },
                thread_id: None,
                message_id: None,
                references: Vec::new(),
                in_reply_to: None,
            }],
            removed: Vec::new(),
            page_boundary: bifrost_types::PageBoundary::Page,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: if checkpoint {
                bifrost_types::PageCheckpoint::Advance(Checkpoint::Change(
                    crate::test_common::cursor_for(&CursorScope::Account, b"page"),
                ))
            } else {
                bifrost_types::PageCheckpoint::None
            },
            coverage: if degraded {
                bifrost_types::InventoryCoverageReport::degraded(
                    bifrost_types::CoverageDomain::full(CursorScope::Account),
                    vec![bifrost_types::InventoryObligation::Object {
                        key: bifrost_types::ObligationKey(b"owed".to_vec()),
                        id: bifrost_types::ObjectId("owed".into()),
                        error: inventory_cursor_contract_error(CursorScope::Account),
                        repair: Vec::new(),
                    }],
                )
            } else {
                bifrost_types::InventoryCoverageReport::complete(
                    bifrost_types::CoverageDomain::full(CursorScope::Account),
                )
            },
        })
    }
    #[tokio::test]
    async fn degraded_pages_warn_once_before_an_unannounced_end() {
        let (fusion, delivery, mut observer, _views) = guarded_fusion();
        let _receiver = delivery.subscribe(None, Arc::clone(&fusion.publisher.coverage));
        let outcome = fusion
            .run_stream(
                CursorScope::Account,
                Box::pin(futures::stream::iter([
                    page(false, true),
                    page(false, true),
                ])),
            )
            .await
            .expect("walk");
        assert!(matches!(outcome, FusionOutcome::NoCursor));
        let mut incomplete = 0;
        let mut unannounced = 0;
        while let Ok(event) = observer.try_recv() {
            if let SyncEvent::Warning(warning) = event.event.as_ref() {
                match warning.kind {
                    bifrost_types::WarningKind::Engine(
                        bifrost_types::EngineNotice::DegradedCoverage { .. },
                    ) => incomplete += 1,
                    bifrost_types::WarningKind::InventoryEndedUnannounced => unannounced += 1,
                    _ => {}
                }
            }
        }
        assert_eq!(
            incomplete, 1,
            "missing objects must be announced even without Done"
        );
        assert_eq!(unannounced, 1);
    }

    #[tokio::test]
    async fn a_fusion_page_under_a_moved_fence_is_refused_and_its_debt_recorded() {
        let (mut fusion, _delivery, mut rx, _views) = guarded_fusion();
        let (tx, mut requests) = tokio::sync::mpsc::channel(4);
        fusion.publisher.writer = crate::engine::writer::WriterHandle::new(tx);
        let writer = tokio::spawn(async move {
            let mut count = 0;
            while let Some(request) = requests.recv().await {
                let crate::engine::writer::WriterRequest::RecordDebt { report, done, .. } = request
                else {
                    panic!("debt request")
                };
                assert_eq!(report.obligations()[0].key().0, b"owed");
                count += 1;
                let _ = done.send(Ok(()));
            }
            count
        });
        fusion
            .publisher
            .coverage
            .raise_scope_fence(&CursorScope::Account);
        let outcome = fusion
            .run_stream(
                CursorScope::Account,
                Box::pin(futures::stream::iter([page(true, true), clean_done()])),
            )
            .await
            .expect("walk");
        assert!(matches!(outcome, FusionOutcome::Invalidated));
        assert!(fusion.cursors.snapshot(&CursorScope::Account).is_none());
        assert_eq!(fusion.publisher.coverage.pending_checkpoints(), 0);
        while let Ok(event) = rx.try_recv() {
            assert!(matches!(event.event.as_ref(), SyncEvent::Warning(_)));
        }
        drop(fusion);
        assert_eq!(writer.await.expect("writer"), 1);
    }
    #[tokio::test]
    async fn a_fusion_done_under_a_moved_fence_is_neither_published_nor_installed() {
        let (fusion, _delivery, mut rx, _views) = guarded_fusion();
        let _numbered = fusion.publisher.delivery.subscribe(
            Some(fusion.publisher.control.clone()),
            Arc::clone(fusion.publisher.coverage()),
        );
        let coverage = Arc::clone(&fusion.publisher.coverage);
        let stream = futures::stream::unfold(0, move |n| {
            let coverage = Arc::clone(&coverage);
            async move {
                match n {
                    0 => Some((page(false, false), 1)),
                    1 => {
                        coverage.raise_scope_fence(&CursorScope::Account);
                        Some((clean_done(), 2))
                    }
                    _ => None,
                }
            }
        });
        assert!(matches!(
            fusion
                .run_stream(CursorScope::Account, Box::pin(stream))
                .await
                .expect("walk"),
            FusionOutcome::Invalidated
        ));
        while let Ok(event) = rx.try_recv() {
            assert!(!matches!(event.event.as_ref(), SyncEvent::Done(_)));
        }
        assert!(fusion.cursors.snapshot(&CursorScope::Account).is_none());
    }
    #[tokio::test]
    async fn a_fusion_finalize_under_a_moved_fence_installs_nothing() {
        let (fusion, _, _, _views) = guarded_fusion();
        fusion
            .publisher
            .coverage
            .raise_scope_fence(&CursorScope::Account);
        assert!(matches!(
            fusion
                .finalize(
                    CursorScope::Account,
                    Some(Checkpoint::Change(crate::test_common::cursor_for(
                        &CursorScope::Account,
                        b"done"
                    )))
                )
                .await
                .expect("finalize"),
            FusionOutcome::Invalidated
        ));
        assert!(fusion.cursors.snapshot(&CursorScope::Account).is_none());
    }
    #[tokio::test]
    async fn a_fusion_forwards_no_batch_once_its_fence_has_moved() {
        let (fusion, _delivery, mut rx, _views) = guarded_fusion();
        fusion
            .publisher
            .coverage
            .raise_scope_fence(&CursorScope::Account);
        assert!(matches!(
            fusion
                .run_stream(
                    CursorScope::Account,
                    Box::pin(futures::stream::iter([page(false, false), clean_done()]))
                )
                .await
                .expect("walk"),
            FusionOutcome::Invalidated
        ));
        let event = rx.try_recv().expect("warning");
        assert_eq!(event.scope, EventScope::Cursor(CursorScope::Account));
        assert!(matches!(event.event.as_ref(), SyncEvent::Warning(_)));
        assert!(rx.try_recv().is_err());
    }

    /// A stub writer exercises the same conditional registration as production:
    /// a moved fence withholds the page and still routes its debt.
    #[tokio::test]
    async fn a_fusion_with_a_stub_writer_refuses_a_stale_checkpoint_page() {
        let (mut fusion, _delivery, mut rx, _views) = guarded_fusion();
        let (tx, mut requests) = tokio::sync::mpsc::channel(4);
        fusion.publisher.writer = crate::engine::writer::WriterHandle::new(tx);
        let writer = tokio::spawn(async move {
            let mut count = 0;
            while let Some(request) = requests.recv().await {
                let crate::engine::writer::WriterRequest::RecordDebt { report, done, .. } = request
                else {
                    panic!("debt request")
                };
                assert_eq!(report.obligations()[0].key().0, b"owed");
                count += 1;
                let _ = done.send(Ok(()));
            }
            count
        });
        fusion
            .publisher
            .coverage
            .raise_scope_fence(&CursorScope::Account);
        let outcome = fusion
            .run_stream(
                CursorScope::Account,
                Box::pin(futures::stream::iter([page(true, true), clean_done()])),
            )
            .await
            .expect("walk");
        assert!(matches!(outcome, FusionOutcome::Invalidated));
        assert!(fusion.cursors.snapshot(&CursorScope::Account).is_none());
        while let Ok(event) = rx.try_recv() {
            assert!(matches!(event.event.as_ref(), SyncEvent::Warning(_)));
        }
        drop(fusion);
        assert_eq!(writer.await.expect("writer"), 1);
    }

    /// The terminal checkpoint uses the same fence guard with a stub writer.
    #[tokio::test]
    async fn a_fusion_with_a_stub_writer_refuses_a_stale_done() {
        let (fusion, _delivery, mut rx, _views) = guarded_fusion();
        fusion
            .publisher
            .coverage
            .raise_scope_fence(&CursorScope::Account);
        assert!(matches!(
            fusion
                .run_stream(
                    CursorScope::Account,
                    Box::pin(futures::stream::iter([clean_done()]))
                )
                .await
                .expect("walk"),
            FusionOutcome::Invalidated
        ));
        let event = rx.try_recv().expect("warning");
        assert!(matches!(event.event.as_ref(), SyncEvent::Warning(_)));
        assert!(rx.try_recv().is_err());
        assert!(fusion.cursors.snapshot(&CursorScope::Account).is_none());
    }

    fn empty_inventory_stream() -> bifrost_types::AccountStream<bifrost_types::InventoryEvent> {
        Box::pin(futures::stream::empty())
    }

    /// The fusion path takes a `Change` cursor and refuses a `Backfill`
    /// checkpoint.
    ///
    #[test]
    fn fresh_invalid_fusion_checkpoint_is_a_scoped_terminal_contract_failure() {
        let scope = CursorScope::Account;
        let mut cursor = crate::test_common::cursor_for(&scope, b"fresh");
        cursor.envelope_version = bifrost_types::TraitEnvelopeVersion::new(u32::MAX);
        let checkpoint = Checkpoint::Change(cursor);
        let Err(Error::Account(error)) = validate_checkpoint_envelope(Some(&checkpoint)) else {
            panic!("expected classified contract error");
        };
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(scope))
        );
        assert!(matches!(
            crate::recovery::plan_recovery(error),
            crate::recovery::RecoveryPlan::Terminal(_)
        ));
    }

    /// Latent rather than live - no provider in this workspace attaches one - and
    /// refused for a structural reason: fusion registers the publication and then
    /// publishes it unstamped, so a `PublicationStream::Backfill` entry created here is never
    /// stamped with a delivery. An unsent stamp is deliberately never swept, so
    /// that entry is charged against the account's backfill bound until an
    /// acknowledgement, a lag or a reset frees it - a permanent slot of the bound
    /// spent per scope, narrowing every later cold start.
    #[test]
    fn a_fusion_batch_may_not_carry_a_backfill_checkpoint() {
        let cursor = bifrost_types::ChangeCursor {
            scope: CursorScope::Account,
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: bifrost_types::PayloadVersion::new(1),
                bytes: vec![1],
            },
            advanced_through: None,
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        };
        assert!(
            validate_checkpoint_envelope(Some(&Checkpoint::Change(cursor))).is_ok(),
            "a change cursor is what this path exists to carry"
        );
        assert!(
            validate_checkpoint_envelope(None).is_ok(),
            "and a batch with no checkpoint is ordinary"
        );

        let backfill = Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
            scope: CursorScope::Account,
            partition: bifrost_types::Partition(b"page:0:10".to_vec()),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        });
        assert!(
            validate_checkpoint_envelope(Some(&backfill)).is_err(),
            "a backfill checkpoint on a fusion batch charges the bound with a page \
             nothing will ever sweep"
        );
    }

    #[test]
    fn resume_guard_rejects_both_classifier_hook_disagreements() {
        let scope = CursorScope::Folder(bifrost_types::FolderId("resume".into()));
        let Err(classified_without_stream) =
            inventory_resume_stream_checked(scope.clone(), true, None)
        else {
            panic!("a declared resume cursor needs a stream")
        };
        let Err(stream_without_classification) =
            inventory_resume_stream_checked(scope.clone(), false, Some(empty_inventory_stream()))
        else {
            panic!("an undeclared cursor cannot have a resume stream")
        };
        for error in [classified_without_stream, stream_without_classification] {
            assert_eq!(
                error.scope(),
                Some(&bifrost_types::ErrorScope::Cursor(scope.clone()))
            );
            assert!(matches!(
                error.recovery(),
                bifrost_types::RecoveryClass::Engine(
                    bifrost_types::EngineDirective::RestartScope(found)
                ) if found == &scope
            ));
        }
        assert!(inventory_resume_stream_checked(scope.clone(), false, None).is_ok());
        assert!(
            inventory_resume_stream_checked(scope, true, Some(empty_inventory_stream())).is_ok()
        );
    }

    #[test]
    fn resume_contract_failure_is_forwarded_as_a_scoped_termination() {
        let scope = CursorScope::Folder(bifrost_types::FolderId("resume".into()));
        let error = inventory_cursor_contract_error(scope.clone());
        let (sender, mut receiver) = tokio::sync::broadcast::channel(2);
        let delivery = Arc::new(ChangeDelivery::new(sender));

        let FusionOutcome::Terminated(outcome_error) =
            forward_resume_contract_failure(&scope, error, &delivery)
        else {
            panic!("a resume disagreement must enter scoped recovery")
        };
        assert_eq!(
            outcome_error.scope(),
            Some(&bifrost_types::ErrorScope::Cursor(scope.clone()))
        );
        let event = receiver.try_recv().expect("termination forwarded");
        assert_eq!(event.scope, EventScope::Cursor(scope.clone()));
        let SyncEvent::Terminated(forwarded) = event.event.as_ref() else {
            panic!("the forwarded event must be terminal")
        };
        assert!(matches!(
            forwarded.recovery(),
            bifrost_types::RecoveryClass::Engine(
                bifrost_types::EngineDirective::RestartScope(found)
            ) if found == &scope
        ));
    }

    fn barrier_coverage() -> bifrost_types::InventoryCoverageReport {
        let error = bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Request(
            bifrost_types::RequestCause::Malformed {
                detail: bifrost_types::DiagnosticText::support_only("barrier"),
            },
        ))
        .try_build()
        .expect("valid error");
        bifrost_types::InventoryCoverageReport::degraded(
            bifrost_types::CoverageDomain::full(CursorScope::Account),
            vec![bifrost_types::InventoryObligation::Region {
                key: bifrost_types::ObligationKey(b"blocked".to_vec()),
                failure_label: "unidentifiable-value".into(),
                error,
                recovery: bifrost_types::RegionRecovery::barrier(),
            }],
        )
    }

    fn fusion_with_writer(
        writer_tx: tokio::sync::mpsc::Sender<crate::engine::writer::WriterRequest>,
    ) -> InventoryFusion {
        let (mut fusion, _delivery, _receiver, _views) = guarded_fusion();
        fusion.publisher.writer = crate::engine::writer::WriterHandle::new(writer_tx);
        fusion
    }

    /// A walk parked on a provider stream that neither yields nor ends leaves
    /// at shutdown, and drops the stream on the way out.
    ///
    /// Without the arm the poll waits on the stream for ever, and in the engine
    /// only the owner's join deadline gets it back. `NoCursor` would be the
    /// wrong answer: callers read it as a finished walk.
    #[tokio::test(start_paused = true)]
    async fn shutdown_cuts_a_walk_parked_on_its_provider_stream() {
        let (writer_tx, _writer_rx) = tokio::sync::mpsc::channel(4);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let mut fusion = fusion_with_writer(writer_tx);
        fusion.publisher.shutdown = shutdown.clone();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        struct OnDrop(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for OnDrop {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }
        let guard = OnDrop(Some(dropped_tx));
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::pending().map(move |event| {
                let _ = &guard;
                event
            }));

        let walk = fusion.run_stream(CursorScope::Account, stream);
        tokio::pin!(walk);
        // Poll the walk once, so it is genuinely parked in the provider poll
        // before the cancel lands.
        tokio::select! {
            biased;
            outcome = &mut walk => panic!("a pending stream cannot finish the walk: {outcome:?}"),
            () = tokio::task::yield_now() => {}
        }
        shutdown.cancel();
        // Bounded so a walk that ignores the cancel fails here rather than
        // hanging; under paused time the bound costs nothing when it holds.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(60), walk)
            .await
            .expect("the cancel must end the walk, not leave it parked");
        assert!(
            matches!(outcome, Err(Error::ShuttingDown)),
            "a cancelled walk reports shutdown, not a finished walk: {outcome:?}"
        );
        dropped_rx
            .await
            .expect("the provider stream is dropped on the way out");
    }

    /// A producer's own inventory warning must reach the consumer.
    ///
    /// Both engine inventory front ends used to absorb `InventoryEvent::Warning`
    /// in the same arm as `Progress`, one arm below the `Terminated` case that
    /// forwards. Two live producers announce a DEGRADE that way and nothing
    /// else: Graph's public-folder over-cap switch to additions-only, where
    /// deletions silently stop propagating, and IMAP's QRESYNC -> CONDSTORE
    /// strategy downgrade. The IMAP case is why absorbing it is worse than
    /// losing an ordinary message - the warning sits behind a one-shot
    /// `AtomicBool` shared with the changes path, so whichever lane arrives
    /// first consumes it and the other can never re-emit.
    #[tokio::test]
    async fn an_inventory_warning_reaches_the_change_stream() {
        let (writer_tx, _writer_rx) = tokio::sync::mpsc::channel(4);
        let mut fusion = fusion_with_writer(writer_tx);
        let warning = bifrost_types::Warning::support_only(
            bifrost_types::WarningKind::StrategyDowngraded,
            "QResync->Condstore",
        );
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::iter(vec![
                bifrost_types::InventoryEvent::Warning(warning),
                bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::complete(
                    bifrost_types::CoverageDomain::full(CursorScope::Account),
                    None,
                    bifrost_types::PartitionEnd::Exhausted,
                )),
            ]));
        let (tx, mut changes_rx) = tokio::sync::broadcast::channel(8);
        fusion.publisher.delivery = Arc::new(ChangeDelivery::new(tx));

        fusion
            .run_stream(CursorScope::Account, stream)
            .await
            .expect("a clean walk carrying a warning still completes");

        let announced = changes_rx
            .try_recv()
            .expect("the producer's warning must be forwarded, not absorbed");
        assert!(
            matches!(announced.event.as_ref(), SyncEvent::Warning(w)
                if w.kind == bifrost_types::WarningKind::StrategyDowngraded),
            "the forwarded event must be the producer's own warning, got {:?}",
            announced.event
        );
    }

    /// A stream that ends with neither `Done` nor `Terminated` establishes no
    /// cursor, and says why on the change stream - the same warning the
    /// backfill runner emits for the same producer defect.
    #[tokio::test]
    async fn a_walk_that_ends_silently_warns_and_establishes_nothing() {
        let (writer_tx, _writer_rx) = tokio::sync::mpsc::channel(4);
        let mut fusion = fusion_with_writer(writer_tx);
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::empty());
        let (tx, mut changes_rx) = tokio::sync::broadcast::channel(8);
        fusion.publisher.delivery = Arc::new(ChangeDelivery::new(tx));

        let outcome = fusion
            .run_stream(CursorScope::Account, stream)
            .await
            .expect("a silent end is not an engine error");
        assert!(matches!(outcome, FusionOutcome::NoCursor), "{outcome:?}");
        let announced = changes_rx.try_recv().expect("the silent end is surfaced");
        assert!(
            matches!(announced.event.as_ref(), SyncEvent::Warning(w)
                if w.kind == bifrost_types::WarningKind::InventoryEndedUnannounced),
            "{:?}",
            announced.event
        );
    }

    /// Same rule the backfill runner enforces, on the OTHER front end of the
    /// shared walk: a barrier whose incident the store refused to persist is
    /// announced to nobody. The walk fails so establishment retries; returning
    /// `NoCursor` and warning instead would announce a barrier a restart
    /// forgets and no operator can block or waive.
    #[tokio::test]
    async fn a_store_refused_barrier_fails_the_fusion_walk_instead_of_announcing_it() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let writer = tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                if let crate::engine::writer::WriterRequest::RecordBarrier { done, .. } = request {
                    let _ = done.send(Err(Error::Other("store write failed".into())));
                }
            }
        });
        let mut fusion = fusion_with_writer(tx);
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::iter(vec![
                bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::new(
                    None,
                    barrier_coverage(),
                    bifrost_types::PartitionEnd::Exhausted,
                )),
            ]));
        let (tx, mut changes_rx) = tokio::sync::broadcast::channel(8);
        fusion.publisher.delivery = Arc::new(ChangeDelivery::new(tx));

        let outcome = fusion.run_stream(CursorScope::Account, stream).await;
        assert!(
            outcome.is_err(),
            "a barrier the store refused must fail the walk, not conclude it"
        );
        assert!(
            changes_rx.try_recv().is_err(),
            "nothing may be announced for an incident nothing durable records"
        );
        drop(fusion);
        writer.await.expect("writer task");
    }

    /// The strictness stays honest: a writer that is GONE (detach, shutdown)
    /// is not a failed write, and must not fail the walk.
    #[tokio::test]
    async fn a_departed_writer_does_not_fail_the_fusion_walk() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let fusion = fusion_with_writer(tx);
        let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
            Box::pin(futures::stream::iter(vec![
                bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::new(
                    None,
                    barrier_coverage(),
                    bifrost_types::PartitionEnd::Exhausted,
                )),
            ]));

        let outcome = fusion
            .run_stream(CursorScope::Account, stream)
            .await
            .expect("a departed writer is not a store failure");
        assert!(matches!(outcome, FusionOutcome::NoCursor));
    }

    /// A page claiming `Advance` over coverage that holds a barrier is the
    /// account contradicting its own stream contract, on either half of the
    /// walk. It terminates as a classified contract violation rather than
    /// being read as an ordinary barrier stop: a producer that mislabels its
    /// checkpoint has told the engine nothing it can trust.
    #[tokio::test]
    async fn a_checkpoint_disagreeing_with_its_coverage_terminates_the_walk() {
        let cursor = Checkpoint::Change(bifrost_types::ChangeCursor {
            scope: CursorScope::Account,
            server_state: bifrost_types::OpaqueChangeState {
                protocol: bifrost_types::ProtocolKind::Imap,
                envelope_version: bifrost_types::PayloadVersion::new(1),
                bytes: vec![1],
            },
            advanced_through: None,
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(1),
        });
        let page = bifrost_types::InventoryEvent::Batch(bifrost_types::InventoryBatch {
            removed: Vec::new(),
            items: Vec::new(),
            page_boundary: bifrost_types::PageBoundary::Page,
            server_latency: std::time::Duration::ZERO,
            bytes_in: 0,
            checkpoint: bifrost_types::PageCheckpoint::Advance(cursor.clone()),
            coverage: barrier_coverage(),
        });
        let completion = bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion {
            checkpoint: bifrost_types::PageCheckpoint::Advance(cursor),
            coverage: barrier_coverage(),
            end: bifrost_types::PartitionEnd::Exhausted,
        });
        for event in [page, completion] {
            // A departed writer, so a walk that skipped the check would stop at
            // the barrier and answer `NoCursor` rather than hang on the writer.
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            drop(rx);
            let stream: bifrost_types::AccountStream<bifrost_types::InventoryEvent> =
                Box::pin(futures::stream::iter(vec![event]));
            let outcome = fusion_with_writer(tx)
                .run_stream(CursorScope::Account, stream)
                .await
                .expect("a contract violation is an outcome, not an engine error");
            let FusionOutcome::Terminated(error) = outcome else {
                panic!("a mislabelled checkpoint must terminate the walk: {outcome:?}");
            };
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Internal(
                    bifrost_types::InternalErrorKind::AccountContract
                )
            );
        }
    }
}
