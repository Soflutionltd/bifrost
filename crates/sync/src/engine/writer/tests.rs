use super::acknowledge::persist_ack_request;
use super::*;
use crate::cursor::CheckpointStore;

struct FlakyLedgerStore {
    inner: crate::cursor::InMemoryCheckpointStore,
    fail_reads: std::sync::atomic::AtomicBool,
    fail_transitions: std::sync::atomic::AtomicBool,
    fail_writes: std::sync::atomic::AtomicBool,
    wrong_backfill_selection: std::sync::atomic::AtomicBool,
}

impl CheckpointStore for FlakyLedgerStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: crate::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.clear_account_checkpoints(account, ledger)
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        transition: crate::cursor::store::CheckpointTransition,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        if self
            .fail_transitions
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Box::pin(async { Err(Error::CheckpointStore("transition failed".into())) });
        }
        self.inner.apply_transition(account, transition)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>,
    > {
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<bifrost_types::BackfillCheckpoint>, Error>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut selected = self.inner.get_backfill(account, scope).await?;
            if self
                .wrong_backfill_selection
                .load(std::sync::atomic::Ordering::SeqCst)
                && let Some(checkpoint) = &mut selected
            {
                checkpoint.partition = bifrost_types::Partition(b"page:0:500".to_vec());
                checkpoint.progress.items_done = 500;
            }
            Ok(selected)
        })
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        ledger: crate::cursor::DebtLedger,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        if self.fail_writes.load(std::sync::atomic::Ordering::SeqCst) {
            return Box::pin(async { Err(Error::CheckpointStore("ledger write failed".into())) });
        }
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<crate::cursor::DebtLedger, Error>> + Send + 'a>,
    > {
        if self.fail_reads.load(std::sync::atomic::Ordering::SeqCst) {
            return Box::pin(async { Err(Error::CheckpointStore("ledger read failed".into())) });
        }
        self.inner.get_ledger(account)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a bifrost_types::AccountId,
        scope: &'a CursorScope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

fn email_scope() -> CursorScope {
    CursorScope::Type(bifrost_types::ObjectType::Email)
}

fn change_cursor(bytes: &[u8]) -> ChangeCursor {
    ChangeCursor {
        scope: email_scope(),
        server_state: bifrost_types::OpaqueChangeState {
            protocol: bifrost_types::ProtocolKind::Imap,
            envelope_version: bifrost_types::PayloadVersion::new(crate::cursor::ENGINE_VERSION),
            bytes: bytes.to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(crate::cursor::ENGINE_VERSION),
    }
}

fn unrepresentable(key: &str) -> bifrost_types::InventoryObligation {
    let error = bifrost_types::AccountErrorBuilder::new(bifrost_types::PrimaryCause::Request(
        bifrost_types::RequestCause::Malformed {
            detail: bifrost_types::DiagnosticText::support_only("unrepresentable"),
        },
    ))
    .try_build()
    .expect("valid account error classification");
    bifrost_types::InventoryObligation::Object {
        key: bifrost_types::ObligationKey(key.as_bytes().to_vec()),
        id: bifrost_types::ObjectId(key.into()),
        error,
        repair: Vec::new(),
    }
}

// These writer protocol tests deliberately retain the raw endpoint to submit
// requests and inspect persistence failures independently of WriterHandle.
fn start_flaky_writer(
    store: Arc<FlakyLedgerStore>,
) -> (mpsc::Sender<WriterRequest>, tokio::task::JoinHandle<()>) {
    let account = AccountId("acct".into());
    let coverage = Arc::new(PendingCoverage::new());
    let (boundary, _view) = crate::cancel::Boundary::new();
    let (priority, _priority_view) = tokio::sync::watch::channel(bifrost_types::Priority::Normal);
    let (bandwidth, _bandwidth_view) = tokio::sync::watch::channel(None);
    let control = SyncControl::new_with_publications(
        account.clone(),
        boundary,
        priority,
        bandwidth,
        Arc::clone(&coverage),
    );
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(ack_writer(account, store, control, coverage, rx));
    (tx, task)
}

async fn refused_debt_failure_case(fail_load: bool) {
    let store = seeded_store().await;
    store
        .fail_reads
        .store(fail_load, std::sync::atomic::Ordering::SeqCst);
    store
        .fail_writes
        .store(!fail_load, std::sync::atomic::Ordering::SeqCst);
    let (tx, task) = start_flaky_writer(Arc::clone(&store));
    let claim = crate::cursor::CoverageClaim::new(
        bifrost_types::InventoryCoverageReport::degraded(
            bifrost_types::CoverageDomain::full(email_scope()),
            vec![unrepresentable("refused")],
        ),
        crate::cursor::WalkGeneration::for_test(7),
    );
    crate::inventory_walk::record_refused_debt(Some(&WriterHandle::new(tx.clone())), claim)
        .await
        .expect("writer retains failed debt");
    store
        .fail_reads
        .store(false, std::sync::atomic::Ordering::SeqCst);
    store
        .fail_writes
        .store(false, std::sync::atomic::Ordering::SeqCst);
    persist_test_cursor(&tx).await.expect("flush ledger");
    let ledger = store
        .inner
        .get_ledger(&AccountId("acct".into()))
        .await
        .expect("ledger");
    assert_eq!(ledger.open_debt().count(), 2);
    drop(tx);
    task.await.expect("writer");
}
#[tokio::test]
async fn barrier_query_preserves_ledger_read_failure() {
    let store = seeded_store().await;
    store
        .fail_reads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (tx, task) = start_flaky_writer(store);
    let (done, recv) = oneshot::channel();
    tx.send(WriterRequest::ScopeBarrierBlocked {
        scope: email_scope(),
        done,
    })
    .await
    .expect("writer alive");
    assert!(matches!(
        recv.await.expect("writer answered"),
        Err(Error::CheckpointStore(_))
    ));
    drop(tx);
    task.await.expect("writer stopped");
}

#[tokio::test]
async fn debt_offered_before_the_ledger_loads_is_kept() {
    refused_debt_failure_case(true).await;
}
#[tokio::test]
async fn refused_debt_survives_a_failed_ledger_write() {
    refused_debt_failure_case(false).await;
}

async fn persist_test_cursor(tx: &mpsc::Sender<WriterRequest>) -> Result<(), Error> {
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::PersistEstablished {
        cursor: change_cursor(b"next"),
        done,
    })
    .await
    .expect("writer alive");
    wait.await.expect("writer answered")
}

async fn seeded_store() -> Arc<FlakyLedgerStore> {
    let store = Arc::new(FlakyLedgerStore {
        inner: crate::cursor::InMemoryCheckpointStore::new(),
        fail_reads: std::sync::atomic::AtomicBool::new(false),
        fail_transitions: std::sync::atomic::AtomicBool::new(false),
        fail_writes: std::sync::atomic::AtomicBool::new(false),
        wrong_backfill_selection: std::sync::atomic::AtomicBool::new(false),
    });
    let report = bifrost_types::InventoryCoverageReport::degraded(
        bifrost_types::CoverageDomain::full(email_scope()),
        vec![unrepresentable("owed")],
    );
    let mut ledger = crate::cursor::DebtLedger::default();
    ledger
        .accrue()
        .ingest_debt_only(&report, crate::cursor::WalkGeneration::for_test(1), 1);
    store
        .inner
        .put_ledger(&AccountId("acct".into()), ledger)
        .await
        .expect("seed debt");
    store
}

async fn provisional_ack_abort_case(case: &str) {
    let store = seeded_store().await;
    let account = AccountId("acct".into());
    let (tx, task) = start_flaky_writer(Arc::clone(&store));
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ReattachInsert {
        attempt: crate::engine::ReattachAttempt::for_test(71),
        cursor: change_cursor(b"provisional"),
        done,
    })
    .await
    .expect("writer alive");
    wait.await.expect("insert answered").expect("insert landed");

    let mut cursor = change_cursor(b"acked");
    if case == "rejected" {
        cursor.envelope_version = bifrost_types::TraitEnvelopeVersion::new(u32::MAX);
    }
    store
        .fail_transitions
        .store(case == "failed", std::sync::atomic::Ordering::SeqCst);
    let checkpoint = if case == "backfill" || case == "withheld" {
        Checkpoint::Backfill(bifrost_types::BackfillCheckpoint {
            scope: email_scope(),
            partition: if case == "withheld" {
                crate::backfill::partitioner::completion_partition()
            } else {
                bifrost_types::Partition(b"page".to_vec())
            },
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(
                crate::cursor::ENGINE_VERSION,
            ),
        })
    } else {
        Checkpoint::Change(cursor)
    };
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(AckRequest {
        scope: email_scope(),
        checkpoint,
        publication: (case == "publication").then(|| {
            PendingCoverage::new().publish(crate::cursor::CoverageClaim {
                reports: Vec::new(),
                generation: crate::cursor::WalkGeneration::for_test(0),
            })
        }),
        complete: Some(done),
    }))
    .await
    .expect("writer alive");
    let result = wait.await.expect("ack answered");
    assert_eq!(
        result.is_err(),
        matches!(case, "rejected" | "failed" | "publication")
    );
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ReattachAbort {
        attempt: crate::engine::ReattachAttempt::for_test(71),
        done,
    })
    .await
    .expect("writer alive");
    wait.await.expect("abort answered");
    let saved = store
        .inner
        .get_change_cursor(&account, &email_scope())
        .await
        .expect("read cursor");
    if case == "durable" {
        assert_eq!(
            saved.expect("committed cursor").server_state.bytes,
            b"acked"
        );
    } else {
        assert!(saved.is_none(), "{case} ack must preserve abort ownership");
    }
    drop(tx);
    task.await.expect("writer finished");
}

#[tokio::test]
async fn a_live_read_neither_sees_nor_adopts_an_unsettled_reattach_row() {
    let store = seeded_store().await;
    let account = AccountId("acct".into());
    let (tx, task) = start_flaky_writer(Arc::clone(&store));
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ReattachInsert {
        attempt: crate::engine::ReattachAttempt::for_test(71),
        cursor: change_cursor(b"never-live"),
        done,
    })
    .await
    .expect("writer alive");
    wait.await.expect("insert answered").expect("insert landed");
    // The live scope's ordinary read is shown the provisional row as absent.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::GetChangeCursor {
        scope: email_scope(),
        claim_for: None,
        done,
    })
    .await
    .expect("writer alive");
    assert!(
        wait.await
            .expect("read answered")
            .expect("read landed")
            .is_none()
    );
    // It establishes fresh and persists its own row, which takes the row
    // over from the aborting attempt.
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::PersistEstablished {
        cursor: change_cursor(b"live"),
        done,
    })
    .await
    .expect("writer alive");
    wait.await
        .expect("persist answered")
        .expect("persist landed");
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ReattachAbort {
        attempt: crate::engine::ReattachAttempt::for_test(71),
        done,
    })
    .await
    .expect("writer alive");
    wait.await.expect("abort answered");
    let saved = store
        .inner
        .get_change_cursor(&account, &email_scope())
        .await
        .expect("durable read")
        .expect("live row survives the abort");
    assert_eq!(saved.server_state.bytes, b"live");
    drop(tx);
    task.await.expect("writer finished");
}

#[tokio::test]
async fn a_live_read_alone_leaves_an_aborted_reattach_row_to_its_rollback() {
    let store = seeded_store().await;
    let account = AccountId("acct".into());
    let (tx, task) = start_flaky_writer(Arc::clone(&store));
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ReattachInsert {
        attempt: crate::engine::ReattachAttempt::for_test(71),
        cursor: change_cursor(b"never-live"),
        done,
    })
    .await
    .expect("writer alive");
    wait.await.expect("insert answered").expect("insert landed");
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::GetChangeCursor {
        scope: email_scope(),
        claim_for: None,
        done,
    })
    .await
    .expect("writer alive");
    assert!(
        wait.await
            .expect("read answered")
            .expect("read landed")
            .is_none()
    );
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ReattachAbort {
        attempt: crate::engine::ReattachAttempt::for_test(71),
        done,
    })
    .await
    .expect("writer alive");
    wait.await.expect("abort answered");
    assert!(
        store
            .inner
            .get_change_cursor(&account, &email_scope())
            .await
            .expect("durable read")
            .is_none()
    );
    drop(tx);
    task.await.expect("writer finished");
}

#[tokio::test]
async fn a_rejected_ack_preserves_provisional_abort_ownership() {
    provisional_ack_abort_case("rejected").await;
}

#[tokio::test]
async fn an_invalid_publication_ack_preserves_provisional_abort_ownership() {
    provisional_ack_abort_case("publication").await;
}

#[tokio::test]
async fn a_failed_ack_preserves_provisional_abort_ownership() {
    provisional_ack_abort_case("failed").await;
}

#[tokio::test]
async fn a_durable_change_ack_survives_provisional_abort() {
    provisional_ack_abort_case("durable").await;
}

#[tokio::test]
async fn a_backfill_ack_does_not_promote_a_provisional_change_cursor() {
    provisional_ack_abort_case("backfill").await;
}

#[tokio::test]
async fn a_withheld_ack_preserves_provisional_abort_ownership() {
    provisional_ack_abort_case("withheld").await;
}

#[tokio::test]
async fn unread_ledger_refuses_writes_then_recovers_without_erasing_debt() {
    let store = seeded_store().await;
    let account = AccountId("acct".into());
    store
        .fail_reads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (tx, task) = start_flaky_writer(Arc::clone(&store));
    assert!(persist_test_cursor(&tx).await.is_err());
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::Ack(AckRequest {
        scope: email_scope(),
        checkpoint: Checkpoint::Change(change_cursor(b"acked")),
        publication: None,
        complete: Some(done),
    }))
    .await
    .expect("writer alive");
    assert!(wait.await.expect("ack answered").is_err());
    assert!(persist_test_cursor(&tx).await.is_err());
    assert!(
        store
            .inner
            .get_change_cursor(&account, &email_scope())
            .await
            .expect("read")
            .is_none()
    );
    let key = bifrost_types::ObligationKey(b"owed".to_vec());
    assert!(
        store
            .inner
            .get_ledger(&account)
            .await
            .expect("read")
            .entry(&key)
            .is_some()
    );
    store
        .fail_reads
        .store(false, std::sync::atomic::Ordering::SeqCst);
    persist_test_cursor(&tx).await.expect("recovered read");
    assert!(
        store
            .inner
            .get_change_cursor(&account, &email_scope())
            .await
            .expect("read")
            .is_some()
    );
    assert!(
        store
            .inner
            .get_ledger(&account)
            .await
            .expect("read")
            .entry(&key)
            .expect("debt retained")
            .is_open()
    );
    drop(tx);
    task.await.expect("writer exits");
}

#[tokio::test]
async fn failed_operator_decisions_do_not_leak_into_later_writes() {
    for decision in [
        OperatorDecision::Waive {
            by: "operator".into(),
            at_unix_seconds: 2,
        },
        OperatorDecision::Block,
    ] {
        let store = seeded_store().await;
        let account = AccountId("acct".into());
        let key = bifrost_types::ObligationKey(b"owed".to_vec());
        let original = store
            .inner
            .get_ledger(&account)
            .await
            .expect("read")
            .entry(&key)
            .expect("seeded")
            .policy
            .clone();
        let (tx, task) = start_flaky_writer(Arc::clone(&store));
        store
            .fail_writes
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let (done, wait) = oneshot::channel();
        tx.send(WriterRequest::OperatorDecision {
            key: key.clone(),
            decision: decision.clone(),
            done,
        })
        .await
        .expect("writer alive");
        assert!(wait.await.expect("answered").is_err());
        store
            .fail_writes
            .store(false, std::sync::atomic::Ordering::SeqCst);
        persist_test_cursor(&tx).await.expect("unrelated write");
        assert_eq!(
            store
                .inner
                .get_ledger(&account)
                .await
                .expect("read")
                .entry(&key)
                .expect("retained")
                .policy,
            original
        );
        let (done, wait) = oneshot::channel();
        tx.send(WriterRequest::OperatorDecision {
            key: key.clone(),
            decision,
            done,
        })
        .await
        .expect("writer alive");
        assert!(wait.await.expect("answered").expect("retry persisted"));
        assert_ne!(
            store
                .inner
                .get_ledger(&account)
                .await
                .expect("read")
                .entry(&key)
                .expect("retained")
                .policy,
            original
        );
        drop(tx);
        task.await.expect("writer exits");
    }
}

#[tokio::test]
async fn failed_ack_proof_does_not_leak_into_an_unrelated_write() {
    let store = seeded_store().await;
    let account = AccountId("acct".into());
    let mut ledger = store.inner.get_ledger(&account).await.expect("seeded");
    let coverage = PendingCoverage::new();
    let checkpoint = Checkpoint::Change(change_cursor(b"proof"));
    let publication = coverage.register(
        checkpoint.clone(),
        crate::cursor::CoverageClaim::new(
            bifrost_types::InventoryCoverageReport::complete(bifrost_types::CoverageDomain::full(
                email_scope(),
            )),
            crate::cursor::WalkGeneration::for_test(2),
        ),
    );
    let request = AckRequest {
        scope: email_scope(),
        checkpoint,
        publication: Some(publication),
        complete: None,
    };
    let shared = Arc::clone(&store);
    let erased: Arc<DynCheckpointStore> = shared;
    store
        .fail_transitions
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        persist_ack_request(
            &account,
            &erased,
            &coverage,
            &mut ledger,
            &request,
            &mut HashSet::new()
        )
        .await
        .is_err()
    );
    assert_eq!(ledger.open_debt().count(), 1);
    super::persist_ledger_only(&account, &erased, &ledger)
        .await
        .expect("unrelated ledger write");
    assert_eq!(
        store
            .inner
            .get_ledger(&account)
            .await
            .expect("read")
            .open_debt()
            .count(),
        1
    );
    store
        .fail_transitions
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(
        persist_ack_request(
            &account,
            &erased,
            &coverage,
            &mut ledger,
            &request,
            &mut HashSet::new()
        )
        .await
        .is_ok()
    );
    assert_eq!(ledger.open_debt().count(), 0);
}

#[tokio::test]
async fn failed_repair_resolutions_do_not_leak_into_later_writes() {
    let store = seeded_store().await;
    let account = AccountId("acct".into());
    let key = bifrost_types::ObligationKey(b"owed".to_vec());
    let (tx, task) = start_flaky_writer(Arc::clone(&store));
    let resolution = || crate::repair::RepairResolution::Irrelevant {
        key: key.clone(),
        detail: "the object is no longer owed".into(),
        generation: crate::cursor::WalkGeneration::for_test(1),
    };

    store
        .fail_writes
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ApplyRepair {
        resolutions: vec![resolution()],
        publication: None,
        done,
    })
    .await
    .expect("writer alive");
    assert!(wait.await.expect("repair answered").is_err());

    store
        .fail_writes
        .store(false, std::sync::atomic::Ordering::SeqCst);
    persist_test_cursor(&tx)
        .await
        .expect("unrelated write after failed repair");
    assert!(
        store
            .inner
            .get_ledger(&account)
            .await
            .expect("read ledger")
            .entry(&key)
            .expect("debt retained")
            .is_open()
    );

    let (done, wait) = oneshot::channel();
    tx.send(WriterRequest::ApplyRepair {
        resolutions: vec![resolution()],
        publication: None,
        done,
    })
    .await
    .expect("writer alive");
    wait.await
        .expect("repair answered")
        .expect("repair retry persisted");
    assert!(
        !store
            .inner
            .get_ledger(&account)
            .await
            .expect("read ledger")
            .entry(&key)
            .expect("resolved debt retained for audit")
            .is_open()
    );

    drop(tx);
    task.await.expect("writer exits");
}

#[test]
fn resume_selection_must_not_hide_a_known_completion() {
    let scope = email_scope();
    let mut row = bifrost_types::BackfillCheckpoint {
        scope: scope.clone(),
        partition: bifrost_types::Partition(b"old partial".to_vec()),
        progress_marker: None,
        progress: bifrost_types::BackfillProgress {
            items_done: 100,
            ..Default::default()
        },
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(crate::cursor::ENGINE_VERSION),
    };
    assert!(validate_backfill_selection(&scope, true, Some(&row)).is_err());
    assert!(validate_backfill_selection(&scope, true, None).is_err());
    assert!(validate_backfill_selection(&scope, false, Some(&row)).is_ok());
    row.partition = crate::backfill::partitioner::completion_partition();
    row.progress.items_done = 0;
    assert!(validate_backfill_selection(&scope, true, Some(&row)).is_ok());
}

#[tokio::test]
async fn writer_refuses_a_partial_resume_after_completion_and_forgets_it_on_reset() {
    let store = seeded_store().await;
    let (tx, task) = start_flaky_writer(Arc::clone(&store));
    let writer = WriterHandle::new(tx);
    let scope = email_scope();
    writer
        .persist_backfill(bifrost_types::BackfillCheckpoint {
            scope: scope.clone(),
            partition: crate::backfill::partitioner::completion_partition(),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(
                crate::cursor::ENGINE_VERSION,
            ),
        })
        .await
        .expect("complete");
    store
        .wrong_backfill_selection
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        writer.get_backfill(scope.clone()).await,
        Err(Error::CheckpointStore(_))
    ));
    writer
        .reset_account_for_schema_recovery()
        .await
        .expect("reset");
    assert!(
        writer
            .get_backfill(scope)
            .await
            .expect("read after reset")
            .is_none()
    );
    drop(writer);
    task.await.expect("writer drained");
}
#[tokio::test]
async fn refused_marker_ack_preserves_completion_selection_guard() {
    for rejected in [false, true] {
        let store = seeded_store().await;
        let (tx, task) = start_flaky_writer(Arc::clone(&store));
        let writer = WriterHandle::new(tx.clone());
        let scope = email_scope();
        let checkpoint = bifrost_types::BackfillCheckpoint {
            scope: scope.clone(),
            partition: crate::backfill::partitioner::completion_partition(),
            progress_marker: None,
            progress: bifrost_types::BackfillProgress::default(),
            envelope_version: bifrost_types::TraitEnvelopeVersion::new(
                crate::cursor::ENGINE_VERSION,
            ),
        };
        writer
            .persist_backfill(checkpoint.clone())
            .await
            .expect("complete");
        let publication = rejected.then(|| {
            PendingCoverage::new().register(
                Checkpoint::Change(change_cursor(b"different lane")),
                crate::cursor::CoverageClaim::new(
                    bifrost_types::InventoryCoverageReport::complete(
                        bifrost_types::CoverageDomain::full(scope.clone()),
                    ),
                    crate::cursor::WalkGeneration::for_test(2),
                ),
            )
        });
        let (done, wait) = oneshot::channel();
        tx.send(WriterRequest::Ack(AckRequest {
            scope: scope.clone(),
            checkpoint: Checkpoint::Backfill(checkpoint),
            publication,
            complete: Some(done),
        }))
        .await
        .expect("writer alive");
        let outcome = wait.await.expect("ack answered");
        if rejected {
            assert!(outcome.is_err(), "mismatched receipt must be rejected");
        } else {
            assert!(matches!(
                outcome,
                Ok(AckOutcome::Withheld(CompletionRefusal::Unvouchable))
            ));
        }
        store
            .wrong_backfill_selection
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Both refusals leave the durable completion untouched. Forgetting its
        // evidence would silently accept this partial row in the old order.
        assert!(matches!(
            writer.get_backfill(scope).await,
            Err(Error::CheckpointStore(_))
        ));
        drop(writer);
        drop(tx);
        task.await.expect("writer drained");
    }
}
