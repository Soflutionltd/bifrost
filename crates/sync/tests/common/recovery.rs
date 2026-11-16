//! Provider scripts shared by attachment and recovery contract tests.

#![allow(dead_code)]

use crate::common;
use bifrost_sync::{
    CheckpointStore, CheckpointTransition, DebtLedger, Error, InMemoryCheckpointStore,
};
use bifrost_types::Checkpoint;
use bifrost_types::{
    Account, AccountCapabilities, AccountError, AccountFactory, AccountFuture, AccountId, Batch,
    CursorEstablishment, CursorScope, ErrorScope, Page, PageBoundary, PushCapability,
    ScopeLifecycleEvent, SubscriptionHandle, SyncEvent,
};
use bifrost_types::{BackfillCheckpoint, ChangeCursor};
use common::{caps, cursor_for, unsupported};
use futures::stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// A store whose `get_change_cursor` reports the row as undecodable -
/// what a consumer store returns when `decode_envelope` hits a schema
/// it cannot read - until the engine deletes the row or clears the account.
#[derive(Default)]
pub(crate) struct PoisonedStore {
    pub(crate) inner: InMemoryCheckpointStore,
    /// Scopes still reporting `SchemaIncompatible`.
    pub(crate) poisoned: Mutex<Vec<CursorScope>>,
    /// Scopes the engine asked to delete one by one, in call order.
    pub(crate) deleted: Mutex<Vec<CursorScope>>,
    pub(crate) fail_reads: bool,
    /// Account-wide clears the engine ran.
    pub(crate) clears: AtomicUsize,
}

impl PoisonedStore {
    pub(crate) fn with_poisoned(scopes: Vec<CursorScope>) -> Self {
        Self {
            poisoned: Mutex::new(scopes),
            ..Default::default()
        }
    }

    pub(crate) fn deleted(&self) -> Vec<CursorScope> {
        self.deleted.lock().expect("deleted lock").clone()
    }

    pub(crate) fn with_read_failure() -> Self {
        Self {
            fail_reads: true,
            ..Default::default()
        }
    }
}

impl CheckpointStore for PoisonedStore {
    fn clear_account_checkpoints<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            self.inner
                .clear_account_checkpoints(account, ledger)
                .await?;
            self.clears.fetch_add(1, Ordering::SeqCst);
            self.poisoned.lock().expect("poisoned").clear();
            Ok(())
        })
    }

    fn apply_transition<'a>(
        &'a self,
        account: &'a AccountId,
        transition: CheckpointTransition,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.apply_transition(account, transition)
    }

    fn put_ledger<'a>(
        &'a self,
        account: &'a AccountId,
        ledger: DebtLedger,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.put_ledger(account, ledger)
    }

    fn get_ledger<'a>(
        &'a self,
        account: &'a AccountId,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<DebtLedger, Error>> + Send + 'a>> {
        self.inner.get_ledger(account)
    }

    fn get_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Option<ChangeCursor>, Error>> + Send + 'a>>
    {
        if self.fail_reads {
            return Box::pin(async {
                Err(Error::CheckpointStore(
                    "forced attach-time read failure".into(),
                ))
            });
        }
        let still_poisoned = self
            .poisoned
            .lock()
            .expect("poisoned lock")
            .iter()
            .any(|s| s == scope);
        if still_poisoned {
            return Box::pin(async { Err(Error::SchemaIncompatible) });
        }
        self.inner.get_change_cursor(account, scope)
    }

    fn get_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<BackfillCheckpoint>, Error>> + Send + 'a,
        >,
    > {
        self.inner.get_backfill(account, scope)
    }

    fn delete_change_cursor<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.deleted
            .lock()
            .expect("deleted lock")
            .push(scope.clone());
        // Clearing the row clears the poison: a re-read now behaves
        // like any other absent cursor.
        self.poisoned
            .lock()
            .expect("poisoned lock")
            .retain(|s| s != scope);
        self.inner.delete_change_cursor(account, scope)
    }

    fn delete_backfill<'a>(
        &'a self,
        account: &'a AccountId,
        scope: &'a CursorScope,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        self.inner.delete_backfill(account, scope)
    }
}

/// Account that discovers a fixed scope list and establishes a cursor
/// for each, recording which scopes it was asked to establish.
pub(crate) type SubscriptionCalls = Arc<Mutex<Vec<(usize, Vec<CursorScope>)>>>;

pub(crate) struct HealAccount {
    pub(crate) caps: AccountCapabilities,
    pub(crate) scopes: Vec<CursorScope>,
    pub(crate) established: Arc<Mutex<Vec<CursorScope>>>,
    pub(crate) closed: Arc<AtomicUsize>,
    pub(crate) generation: usize,
    pub(crate) closed_generations: Arc<Mutex<Vec<usize>>>,
    pub(crate) subscribed: SubscriptionCalls,
    pub(crate) unsubscribed: Arc<Mutex<Vec<(usize, SubscriptionHandle)>>>,
    pub(crate) unsubscribe_failures: Arc<AtomicUsize>,
    pub(crate) lifecycle_calls: Arc<Mutex<Vec<usize>>>,
    /// Events the next `scope_lifecycle_stream` call yields before the
    /// stream ends. Empty means every stream ends immediately, which is
    /// the shape most tests want.
    pub(crate) lifecycle_script: Arc<Mutex<VecDeque<ScopeLifecycleEvent>>>,
    /// When set, `contacts_list` answers with a page carrying both loss
    /// lanes populated. Off by default so the stub stays neutral for
    /// every test that does not care.
    pub(crate) degraded_contacts: bool,
    /// When set, `close()` announces that it has been entered and then
    /// parks until released, holding `detach` open inside its teardown
    /// window. `None` for every test that does not stage that window.
    pub(crate) close_gate: Arc<Mutex<Option<CloseGate>>>,
    /// When `Some`, `changes_stream` emits an ENDLESS run of batches that
    /// violate the boundary contract (`PageBoundary::Partial` carrying a
    /// checkpoint), counting each one it hands out. Endless is the point:
    /// a driver that merely logs the violation and re-polls would consume
    /// them without bound, so the count is the spin detector.
    pub(crate) bad_boundary_batches: Option<Arc<AtomicUsize>>,
    /// Emit ONE well-formed `Final` batch carrying a change checkpoint.
    pub(crate) live_checkpoint_batch: bool,
}

/// Test handle for pinning `detach` inside `Account::close()`.
pub(crate) struct CloseGate {
    pub(crate) entered: tokio::sync::oneshot::Sender<()>,
    pub(crate) release: tokio::sync::oneshot::Receiver<()>,
}

pub(crate) fn no_close_gate() -> Arc<Mutex<Option<CloseGate>>> {
    Arc::new(Mutex::new(None))
}

pub(crate) struct HealFactory {
    pub(crate) scopes: Vec<CursorScope>,
    pub(crate) established: Arc<Mutex<Vec<CursorScope>>>,
    pub(crate) closed: Arc<AtomicUsize>,
    pub(crate) degraded_contacts: bool,
    /// Handed to every account this factory opens; the first `close()`
    /// consumes it.
    pub(crate) close_gate: Arc<Mutex<Option<CloseGate>>>,
}

impl HealFactory {
    pub(crate) fn new(
        scopes: Vec<CursorScope>,
        established: Arc<Mutex<Vec<CursorScope>>>,
        closed: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            scopes,
            established,
            closed,
            degraded_contacts: false,
            close_gate: no_close_gate(),
        }
    }

    /// Opens accounts whose first `close()` parks until the test
    /// releases it, holding `detach` inside its teardown window.
    pub(crate) fn with_close_gate(
        scopes: Vec<CursorScope>,
        established: Arc<Mutex<Vec<CursorScope>>>,
        closed: Arc<AtomicUsize>,
        gate: CloseGate,
    ) -> Self {
        Self {
            close_gate: Arc::new(Mutex::new(Some(gate))),
            ..Self::new(scopes, established, closed)
        }
    }

    /// Opens accounts whose `contacts_list` returns a page that lost
    /// resources and skipped a scope.
    pub(crate) fn with_degraded_contacts(
        scopes: Vec<CursorScope>,
        established: Arc<Mutex<Vec<CursorScope>>>,
        closed: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            degraded_contacts: true,
            ..Self::new(scopes, established, closed)
        }
    }
}

impl AccountFactory for HealFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let scopes = self.scopes.clone();
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        let degraded_contacts = self.degraded_contacts;
        let close_gate = Arc::clone(&self.close_gate);
        Box::pin(async move {
            let account: Arc<dyn Account> = Arc::new(
                HealAccount {
                    caps: caps(),
                    scopes,
                    established,
                    closed,
                    generation: 0,
                    closed_generations: Arc::new(Mutex::new(Vec::new())),
                    subscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribed: Arc::new(Mutex::new(Vec::new())),
                    unsubscribe_failures: Arc::new(AtomicUsize::new(0)),
                    lifecycle_calls: Arc::new(Mutex::new(Vec::new())),
                    lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
                    degraded_contacts,
                    close_gate: Arc::clone(&close_gate),
                    bad_boundary_batches: None,
                    live_checkpoint_batch: false,
                }
                .into_stub(),
            );
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

pub(crate) struct RotatingFactory {
    pub(crate) scopes: Mutex<VecDeque<Vec<CursorScope>>>,
    pub(crate) established: Arc<Mutex<Vec<CursorScope>>>,
    pub(crate) closed: Arc<AtomicUsize>,
    pub(crate) closed_generations: Arc<Mutex<Vec<usize>>>,
    pub(crate) subscribed: SubscriptionCalls,
    pub(crate) unsubscribed: Arc<Mutex<Vec<(usize, SubscriptionHandle)>>>,
    pub(crate) unsubscribe_failures: Arc<AtomicUsize>,
    pub(crate) lifecycle_calls: Arc<Mutex<Vec<usize>>>,
    pub(crate) opens: AtomicUsize,
}

impl AccountFactory for RotatingFactory {
    fn open(
        &self,
        _account_id: AccountId,
    ) -> AccountFuture<Result<bifrost_types::OpenedAccount, AccountError>> {
        let scopes = self
            .scopes
            .lock()
            .expect("scopes lock")
            .pop_front()
            .expect("one scope set per expected open");
        let generation = self.opens.fetch_add(1, Ordering::SeqCst);
        let established = Arc::clone(&self.established);
        let closed = Arc::clone(&self.closed);
        let closed_generations = Arc::clone(&self.closed_generations);
        let subscribed = Arc::clone(&self.subscribed);
        let unsubscribed = Arc::clone(&self.unsubscribed);
        let unsubscribe_failures = Arc::clone(&self.unsubscribe_failures);
        let lifecycle_calls = Arc::clone(&self.lifecycle_calls);
        Box::pin(async move {
            let mut account_caps = caps();
            // `subscribe_push` refuses an account with no effective push
            // lanes, and a lane-less capability is `None`, so the first
            // incarnation must advertise a real push capability to be
            // subscribable. It stays distinct from the replacement's so the
            // capability-snapshot-refresh assertion still observes a change.
            account_caps.push = if generation == 0 {
                PushCapability::Webhook
            } else {
                PushCapability::InProcess
            };
            let account: Arc<dyn Account> = Arc::new(
                HealAccount {
                    caps: account_caps,
                    scopes,
                    established,
                    closed,
                    generation,
                    closed_generations,
                    subscribed,
                    unsubscribed,
                    unsubscribe_failures,
                    lifecycle_calls,
                    lifecycle_script: Arc::new(Mutex::new(VecDeque::new())),
                    degraded_contacts: false,
                    close_gate: no_close_gate(),
                    bad_boundary_batches: None,
                    live_checkpoint_batch: false,
                }
                .into_stub(),
            );
            Ok(bifrost_types::OpenedAccount::complete(account))
        })
    }
}

impl HealAccount {
    pub(crate) fn into_stub(self) -> common::StubAccount {
        let mut stub = common::StubAccount::new(self.scopes.clone());
        stub.caps = self.caps.clone();
        stub.established = Arc::clone(&self.established);
        stub.establishment =
            |scope| CursorEstablishment::Ready(cursor_for(scope, b"freshly-established"));
        stub.push_lane_specs = common::StubAccount::registration_lanes();
        let config = Arc::new(self);
        let config_for_hook = Arc::clone(&config);
        stub.lanes.changes.changes_stream = Some(Arc::new(move |cursor| {
            let config = &config_for_hook;
            if config.live_checkpoint_batch {
                let items = vec![SyncEvent::Batch(Batch {
                    items: Vec::new(),
                    page_boundary: PageBoundary::Final,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: Some(Checkpoint::Change(cursor)),
                })];
                return Box::pin(stream::iter(items));
            }
            let Some(counter) = config.bad_boundary_batches.clone() else {
                return Box::pin(stream::empty());
            };
            Box::pin(stream::repeat_with(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                SyncEvent::Batch(Batch {
                    items: Vec::new(),
                    page_boundary: PageBoundary::Partial,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: Some(Checkpoint::Change(cursor.clone())),
                })
            }))
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.lifecycle.scope_lifecycle_stream = Some(Arc::new(move || {
            let config = &config_for_hook;
            config
                .lifecycle_calls
                .lock()
                .expect("lifecycle calls lock")
                .push(config.generation);
            let scripted: Vec<ScopeLifecycleEvent> = config
                .lifecycle_script
                .lock()
                .expect("lifecycle script lock")
                .drain(..)
                .collect();
            Box::pin(stream::iter(scripted))
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.lifecycle.close = Some(Arc::new(move || {
            let config = &config_for_hook;
            config.closed.fetch_add(1, Ordering::SeqCst);
            config
                .closed_generations
                .lock()
                .expect("closed generations lock")
                .push(config.generation);
            let gate = config.close_gate.lock().expect("close gate lock").take();
            Box::pin(async move {
                if let Some(gate) = gate {
                    let _ = gate.entered.send(());
                    let _ = gate.release.await;
                }
                Ok(())
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.push.push_subscribe = Some(Arc::new(move |scopes| {
            let config = &config_for_hook;
            config
                .subscribed
                .lock()
                .expect("subscribed lock")
                .push((config.generation, scopes.to_vec()));
            let handle = SubscriptionHandle(format!("generation-{}", config.generation));
            let scopes = scopes.to_vec();
            Box::pin(async move {
                Ok(
                    bifrost_types::PushSubscription::all_succeeded(handle, &scopes)
                        .expect("test push subscription covers every requested scope"),
                )
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.push.push_unsubscribe = Some(Arc::new(move |handle| {
            let config = &config_for_hook;
            config
                .unsubscribed
                .lock()
                .expect("unsubscribed lock")
                .push((config.generation, handle));
            let mut remaining = config.unsubscribe_failures.load(Ordering::SeqCst);
            let fail = loop {
                if remaining == 0 {
                    break false;
                }
                match config.unsubscribe_failures.compare_exchange(
                    remaining,
                    remaining - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break true,
                    Err(actual) => remaining = actual,
                }
            };
            Box::pin(async move {
                if fail {
                    Err(unsupported(
                        bifrost_types::AccountOperation::PushUnsubscribe,
                    ))
                } else {
                    Ok(())
                }
            })
        }));
        let config_for_hook = Arc::clone(&config);
        stub.lanes.pages.contacts_list = Some(Arc::new(move |_address_book, _page_cursor| {
            let config = &config_for_hook;
            if !config.degraded_contacts {
                // A successful page with both lanes empty - the shape that
                // must stay silent. An `Err` here would exercise the `?`
                // instead of the emptiness guard.
                return Box::pin(async { Ok(Page::single(Vec::new(), Vec::new(), Vec::new())) });
            }
            // A walk that returned some cards, could not materialize one
            // resource, and gave up on a whole address book part way
            // through - the DAV shape this test models.
            Box::pin(async {
                Ok(Page {
                    truncated: false,
                    items: Vec::new(),
                    next_cursor: None,
                    estimated_total: None,
                    failed_ids: vec!["urn:uuid:unparseable-vcard".to_owned()],
                    skipped_scopes: vec![bifrost_types::SkippedScope {
                        scope: ErrorScope::Mailbox {
                            id: "shared-book".into(),
                        },
                        error: unsupported(bifrost_types::AccountOperation::ContactsList),
                    }],
                })
            })
        }));
        stub
    }
}
