use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::connection::{DriverTask, ImapConnection};
use crate::error::Error;
use crate::types::MailboxName;

use super::factory::ImapAccountConfig;

/// Everything a [`Connector`] needs for one dial.
pub(crate) struct ConnectRequest {
    /// The account's IMAP config with keepalive disabled: `dial_admitted`
    /// sets keepalive only after the connection is registered.
    pub(crate) config: crate::ImapConfig,
    pub(crate) meter: Option<bifrost_net::MeterSinkHandle>,
    pub(crate) bandwidth_budget: bifrost_net::BandwidthBudget,
    pub(crate) leases: crate::connection::LeaseHandoff,
}

/// The pool's way of opening a transport (the factory passes
/// `production_connector`, tests a scripted one).
///
/// Contract: spawn the connection's driver as the final step, with
/// `req.leases.take()` handed to the spawn and no await between the take and
/// the spawn, and return immediately after the spawn. `dial_admitted`
/// registers the connection only once the connector has returned, so a
/// connector that awaited after spawning would, on cancellation, drop an
/// unregistered live connection: close would never join it, and it would run
/// a graceful LOGOUT after `Account::close()` returned.
pub(crate) type Connector = Arc<
    dyn Fn(ConnectRequest) -> futures::future::BoxFuture<'static, Result<ImapConnection, Error>>
        + Send
        + Sync,
>;

/// The real network connector: `connect_config_metered` with the request's
/// leases. It meets the contract only because the request's keepalive is
/// disabled: with keepalive set, `connect_config_metered` awaits keepalive
/// setup after the spawn.
pub(crate) fn production_connector() -> Connector {
    Arc::new(|req| {
        async move {
            ImapConnection::connect_config_metered(
                &req.config,
                req.meter,
                Some(req.bandwidth_budget),
                &req.leases,
            )
            .await
        }
        .boxed()
    })
}

struct PoolMember {
    conn: Arc<ImapConnection>,
    selected: Option<MailboxName>,
}

struct SessionRegistration {
    connection: std::sync::Weak<ImapConnection>,
    driver: Arc<DriverTask>,
}

/// Under this lock `parked` and `waiters` are never both non-empty: a member
/// is parked only when nobody waits, and an acquisition registers as a
/// waiter only when nothing is parked or someone already waits.
struct IdleState {
    /// Each parked member still owns its session permit through its driver
    /// exit slot; parking releases no capacity.
    parked: Vec<PoolMember>,
    /// Acquisitions waiting for a returned member, oldest first.
    waiters: VecDeque<Waiter>,
    next_waiter: u64,
}

struct Waiter {
    id: u64,
    tx: tokio::sync::oneshot::Sender<PoolMember>,
}

/// Built under the idle lock in the same synchronous section that pushes its
/// `Waiter`, never inside a later await, so a future dropped before its next
/// poll still runs this guard. Drop removes the entry if it is still queued,
/// then takes any member already handed here and sends it back through
/// `return_member` (next waiter, or parked) instead of losing it.
struct WaiterGuard {
    pool: Arc<PoolInner>,
    id: u64,
    rx: tokio::sync::oneshot::Receiver<PoolMember>,
}

impl WaiterGuard {
    fn take_handoff(&mut self) -> Option<PoolMember> {
        let mut idle = self.pool.idle.lock().expect("pool lock poisoned");
        idle.waiters.retain(|w| w.id != self.id);
        self.rx.try_recv().ok()
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        if let Some(member) = self.take_handoff() {
            self.pool.return_member(member);
        }
    }
}

/// Capacity granted for one fresh session, not yet dialed. Dropping it
/// releases the permit and the extra values at once: no session exists yet.
pub(crate) struct FreshReservation {
    pool: Arc<PoolInner>,
    permit: OwnedSemaphorePermit,
    extra: Vec<Box<dyn Send>>,
    waited: bool,
}

impl FreshReservation {
    /// Whether capacity was not available on the first attempt.
    pub(crate) fn waited(&self) -> bool {
        self.waited
    }

    /// Fold in a wait that happened before the pool was asked (the push slot).
    pub(crate) fn with_prior_wait(mut self, waited: bool) -> Self {
        self.waited |= waited;
        self
    }

    /// Dial the session. The permit and extras move into the dial task
    /// synchronously and from there into the new driver's exit slot.
    pub(crate) async fn dial(self) -> Result<Arc<ImapConnection>, Error> {
        self.pool.dial(self.permit, self.extra).await
    }
}

pub(crate) struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    /// One permit per physical session, `config.session_cap()` in total.
    /// Invariant: every connecting, live or tailing session owns exactly one
    /// permit, from before its connector starts until its driver has exited
    /// (the permit lives in the driver's exit slot), and every push session
    /// additionally owns one push slot over the same interval. The physical
    /// cap is enforced by this semaphore alone; parked adoption only avoids
    /// dials.
    sessions: Arc<Semaphore>,
    connector: Connector,
    idle: Mutex<IdleState>,
    config: Arc<ImapAccountConfig>,
    meter: Option<bifrost_net::MeterSinkHandle>,
    bandwidth_budget: bifrost_net::BandwidthBudget,
    closed: std::sync::atomic::AtomicBool,
    /// The account's QRESYNC policy (`ImapAccount::qresync_enabled`). Lives
    /// here because `dial` reads it: a member dialed while it is set is
    /// brought to QRESYNC before it is handed out.
    qresync_policy: std::sync::atomic::AtomicBool,
    /// Every registered session minted by this pool, including checked-out and
    /// push sessions. The weak connection reference does not extend the
    /// command handle's lifetime; the shared driver owner does keep its join
    /// handle available so close can wait for a detached graceful LOGOUT.
    ///
    /// This mutex is also the linearization point for closure: `close`
    /// stores `closed` before it drains here, and registration re-reads
    /// `closed` while holding it. A dial that reaches the lock first is
    /// therefore seen by the drain; one that reaches it after the drain
    /// observes the flag and terminates itself. Nothing can land a live
    /// session on the far side of a completed close.
    registry: Mutex<Vec<SessionRegistration>>,
    // A read guard spans connect, authentication and initialization. Close takes
    // the write guard before draining, so no admitted dial outlives completion.
    dials: tokio::sync::RwLock<()>,
    cancel_dials: tokio_util::sync::CancellationToken,
    close_completion:
        std::sync::OnceLock<futures::future::Shared<futures::future::BoxFuture<'static, ()>>>,
}

impl Pool {
    pub(super) fn sieve_meter(&self) -> Option<bifrost_net::MeterSinkHandle> {
        self.inner.meter.clone()
    }

    pub(crate) fn new(
        config: Arc<ImapAccountConfig>,
        primed: ImapConnection,
        connector: Connector,
        meter: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_budget: bifrost_net::BandwidthBudget,
    ) -> Self {
        let sessions = Arc::new(Semaphore::new(config.session_cap()));
        let permit = Arc::clone(&sessions)
            .try_acquire_owned()
            .expect("fresh session budget");
        primed.driver_task().retain_until_exit(Box::new(permit));
        let primed = Arc::new(primed);
        let primed_weak = Arc::downgrade(&primed);
        let primed_driver = primed.driver_task();
        Self {
            inner: Arc::new(PoolInner {
                sessions,
                idle: Mutex::new(IdleState {
                    parked: vec![PoolMember {
                        conn: primed,
                        selected: None,
                    }],
                    waiters: VecDeque::new(),
                    next_waiter: 0,
                }),
                connector,
                config,
                meter,
                bandwidth_budget,
                closed: std::sync::atomic::AtomicBool::new(false),
                qresync_policy: std::sync::atomic::AtomicBool::new(false),
                registry: Mutex::new(vec![SessionRegistration {
                    connection: primed_weak,
                    driver: primed_driver,
                }]),
                dials: tokio::sync::RwLock::new(()),
                cancel_dials: tokio_util::sync::CancellationToken::new(),
                close_completion: std::sync::OnceLock::new(),
            }),
        }
    }

    /// The account's QRESYNC policy. See `ImapAccount::qresync_enabled`.
    pub(crate) fn qresync_policy(&self) -> bool {
        self.inner
            .qresync_policy
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn set_qresync_policy(&self, enabled: bool) {
        self.inner
            .qresync_policy
            .store(enabled, std::sync::atomic::Ordering::Release);
    }

    pub(crate) async fn checkout_for_folder(
        &self,
        folder: &MailboxName,
    ) -> Result<PooledConn, Error> {
        self.checkout(Some(folder)).await
    }

    pub(crate) async fn checkout_any(&self) -> Result<PooledConn, Error> {
        self.checkout(None).await
    }

    async fn checkout(&self, affinity: Option<&MailboxName>) -> Result<PooledConn, Error> {
        Ok(PooledConn {
            member: Some(self.inner.acquire_member(affinity).await?),
            pool: Arc::clone(&self.inner),
        })
    }

    /// Capacity for a fresh session that never comes from the parked list
    /// (push). `extra` values live as long as the session's transport.
    pub(crate) async fn reserve_fresh(
        &self,
        extra: Vec<Box<dyn Send>>,
    ) -> Result<FreshReservation, Error> {
        self.inner.reserve_fresh(extra, None).await
    }

    /// `reserve_fresh(extra)`, then dial.
    pub(crate) async fn acquire_fresh(
        &self,
        extra: Vec<Box<dyn Send>>,
    ) -> Result<Arc<ImapConnection>, Error> {
        self.reserve_fresh(extra).await?.dial().await
    }

    #[cfg(test)]
    pub(crate) fn available_sessions(&self) -> usize {
        self.inner.sessions.available_permits()
    }

    pub(crate) async fn close(&self) {
        let completion = self
            .inner
            .close_completion
            .get_or_init(|| {
                self.inner
                    .closed
                    .store(true, std::sync::atomic::Ordering::Release);
                self.inner.sessions.close();
                self.inner
                    .idle
                    .lock()
                    .expect("pool lock poisoned")
                    .waiters
                    .clear();
                self.inner.cancel_dials.cancel();
                let inner = Arc::clone(&self.inner);
                let task = tokio::spawn(async move { inner.drain().await });
                async move { task.await.expect("pool teardown task panicked") }
                    .boxed()
                    .shared()
            })
            .clone();
        completion.await;
    }
}

impl PoolInner {
    async fn drain(&self) {
        let _dials = self.dials.write().await;
        let members = {
            let mut idle = self.idle.lock().expect("pool lock poisoned");
            std::mem::take(&mut idle.parked)
        };
        // Drain under the same mutex registration re-checks `closed` under.
        // Exclusive dial admission means every admitted connect/authenticate
        // task has finished or terminated its unclaimed session before here.
        let sessions: Vec<_> = self
            .registry
            .lock()
            .expect("pool sessions lock poisoned")
            .drain(..)
            .map(|session| (session.connection.upgrade(), session.driver))
            .collect();
        drop(members);
        // LOGOUT is best effort and the pool is already gated shut, so the
        // drain must not be able to hold `Account::close` open. Members go
        // out concurrently (a silent peer must not make the others wait its
        // turn) under one `command_timeout` for the whole drain, which is
        // the bound every other command in the account layer already has.
        let drain =
            futures::future::join_all(sessions.iter().filter_map(|(conn, _)| conn.as_ref()).map(
                |conn| async move {
                    let _ = conn.logout().await;
                },
            ));
        let _ = tokio::time::timeout(self.config.imap.command_timeout, drain).await;
        // Detached drivers are joined concurrently: each owns its own
        // LOGOUT_DRAIN_TIMEOUT bound, and a sequential walk would let N silent
        // peers hold close open for N times that bound.
        futures::future::join_all(sessions.into_iter().map(|(connection, driver)| async move {
            if let Some(conn) = connection {
                conn.terminate().await;
            } else {
                driver
                    .finish_or_abort(crate::connection::LOGOUT_DRAIN_TIMEOUT)
                    .await;
            }
        }))
        .await;
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// A data checkout: adopt a parked member (affinity first), else wait for
    /// a returned member or a session permit, whichever comes first.
    ///
    /// Fairness: both queues are FIFO - the waiter queue for returned
    /// members, tokio's semaphore for released sessions. A new acquisition
    /// never adopts while anyone waits, so a re-checkout loop cannot overtake
    /// a queued waiter, and each acquisition holds one semaphore position for
    /// its whole call. The remaining non-FIFO edge is between the two queues:
    /// a waiter granted a permit and a member at once keeps the member and
    /// releases the permit to the next semaphore waiter. No physical-cap
    /// property depends on fairness; latency at the cap does.
    async fn acquire_member(
        self: &Arc<Self>,
        affinity: Option<&MailboxName>,
    ) -> Result<PoolMember, Error> {
        let acquire = Arc::clone(&self.sessions).acquire_owned();
        tokio::pin!(acquire);
        loop {
            let mut guard = {
                let mut idle = self.idle.lock().expect("pool lock poisoned");
                if self.is_closed() {
                    return Err(Error::closed());
                }
                idle.parked.retain(|m| m.conn.is_alive());
                if idle.waiters.is_empty() {
                    let idx = idle
                        .parked
                        .iter()
                        .position(|m| m.selected.as_ref() == affinity);
                    if let Some(member) = idx
                        .map(|i| idle.parked.swap_remove(i))
                        .or_else(|| idle.parked.pop())
                    {
                        return Ok(member);
                    }
                }
                self.waiter(&mut idle)
            };
            tokio::select! {
                biased;
                member = &mut guard.rx => match member {
                    Ok(m) if m.conn.is_alive() => return Ok(m),
                    Ok(_) => continue,
                    Err(_) => return Err(Error::closed()),
                },
                permit = &mut acquire => {
                    let permit = permit.map_err(|_| Error::closed())?;
                    if self.is_closed() { return Err(Error::closed()); }
                    // A dead member's permit follows its own driver; the
                    // grant this acquisition already holds dials instead.
                    if let Some(member) = guard.take_handoff()
                        && member.conn.is_alive()
                    {
                        drop(permit);
                        return Ok(member);
                    }
                    return Ok(PoolMember { conn: self.dial(permit, Vec::new()).await?, selected: None });
                }
            }
        }
    }

    fn waiter(self: &Arc<Self>, idle: &mut IdleState) -> WaiterGuard {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let id = idle.next_waiter;
        idle.next_waiter = idle.next_waiter.wrapping_add(1);
        idle.waiters.push_back(Waiter { id, tx });
        WaiterGuard {
            pool: Arc::clone(self),
            id,
            rx,
        }
    }

    /// Hand a reusable member to the oldest waiter still listening, or park
    /// it when nobody waits. A closed pool drops it. No await.
    fn return_member(&self, mut member: PoolMember) {
        let mut idle = self.idle.lock().expect("pool lock poisoned");
        if self.is_closed() {
            return;
        }
        while let Some(waiter) = idle.waiters.pop_front() {
            match waiter.tx.send(member) {
                Ok(()) => return,
                Err(m) => member = m,
            }
        }
        idle.parked.push(member);
    }

    /// Bounded LOGOUT, then abort and join: once this returns the member's
    /// driver has exited and its permit is back. The member is still
    /// registered, so a concurrent close may terminate it too; the second
    /// terminate waits on the driver handle mutex for the first join.
    async fn retire(&self, member: PoolMember) {
        let _ = tokio::time::timeout(self.config.imap.command_timeout, member.conn.logout()).await;
        member.conn.terminate().await;
    }

    /// Capacity for a fresh session that never comes from the parked list.
    /// `retiring` is the caller's own member (member replacement), retired
    /// after this acquisition has queued on the semaphore.
    ///
    /// The semaphore acquisition is created once and polled BEFORE any
    /// retire, so this call is already queued when a retired driver exits.
    /// Tokio's semaphore assigns a released permit to the oldest queued
    /// acquisition even while it is not being polled, so the room a retire
    /// makes comes back here ahead of anyone who queued during the retire -
    /// but not ahead of an acquisition that was already queued before this
    /// call started, which is older and takes it by design.
    async fn reserve_fresh(
        self: &Arc<Self>,
        extra: Vec<Box<dyn Send>>,
        retiring: Option<PoolMember>,
    ) -> Result<FreshReservation, Error> {
        if self.is_closed() {
            return Err(Error::closed());
        }
        let acquire = Arc::clone(&self.sessions).acquire_owned();
        tokio::pin!(acquire);
        let immediate = futures::poll!(&mut acquire);
        // Replacement retires its own selected member even when a spare
        // permit was granted at once: the old session must be gone before
        // the replacement dials and the caller's DELETE / RENAME goes out
        // (RFC 2683 2.2.2), rather than tailing its LOGOUT concurrently.
        if let Some(own) = retiring {
            self.retire(own).await;
        }
        if let std::task::Poll::Ready(permit) = immediate {
            return self.reservation(permit.map_err(|_| Error::closed())?, extra, false);
        }
        loop {
            if let std::task::Poll::Ready(permit) = futures::poll!(&mut acquire) {
                return self.reservation(permit.map_err(|_| Error::closed())?, extra, true);
            }
            let (taken, mut guard) = {
                let mut idle = self.idle.lock().expect("pool lock poisoned");
                if self.is_closed() {
                    return Err(Error::closed());
                }
                idle.parked.retain(|m| m.conn.is_alive());
                if idle.waiters.is_empty() && !idle.parked.is_empty() {
                    (idle.parked.pop(), None)
                } else {
                    (None, Some(self.waiter(&mut idle)))
                }
            };
            if let Some(member) = taken {
                self.retire(member).await;
                continue;
            }
            let guard = guard.as_mut().expect("fresh waiter present");
            tokio::select! {
                biased;
                permit = &mut acquire => {
                    let permit = permit.map_err(|_| Error::closed())?;
                    if let Some(member) = guard.take_handoff() { self.return_member(member); }
                    return self.reservation(permit, extra, true);
                }
                member = &mut guard.rx => match member {
                    Ok(m) => self.retire(m).await,
                    Err(_) => return Err(Error::closed()),
                },
            }
        }
    }

    fn reservation(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
        extra: Vec<Box<dyn Send>>,
        waited: bool,
    ) -> Result<FreshReservation, Error> {
        if self.is_closed() {
            return Err(Error::closed());
        }
        Ok(FreshReservation {
            pool: Arc::clone(self),
            permit,
            extra,
            waited,
        })
    }

    async fn dial(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
        extra: Vec<Box<dyn Send>>,
    ) -> Result<Arc<ImapConnection>, Error> {
        let mut config = self.config.imap.clone();
        config.keepalive = None;
        let connector = Arc::clone(&self.connector);
        let meter = self.meter.clone();
        let bandwidth_budget = self.bandwidth_budget.clone();
        self.dial_with(permit, extra, move |leases| {
            connector(ConnectRequest {
                config,
                meter,
                bandwidth_budget,
                leases,
            })
        })
        .await
    }

    async fn dial_with<C, F>(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
        mut extra: Vec<Box<dyn Send>>,
        connect: C,
    ) -> Result<Arc<ImapConnection>, Error>
    where
        C: FnOnce(crate::connection::LeaseHandoff) -> F + Send + 'static,
        F: std::future::Future<Output = Result<ImapConnection, Error>> + Send + 'static,
    {
        extra.push(Box::new(permit));
        let handoff = crate::connection::LeaseHandoff::new(extra);
        let (mut tx, rx) = tokio::sync::oneshot::channel();
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            let _dial = inner.dials.read().await;
            let mut connected = None;
            let result = tokio::select! {
                biased;
                _ = inner.cancel_dials.cancelled() => Err(Error::closed()),
                _ = tx.closed() => Err(Error::closed()),
                result = inner.dial_admitted(connect(handoff.clone()), &mut connected) => result,
            };
            // Drop the connector future (and any pre-spawn transport) before
            // releasing leases. After spawn, the exit slot owns the leases.
            drop(handoff);
            match result {
                Ok(conn) => {
                    if let Err(Ok(conn)) = tx.send(Ok(conn)) {
                        conn.terminate().await;
                    }
                }
                Err(error) => {
                    if let Some(conn) = connected {
                        conn.terminate().await;
                    }
                    let _ = tx.send(Err(error));
                }
            }
        });
        rx.await
            .map_err(|_| Error::driver_gone_at(bifrost_types::TransmissionState::Unsent))?
    }

    async fn dial_admitted<F>(
        &self,
        connect: F,
        connected: &mut Option<Arc<ImapConnection>>,
    ) -> Result<Arc<ImapConnection>, Error>
    where
        F: std::future::Future<Output = Result<ImapConnection, Error>>,
    {
        if self.is_closed() {
            return Err(Error::closed());
        }
        let conn = Arc::new(connect.await?);
        *connected = Some(Arc::clone(&conn));
        if !self.register(&conn) {
            return Err(Error::closed());
        }
        if let Some(keepalive) = self.config.imap.keepalive {
            conn.set_keepalive(keepalive).await?;
        }
        conn.authenticate_best(
            &self.config.credentials,
            &self.config.auth_policy,
            self.config.imap.command_timeout,
        )
        .await?;
        if self
            .qresync_policy
            .load(std::sync::atomic::Ordering::Acquire)
        {
            enable_qresync(&conn, self.config.imap.command_timeout).await?;
        }
        Ok(conn)
    }

    /// Record a freshly minted session, pruning drivers that have completed.
    /// A missing connection handle may still have a graceful LOGOUT in flight,
    /// so its registration remains until that driver finishes. Pruning keeps
    /// reconnecting IDLE workers from retaining completed tasks per dial.
    /// Returns `false` when the pool closed before this session could be
    /// registered, meaning the caller owns tearing it down (the drain in
    /// `close` has already run and will not see it).
    fn register(&self, conn: &Arc<ImapConnection>) -> bool {
        let mut sessions = self.registry.lock().expect("pool sessions lock poisoned");
        if self.is_closed() {
            return false;
        }
        sessions.retain(|session| !session.driver.is_finished());
        sessions.push(SessionRegistration {
            connection: Arc::downgrade(conn),
            driver: conn.driver_task(),
        });
        true
    }
}

/// Bring a freshly authenticated connection to QRESYNC, before it has run a
/// single mailbox command - the one moment `ENABLE` is always legal (RFC 5161
/// Section 3.1 forbids it once any mailbox has been selected).
///
/// The one initialization rule, for the factory's primed connection and every
/// pool dial alike. Whether the connection DOES QRESYNC afterwards is its
/// committed profile (`ServerProfile::enabled("QRESYNC")`), never this
/// function's result: a server can echo `* ENABLED QRESYNC` and then refuse
/// the command, and the echo is what tells us its wire behaviour changed.
///
/// `Ok` covers every outcome that leaves a healthy connection: a server
/// that cannot negotiate QRESYNC (no QRESYNC or no ENABLE capability; the
/// connection simply serves CONDSTORE), a completed ENABLE, and a completed
/// refusal (`NO` / `BAD`) on a connection still reusable. `Err` means the
/// connection must not be handed out: the exchange did not finish (a timeout,
/// the ENABLE withdrawn unsent or abandoned with the driver still inside it)
/// or the connection is dead. The caller retires it.
pub(crate) async fn enable_qresync(
    conn: &ImapConnection,
    timeout: std::time::Duration,
) -> Result<(), Error> {
    let profile = conn.server_profile();
    if !profile.supports_qresync() || !profile.supports(crate::types::Capability::Enable) {
        return Ok(());
    }
    match conn.enable(&["QRESYNC"], timeout).await {
        Ok(_) => Ok(()),
        Err(Error::No { .. } | Error::Bad { .. }) if conn.is_reusable() => Ok(()),
        Err(error) => Err(error),
    }
}

pub(crate) struct PooledConn {
    member: Option<PoolMember>,
    pool: Arc<PoolInner>,
}

impl PooledConn {
    /// The live connection of this checkout.
    ///
    /// Panics if the member has been taken: after `discard()`, or after a
    /// `deselect_target` fallback whose redial failed (the old member is
    /// already logged out by then, so there is nothing valid to return).
    /// Both are terminal for the checkout - every caller propagates the
    /// error and drops the `PooledConn` - so the panic marks a caller
    /// reusing a checkout it was told is dead, which is a bug worth being
    /// loud about rather than surfacing as a quiet dead-connection error.
    pub(crate) fn connection(&self) -> &ImapConnection {
        &self
            .member
            .as_ref()
            .expect("pooled connection present")
            .conn
    }

    pub(crate) fn set_selected(&mut self, folder: MailboxName) {
        self.set_selected_affinity(Some(folder));
    }

    /// Record which mailbox this member's session has selected, or that it
    /// has none. `deselect_target` trusts this record to decide whether an
    /// UNSELECT must precede a DELETE or RENAME, so it must follow the
    /// session through failures too, not only through successful SELECTs.
    pub(crate) fn set_selected_affinity(&mut self, folder: Option<MailboxName>) {
        if let Some(member) = &mut self.member {
            member.selected = folder;
        }
    }

    /// The mailbox this member's session is recorded as having selected.
    #[cfg(test)]
    pub(crate) fn selected_affinity(&self) -> Option<&MailboxName> {
        self.member
            .as_ref()
            .and_then(|member| member.selected.as_ref())
    }

    /// Make a mailbox-management command safe for a target that this pooled
    /// member happens to have selected. RFC 3501 permits servers to reject
    /// DELETE and RENAME for the selected mailbox. Prefer UNSELECT because
    /// it keeps the checkout on its existing connection; old servers without
    /// UNSELECT replace the selected member with a freshly dialed connection.
    pub(crate) async fn deselect_target(
        &mut self,
        target: &MailboxName,
        timeout: std::time::Duration,
    ) -> Result<(), Error> {
        let selected_target = self
            .member
            .as_ref()
            .is_some_and(|member| member.selected.as_ref() == Some(target));
        if !selected_target {
            return Ok(());
        }

        match self.connection().unselect(timeout).await {
            Ok(()) => {
                self.member
                    .as_mut()
                    .expect("pooled connection present")
                    .selected = None;
                Ok(())
            }
            Err(Error::MissingCapability(_)) => self.replace_selected_member().await,
            Err(error) => Err(error),
        }
    }

    /// Replace this checkout's connection with a freshly dialed one.
    ///
    /// The old member is retired (bounded LOGOUT, then terminate) after the
    /// replacement's capacity request has queued and BEFORE the replacement
    /// is dialed. Dialing first would hold cap+1 physical connections for
    /// the duration of the handshake, and a server that enforces its
    /// per-user connection limit rejects exactly the fallback that a
    /// pre-UNSELECT server needs. It would also keep the old session
    /// SELECTed on the target while the DELETE that follows is issued, which
    /// RFC 2683 2.2.2 warns against. The replacement is always a fresh dial,
    /// never a parked member, so it is unselected by construction. At the
    /// cap it can wait: the room its own member frees goes to an
    /// acquisition that was already queued before it, if there is one.
    async fn replace_selected_member(&mut self) -> Result<(), Error> {
        let old = self.member.take();
        self.member = Some(PoolMember {
            conn: self
                .pool
                .reserve_fresh(Vec::new(), old)
                .await?
                .dial()
                .await?,
            selected: None,
        });
        Ok(())
    }

    pub(crate) fn discard(&mut self) {
        self.member.take();
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        let Some(member) = self.member.take() else {
            return;
        };
        if matches!(
            member.conn.session_state(),
            crate::connection::SessionState::Logout
        ) || !member.conn.is_reusable()
        {
            return;
        }
        self.pool.return_member(member);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use super::super::factory::ImapAccountConfig;
    use super::{Pool, PoolMember, SessionRegistration};
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, tag_of,
    };
    use crate::error::Error;
    use crate::types::{AuthPolicy, Capability, Credentials, MailboxName};

    /// A config whose dial target is unroutable. Pools built on it dial only
    /// through an injected connector (`pool_of` passes one that refuses), so
    /// a test that must be served from parked members fails instead of
    /// silently dialing.
    fn config() -> Arc<ImapAccountConfig> {
        let mut cfg = ImapAccountConfig::new(
            crate::ImapConfig::plaintext("test.invalid"),
            Credentials::password("user", "pass"),
            AuthPolicy::default().allow_cleartext_without_tls(),
        );
        cfg.imap.keepalive = None;
        Arc::new(cfg)
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_two_pool_members_share_one_inbound_rate() {
        use crate::connection::test_support::driver_pair_metered;
        use tokio::time::Instant;
        let budget = bifrost_net::BandwidthBudget::default();
        let (a, mut peer_a) =
            driver_pair_metered(&preauth_greeting("IMAP4rev1"), budget.clone()).await;
        let (b, mut peer_b) =
            driver_pair_metered(&preauth_greeting("IMAP4rev1"), budget.clone()).await;
        let pool = Pool::new(
            Arc::new(ImapAccountConfig {
                pool_cap: 2,
                ..(*config()).clone()
            }),
            a,
            crate::account::test_support::refusing_connector(),
            None,
            budget.clone(),
        );
        b.driver_task().retain_until_exit(Box::new(
            Arc::clone(&pool.inner.sessions)
                .try_acquire_owned()
                .unwrap(),
        ));
        pool.inner.idle.lock().unwrap().parked.push(PoolMember {
            conn: Arc::new(b),
            selected: None,
        });
        let a = pool.checkout_any().await.unwrap();
        let b = pool.checkout_any().await.unwrap();
        budget.set_cap(Some(64));
        let start = Instant::now();
        async fn answer(peer: &mut tokio::io::DuplexStream) {
            let command = read_line(peer).await;
            let tag = tag_of(&command);
            respond(
                peer,
                &format!("{tag} OK {}\r\n", "x".repeat(64 - tag.len() - 6)),
            )
            .await;
        }
        let (a, b, (), ()) = tokio::join!(
            a.connection().noop(Duration::from_secs(10)),
            b.connection().noop(Duration::from_secs(10)),
            answer(&mut peer_a),
            answer(&mut peer_b),
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn direct_pool_close_survives_caller_cancellation() {
        let (primed, mut peer) = marked("ACL").await;
        let pool = pool_of(primed, 1);
        let held = pool.checkout_any().await.unwrap();
        let mut first = Box::pin(pool.close());
        assert!(futures::poll!(first.as_mut()).is_pending());
        let logout = read_line(&mut peer).await;
        assert!(logout.contains("LOGOUT"));
        drop(first);
        respond(
            &mut peer,
            &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
        )
        .await;
        pool.close().await;
        assert!(!held.connection().is_alive());
        assert!(pool.inner.registry.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn close_joins_a_detached_healthy_drivers_graceful_logout() {
        let (primed, mut peer) = marked("ACL").await;
        let pool = pool_of(primed, 1);
        let member = pool.inner.idle.lock().unwrap().parked.pop().unwrap();
        let PoolMember { conn: detached, .. } = member;
        drop(detached);

        let mut closing = Box::pin(pool.close());
        assert!(futures::poll!(closing.as_mut()).is_pending());
        let logout = read_line(&mut peer).await;
        assert!(logout.contains("LOGOUT"));
        assert!(
            futures::poll!(closing.as_mut()).is_pending(),
            "close must retain and await the healthy driver's detached LOGOUT",
        );

        respond(
            &mut peer,
            &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
        )
        .await;
        closing.await;
        assert!(pool.inner.registry.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn close_cancels_a_dial_before_registration() {
        let (primed, primed_peer) = marked("ACL").await;
        let pool = pool_of(primed, 2);
        drop(primed_peer);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut dial = Box::pin(
            pool.inner.dial_with(
                Arc::clone(&pool.inner.sessions)
                    .try_acquire_owned()
                    .unwrap(),
                Vec::new(),
                move |_handoff| async move {
                    started_tx.send(()).unwrap();
                    std::future::pending().await
                },
            ),
        );
        assert!(futures::poll!(dial.as_mut()).is_pending());
        started_rx.await.unwrap();
        pool.close().await;
        assert!(matches!(dial.await, Err(Error::Closed { .. })));
        assert!(pool.inner.registry.lock().unwrap().is_empty());
        assert_eq!(pool.available_sessions(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn close_cancels_authentication_already_on_the_wire() {
        let (primed, primed_peer) = marked("ACL").await;
        drop(primed_peer);
        let (peer_tx, peer_rx) = tokio::sync::oneshot::channel();
        let mut cfg = (*config()).clone();
        cfg.pool_cap = 2;
        cfg.imap.keepalive = None;
        cfg.auth_policy = AuthPolicy::default().allow_cleartext_without_tls();
        let pool = Pool::new(
            Arc::new(cfg),
            primed,
            crate::account::test_support::refusing_connector(),
            None,
            bifrost_net::BandwidthBudget::default(),
        );
        let mut dial = Box::pin(
            pool.inner.dial_with(
                Arc::clone(&pool.inner.sessions)
                    .try_acquire_owned()
                    .unwrap(),
                Vec::new(),
                move |handoff| async move {
                    let (late, peer) = crate::connection::test_support::driver_pair_leased(
                        b"* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN SASL-IR] ready\r\n",
                        &handoff,
                    )
                    .await;
                    peer_tx.send(peer).unwrap();
                    Ok(late)
                },
            ),
        );
        assert!(futures::poll!(dial.as_mut()).is_pending());
        let mut peer = peer_rx.await.unwrap();
        assert!(read_line(&mut peer).await.contains("AUTHENTICATE PLAIN"));
        let conn = pool
            .inner
            .registry
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .connection
            .upgrade()
            .unwrap();
        let started = tokio::time::Instant::now();
        pool.close().await;
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert!(!conn.is_alive());
        assert!(matches!(dial.await, Err(Error::Closed { .. })));
        assert_eq!(pool.available_sessions(), 2);
    }

    struct StalledTokenSource {
        entered: Arc<tokio::sync::Semaphore>,
    }

    impl bifrost_net::TokenSource for StalledTokenSource {
        fn current(
            &self,
        ) -> bifrost_types::AccountFuture<Result<bifrost_net::AccessToken, bifrost_net::Error>>
        {
            let entered = Arc::clone(&self.entered);
            Box::pin(async move {
                entered.add_permits(1);
                std::future::pending().await
            })
        }

        fn refresh(
            &self,
        ) -> bifrost_types::AccountFuture<Result<bifrost_net::AccessToken, bifrost_net::Error>>
        {
            self.current()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn close_cancels_stalled_token_lookup_and_joins_its_driver() {
        let (primed, primed_peer) = marked("ACL").await;
        drop(primed_peer);
        let (peer_tx, peer_rx) = tokio::sync::oneshot::channel();
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let mut cfg = (*config()).clone();
        cfg.pool_cap = 2;
        cfg.imap.keepalive = None;
        cfg.credentials = Credentials::oauth2_source(
            "user",
            Arc::new(StalledTokenSource {
                entered: Arc::clone(&entered),
            }),
        );
        cfg.auth_policy = AuthPolicy::default().allow_cleartext_without_tls();
        let pool = Pool::new(
            Arc::new(cfg),
            primed,
            crate::account::test_support::refusing_connector(),
            None,
            bifrost_net::BandwidthBudget::default(),
        );
        let mut dial = Box::pin(
            pool.inner.dial_with(
                Arc::clone(&pool.inner.sessions)
                    .try_acquire_owned()
                    .unwrap(),
                Vec::new(),
                move |handoff| async move {
                    let (late, peer) = crate::connection::test_support::driver_pair_leased(
                        b"* OK [CAPABILITY IMAP4rev1 AUTH=XOAUTH2 SASL-IR] ready\r\n",
                        &handoff,
                    )
                    .await;
                    peer_tx.send(peer).unwrap();
                    Ok(late)
                },
            ),
        );
        assert!(futures::poll!(dial.as_mut()).is_pending());
        let _peer = peer_rx.await.unwrap();
        entered.acquire().await.unwrap().forget();
        let conn = pool
            .inner
            .registry
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .connection
            .upgrade()
            .unwrap();
        drop(dial);
        let started = tokio::time::Instant::now();
        pool.close().await;
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "close cancels instead of waiting for the token timeout"
        );
        assert!(!conn.is_alive());
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_token_lookup_has_its_own_timeout() {
        let (conn, _peer) =
            driver_pair(b"* OK [CAPABILITY IMAP4rev1 AUTH=XOAUTH2 SASL-IR] ready\r\n").await;
        let credentials = Credentials::oauth2_source(
            "user",
            Arc::new(StalledTokenSource {
                entered: Arc::new(tokio::sync::Semaphore::new(0)),
            }),
        );
        let timeout = Duration::from_secs(3);
        let started = tokio::time::Instant::now();
        let error = conn
            .authenticate_best(
                &credentials,
                &AuthPolicy::default().allow_cleartext_without_tls(),
                timeout,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Timeout { .. }));
        assert_eq!(started.elapsed(), timeout);
        conn.terminate().await;
    }

    #[tokio::test]
    async fn cancelled_checkouts_keep_capacity_until_the_dial_is_cancelled() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Active(Arc<AtomicUsize>);
        impl Drop for Active {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let (primed, _peer) = marked("ACL").await;
        let pool = pool_of(primed, 2);
        let active = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        for _ in 0..16 {
            let inner = Arc::clone(&pool.inner);
            let dial_active = Arc::clone(&active);
            let dial_entered = Arc::clone(&entered);
            let mut checkout = Box::pin(async move {
                let permit = Arc::clone(&inner.sessions)
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::closed())?;
                inner
                    .dial_with(permit, Vec::new(), |_| async move {
                        assert_eq!(dial_active.fetch_add(1, Ordering::SeqCst), 0);
                        let _active = Active(dial_active);
                        dial_entered.add_permits(1);
                        std::future::pending().await
                    })
                    .await
            });
            assert!(futures::poll!(checkout.as_mut()).is_pending());
            entered.acquire().await.unwrap().forget();
            drop(checkout);
            // No executor yield: cancellation has not reached the detached task.
            assert_eq!(pool.available_sessions(), 0);
            let permit = Arc::clone(&pool.inner.sessions)
                .acquire_owned()
                .await
                .unwrap();
            assert_eq!(active.load(Ordering::SeqCst), 0);
            drop(permit);
        }
    }

    fn folder(name: &str) -> MailboxName {
        MailboxName::new(name).unwrap()
    }

    /// A live connection tagged with an otherwise unused capability atom,
    /// so a checkout can be identified without writing a byte to the wire.
    ///
    /// The returned server end must stay alive for the duration of the
    /// test: dropping it ends the driver task, and the pool's liveness
    /// filter would then evict the member before affinity is consulted.
    async fn marked(mark: &str) -> (crate::ImapConnection, tokio::io::DuplexStream) {
        driver_pair(&preauth_greeting(&format!("IMAP4rev1 {mark}"))).await
    }

    fn pool_of(primed: crate::ImapConnection, pool_cap: usize) -> Pool {
        Pool::new(
            Arc::new(ImapAccountConfig {
                pool_cap,
                ..(*config()).clone()
            }),
            primed,
            crate::account::test_support::refusing_connector(),
            None,
            bifrost_net::BandwidthBudget::default(),
        )
    }

    /// Park an extra member directly. `Pool::new` accepts only one primed
    /// connection. Injected members must retain their own session lease.
    fn park(pool: &Pool, conn: crate::ImapConnection, selected: Option<MailboxName>) {
        conn.driver_task().retain_until_exit(Box::new(
            Arc::clone(&pool.inner.sessions)
                .try_acquire_owned()
                .unwrap(),
        ));
        let conn = Arc::new(conn);
        pool.inner
            .registry
            .lock()
            .unwrap()
            .push(SessionRegistration {
                connection: Arc::downgrade(&conn),
                driver: conn.driver_task(),
            });
        pool.inner
            .idle
            .lock()
            .unwrap()
            .parked
            .push(PoolMember { conn, selected });
    }

    fn set_primed_affinity(pool: &Pool, selected: MailboxName) {
        pool.inner.idle.lock().unwrap().parked[0].selected = Some(selected);
    }

    fn idle_len(pool: &Pool) -> usize {
        pool.inner.idle.lock().unwrap().parked.len()
    }

    fn idle_selection(pool: &Pool) -> Vec<Option<MailboxName>> {
        pool.inner
            .idle
            .lock()
            .unwrap()
            .parked
            .iter()
            .map(|member| member.selected.clone())
            .collect()
    }

    /// Checkout affinity is by selected mailbox, not by parking order.
    ///
    /// Two parked members, both usable; the one already SELECTed on the
    /// requested folder must win, otherwise every folder-scoped operation
    /// pays a re-SELECT round trip on a connection that was already there.
    #[tokio::test]
    async fn checkout_prefers_the_member_already_selected_on_the_folder() {
        let (primed, _primed_server) = marked("ACL").await;
        let (extra, _extra_server) = marked("BINARY").await;
        let pool = pool_of(primed, 2);
        set_primed_affinity(&pool, folder("INBOX"));
        park(&pool, extra, Some(folder("Archive")));

        let archive = pool.checkout_for_folder(&folder("Archive")).await.unwrap();
        assert!(
            archive
                .connection()
                .capabilities()
                .contains(&Capability::Binary),
            "the member selected on Archive must serve the Archive checkout",
        );

        let inbox = pool.checkout_for_folder(&folder("INBOX")).await.unwrap();
        assert!(
            inbox.connection().capabilities().contains(&Capability::Acl),
            "the member selected on INBOX must serve the INBOX checkout",
        );
    }

    /// `checkout_any` is the affinity-free lane: it reuses a parked member
    /// even though that member is selected on some unrelated mailbox.
    /// Dialing instead would burn a connection for a command that does not
    /// care which mailbox is selected.
    #[tokio::test]
    async fn checkout_any_reuses_a_member_selected_elsewhere() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 2);
        set_primed_affinity(&pool, folder("Archive"));

        let conn = pool.checkout_any().await.unwrap();
        assert!(conn.connection().capabilities().contains(&Capability::Acl));
        assert_eq!(idle_len(&pool), 0, "the parked member was reused, not left");
    }

    /// Dropping a checkout parks it again WITH its selection, which is what
    /// makes the affinity above reachable on the next checkout.
    #[tokio::test]
    async fn dropping_a_checkout_parks_it_with_its_selection() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        {
            let mut conn = pool.checkout_for_folder(&folder("INBOX")).await.unwrap();
            assert_eq!(idle_len(&pool), 0);
            conn.set_selected(folder("INBOX"));
        }

        assert_eq!(idle_selection(&pool), vec![Some(folder("INBOX"))]);
    }

    /// `discard` is the "this connection is suspect" exit: the member must
    /// not come back to the pool, or the next checkout hands the same
    /// broken session to another caller.
    #[tokio::test]
    async fn a_discarded_checkout_is_not_parked_again() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        {
            let mut conn = pool.checkout_for_folder(&folder("INBOX")).await.unwrap();
            conn.discard();
        }

        assert_eq!(idle_len(&pool), 0, "a discarded member must not be parked");
    }

    /// Dropping a checkout returns its member. A lost member is
    /// invisible until the pool is saturated, and at `pool_cap == 1` it is
    /// a permanent deadlock, so pin it at the smallest cap.
    #[tokio::test]
    async fn a_returned_checkout_is_available_to_the_next_checkout() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        drop(pool.checkout_for_folder(&folder("INBOX")).await.unwrap());

        let second = tokio::time::timeout(
            Duration::from_secs(5),
            pool.checkout_for_folder(&folder("INBOX")),
        )
        .await
        .expect("the returned member must be available");
        assert!(second.is_ok());
    }

    /// session_cap() clamps zero to one physical session, including primed.
    #[tokio::test]
    async fn a_zero_pool_cap_is_clamped_to_one_session() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 0);

        let conn = tokio::time::timeout(
            Duration::from_secs(5),
            pool.checkout_for_folder(&folder("INBOX")),
        )
        .await
        .expect("a zero cap must still allow one session");
        assert!(conn.is_ok());
    }

    /// After `close`, every way into the pool refuses instead of dialing.
    /// A post-close dial would outlive the account and leak a session the
    /// consumer believes is gone.
    #[tokio::test]
    async fn close_gates_every_way_into_the_pool() {
        let (primed, primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);
        // No responder for the LOGOUT close issues: dropping the server end
        // makes it fail fast instead of blocking on a read.
        drop(primed_server);

        tokio::time::timeout(Duration::from_secs(5), pool.close())
            .await
            .expect("close must not block on an unresponsive peer");

        assert!(matches!(
            pool.checkout_for_folder(&folder("INBOX")).await,
            Err(Error::Closed { .. })
        ));
        assert!(matches!(
            pool.checkout_any().await,
            Err(Error::Closed { .. })
        ));
        assert!(matches!(
            pool.acquire_fresh(Vec::new()).await,
            Err(Error::Closed { .. })
        ));
        assert_eq!(idle_len(&pool), 0, "close drains the parked members");
    }

    /// A peer that accepts the LOGOUT and then says nothing must not hold
    /// `close` open. The server ends stay alive and unresponsive here, so
    /// each `logout()` would wait forever on its tagged reply; the drain's
    /// `command_timeout` is the only thing that can end it. Two members pin
    /// the concurrency too: serial logouts would need two timeouts to
    /// finish, and the assertion below allows only one.
    #[tokio::test(start_paused = true)]
    async fn close_does_not_wait_forever_on_a_silent_peer() {
        let (primed, _primed_server) = marked("ACL").await;
        let (extra, _extra_server) = marked("BINARY").await;
        let pool = pool_of(primed, 2);
        park(&pool, extra, None);
        let budget = pool.inner.config.imap.command_timeout;

        let started = tokio::time::Instant::now();
        pool.close().await;

        assert!(
            started.elapsed() <= budget,
            "close drained two silent peers in {:?}, past the {budget:?} budget",
            started.elapsed(),
        );
        assert_eq!(idle_len(&pool), 0, "close drains the parked members");
        assert!(matches!(
            pool.checkout_any().await,
            Err(Error::Closed { .. })
        ));
    }

    /// Close reaches a checkout even while it is outside the parked list.
    #[tokio::test]
    async fn a_checkout_outstanding_at_close_is_not_parked_on_drop() {
        let (primed, mut primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        let conn = pool.checkout_for_folder(&folder("INBOX")).await.unwrap();
        let server = tokio::spawn(async move {
            let logout = read_line(&mut primed_server).await;
            assert!(
                logout.contains("LOGOUT"),
                "close must reach the outstanding checkout: {logout}"
            );
            respond(
                &mut primed_server,
                &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
            )
            .await;
        });
        tokio::time::timeout(Duration::from_secs(5), pool.close())
            .await
            .expect("close must not block");
        server.await.unwrap();
        drop(conn);

        assert_eq!(
            idle_len(&pool),
            0,
            "a closed pool must not accept a returning checkout",
        );
    }

    /// A drop that starts before `close` and finishes after its drain must
    /// not park the member it is returning.
    ///
    /// `close` stores the closed flag and only then drains `idle`, so a
    /// `Drop` that reads the flag BEFORE taking the idle lock can be
    /// preempted in between and push a member the drain has already
    /// LOGOUT'd and terminated - leaving a dead entry parked on a closed
    /// pool and falsifying the `idle_len == 0` postcondition the close
    /// tests assert. The interleaving is forced here rather than raced:
    /// this thread holds the idle lock for the whole of close's
    /// linearization (store the flag, drain the list), so the dropper can
    /// only reach its push afterwards. A drop that re-checks the flag under
    /// that lock parks nothing; one that checked it earlier parks a corpse.
    #[tokio::test]
    async fn a_drop_that_finishes_after_the_close_drain_parks_nothing() {
        let (primed, primed_server) = marked("ACL").await;
        // No responder is needed: this test never issues a command.
        drop(primed_server);
        let pool = pool_of(primed, 1);
        let conn = pool.checkout_any().await.unwrap();

        let mut guard = pool.inner.idle.lock().unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let dropper = std::thread::spawn(move || {
            ready_tx.send(()).expect("the test is still waiting");
            drop(conn);
        });
        ready_rx.recv().expect("the dropper thread started");
        // Let the dropper run far enough to perform the pre-lock `closed`
        // read the racing ordering does, and block on the lock this thread
        // holds. No wall clock is involved.
        for _ in 0..10_000 {
            std::thread::yield_now();
        }

        // Close's linearization, under the lock the drop must respect.
        pool.inner
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        guard.parked.clear();
        drop(guard);

        dropper.join().expect("the dropper thread finished");
        assert_eq!(
            idle_len(&pool),
            0,
            "a member returned after the close drain must not be parked",
        );
    }

    /// A checkout waiting at the cap when `close` runs must refuse rather
    /// than dial. `close` drains `registry` once; a session minted after
    /// that drain is a live connection the consumer has already been told is
    /// gone, which is exactly the guarantee this pool makes. The only
    /// session is held for the whole test, so the waiter can only ever be
    /// released by `close` itself (the closed semaphore, the cleared waiter
    /// queue, or the post-grant re-check).
    #[tokio::test]
    async fn a_checkout_blocked_on_a_permit_at_close_refuses_instead_of_dialing() {
        let (primed, primed_server) = marked("ACL").await;
        let pool = Arc::new(pool_of(primed, 1));
        let held = pool.checkout_any().await.unwrap();
        // No responder for the LOGOUT close issues; dropping the server end
        // makes the drain fail fast instead of spending `command_timeout`.
        drop(primed_server);

        let waiting = tokio::spawn({
            let pool = Arc::clone(&pool);
            async move { pool.checkout_any().await.map(|_| ()) }
        });
        // Let the spawned checkout register as a waiter before closing, so
        // close's release of blocked acquisitions is the thing under test
        // rather than the cheap pre-check.
        tokio::task::yield_now().await;
        tokio::time::timeout(Duration::from_secs(5), pool.close())
            .await
            .expect("close must not block on the blocked waiter");

        let outcome = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("close must release a blocked checkout rather than strand it")
            .unwrap();
        assert!(
            matches!(outcome, Err(Error::Closed { .. })),
            "a checkout released by close must refuse, not mint a session: {outcome:?}",
        );
        drop(held);
    }

    /// The registration side of the same race. `dial` completing after the
    /// drain must be refused rather than filed, because nothing would ever
    /// tear it down: `close` has already run. This is `dial`'s decision
    /// point, exercised directly - a hermetic test cannot make the real
    /// handshake straddle `close`.
    #[tokio::test]
    async fn a_session_finished_after_close_is_refused_registration() {
        let (primed, primed_server) = marked("ACL").await;
        let (late, _late_server) = marked("BINARY").await;
        let pool = pool_of(primed, 1);
        drop(primed_server);

        tokio::time::timeout(Duration::from_secs(5), pool.close())
            .await
            .expect("close must not block");

        let late = Arc::new(late);
        assert!(
            !pool.inner.register(&late),
            "a session that finished after the drain must not be filed as live",
        );
        assert!(
            pool.inner.registry.lock().unwrap().is_empty(),
            "close leaves the registry drained",
        );
    }

    /// Completed detached drivers are pruned at the next registration, so a
    /// reconnecting account does not retain one join handle per old session.
    #[tokio::test]
    async fn registration_prunes_completed_detached_drivers() {
        let (primed, primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        for _ in 0..32 {
            let (transient, mut peer) = marked("BINARY").await;
            let transient = Arc::new(transient);
            assert!(pool.inner.register(&transient));
            let driver = transient.driver_task();
            drop(transient);
            let logout = read_line(&mut peer).await;
            respond(
                &mut peer,
                &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
            )
            .await;
            driver
                .finish_or_abort(crate::connection::LOGOUT_DRAIN_TIMEOUT)
                .await;
        }

        let registered = pool.inner.registry.lock().unwrap().len();
        assert!(
            registered <= 2,
            "completed detached sessions left {registered} registry entries",
        );
        drop(primed_server);
        pool.close().await;
        assert!(pool.inner.registry.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_checkout_dials_through_the_injected_connector() {
        use super::super::test_support::{scripted_account_parts_with, scripted_connector};
        let (primed, _peer) = marked("ACL").await;
        let (connector, mut dials) = scripted_connector(vec![
            b"* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN SASL-IR] ready\r\n".to_vec(),
        ]);
        let parts = scripted_account_parts_with(primed, 2, connector);
        let _held = parts.pool.checkout_any().await.unwrap();
        let script = async {
            let mut peer = dials.servers.recv().await.unwrap();
            let auth = read_line(&mut peer).await;
            assert!(auth.contains("AUTHENTICATE PLAIN"));
            respond(
                &mut peer,
                &format!(
                    "{} OK [CAPABILITY IMAP4rev1 IDLE UNSELECT BINARY] done\r\n",
                    tag_of(&auth)
                ),
            )
            .await;
            peer
        };
        let (checkout, _peer) = tokio::join!(parts.pool.checkout_any(), script);
        let checkout = checkout.unwrap();
        assert!(
            checkout
                .connection()
                .server_profile()
                .supports(Capability::Binary)
        );
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    const DIAL_GREETING: &[u8] = b"* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN SASL-IR] ready\r\n";

    async fn budget_pool(
        cap: usize,
        count: usize,
    ) -> (
        Arc<Pool>,
        tokio::io::DuplexStream,
        super::super::test_support::ScriptedDials,
    ) {
        let (primed, peer) = marked("ACL").await;
        let (connector, dials) =
            super::super::test_support::scripted_connector(vec![DIAL_GREETING.to_vec(); count]);
        let parts = super::super::test_support::scripted_account_parts_with(primed, cap, connector);
        (parts.pool, peer, dials)
    }

    async fn answer_dial(
        dials: &mut super::super::test_support::ScriptedDials,
    ) -> tokio::io::DuplexStream {
        let mut peer = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut peer).await;
        assert!(auth.contains("AUTHENTICATE PLAIN"), "{auth}");
        respond(
            &mut peer,
            &format!(
                "{} OK [CAPABILITY IMAP4rev1 IDLE UNSELECT BINARY] done\r\n",
                tag_of(&auth)
            ),
        )
        .await;
        peer
    }

    async fn answer_logout(peer: &mut tokio::io::DuplexStream) {
        let logout = read_line(peer).await;
        assert!(logout.contains("LOGOUT"), "{logout}");
        respond(
            peer,
            &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
        )
        .await;
    }

    #[tokio::test]
    async fn the_primed_session_owns_a_permit() {
        let (pool, _peer, _) = budget_pool(1, 0).await;
        assert_eq!(pool.available_sessions(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_discarded_session_keeps_its_permit_until_its_driver_exits() {
        let (pool, _peer, mut dials) = budget_pool(2, 1).await;
        let _held = pool.checkout_any().await.unwrap();
        let (second, mut peer) = tokio::join!(pool.checkout_any(), answer_dial(&mut dials));
        let mut second = second.unwrap();
        let driver = second.connection().driver_task();
        assert_eq!(pool.available_sessions(), 0);
        second.discard();
        assert_eq!(pool.available_sessions(), 0);
        answer_logout(&mut peer).await;
        driver
            .finish_or_abort(crate::connection::LOGOUT_DRAIN_TIMEOUT)
            .await;
        assert_eq!(pool.available_sessions(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_checkout_at_the_cap_waits_for_a_member_parked_later() {
        let (pool, _peer, dials) = budget_pool(1, 0).await;
        let held = pool.checkout_any().await.unwrap();
        let mut second = Box::pin(pool.checkout_any());
        assert!(futures::poll!(&mut second).is_pending());
        drop(held);
        let second = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap();
        assert!(
            second
                .connection()
                .server_profile()
                .supports(Capability::Acl)
        );
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_returned_member_goes_to_the_oldest_waiter_not_a_barging_checkout() {
        let (pool, _peer, _) = budget_pool(1, 0).await;
        let held = pool.checkout_any().await.unwrap();
        let mut waiter = Box::pin(pool.checkout_any());
        assert!(futures::poll!(&mut waiter).is_pending());
        drop(held);
        let mut barger = Box::pin(pool.checkout_any());
        assert!(futures::poll!(&mut barger).is_pending());
        let std::task::Poll::Ready(Ok(held)) = futures::poll!(&mut waiter) else {
            panic!("oldest waiter was not handed the member")
        };
        assert!(held.connection().server_profile().supports(Capability::Acl));
    }

    #[tokio::test]
    async fn a_checkout_at_the_cap_dials_once_a_session_exits() {
        let (pool, peer, mut dials) = budget_pool(1, 1).await;
        let mut held = pool.checkout_any().await.unwrap();
        let mut second = Box::pin(pool.checkout_any());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        held.discard();
        drop(peer);
        let (second, _peer) = tokio::join!(second, answer_dial(&mut dials));
        assert!(
            second
                .unwrap()
                .connection()
                .server_profile()
                .supports(Capability::Binary)
        );
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_acquisition_retires_a_parked_member_to_make_room() {
        let (pool, mut peer, mut dials) = budget_pool(1, 1).await;
        let old = Arc::clone(&pool.inner.idle.lock().unwrap().parked[0].conn);
        let script = async {
            answer_logout(&mut peer).await;
            answer_dial(&mut dials).await
        };
        let (fresh, _peer) = tokio::join!(pool.acquire_fresh(Vec::new()), script);
        assert!(fresh.unwrap().server_profile().supports(Capability::Binary));
        assert!(!old.is_alive());
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_acquisition_takes_a_member_returned_while_it_waits() {
        let (pool, mut peer, mut dials) = budget_pool(1, 1).await;
        let held = pool.checkout_any().await.unwrap();
        let mut fresh = Box::pin(pool.acquire_fresh(Vec::new()));
        assert!(futures::poll!(&mut fresh).is_pending());
        drop(held);
        let mut data = Box::pin(pool.checkout_any());
        assert!(futures::poll!(&mut data).is_pending());
        let script = async {
            answer_logout(&mut peer).await;
            answer_dial(&mut dials).await
        };
        let (fresh, _peer) = tokio::join!(fresh, script);
        let fresh = fresh.unwrap();
        assert!(fresh.server_profile().supports(Capability::Binary));
        assert!(futures::poll!(&mut data).is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn the_room_a_retire_makes_returns_to_the_retiring_acquisition() {
        let (pool, mut peer, mut dials) = budget_pool(1, 1).await;
        let mut fresh = Box::pin(pool.acquire_fresh(Vec::new()));
        assert!(futures::poll!(&mut fresh).is_pending());
        let logout = read_line(&mut peer).await;
        assert!(logout.contains("LOGOUT"));
        let mut data = Box::pin(pool.checkout_any());
        assert!(futures::poll!(&mut data).is_pending());
        respond(
            &mut peer,
            &format!("* BYE closing\r\n{} OK done\r\n", tag_of(&logout)),
        )
        .await;
        let (fresh, _peer) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(5), fresh),
            answer_dial(&mut dials)
        );
        let fresh = fresh.unwrap().unwrap();
        assert!(fresh.server_profile().supports(Capability::Binary));
        assert!(futures::poll!(&mut data).is_pending());
    }

    #[tokio::test]
    async fn fresh_acquisition_never_adopts_a_parked_member() {
        let (pool, _peer, mut dials) = budget_pool(2, 1).await;
        let (fresh, _peer) = tokio::join!(pool.acquire_fresh(Vec::new()), answer_dial(&mut dials));
        assert!(fresh.unwrap().server_profile().supports(Capability::Binary));
        assert_eq!(idle_len(&pool), 1);
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    struct Flag(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Flag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn extra_values_are_released_only_at_the_fresh_session_driver_exit() {
        let (pool, _peer, mut dials) = budget_pool(2, 1).await;
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (fresh, peer) = tokio::join!(
            pool.acquire_fresh(vec![Box::new(Flag(Arc::clone(&flag)))]),
            answer_dial(&mut dials)
        );
        let fresh = fresh.unwrap();
        let driver = fresh.driver_task();
        drop(fresh);
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(pool.available_sessions(), 0);
        drop(peer);
        driver
            .finish_or_abort(crate::connection::LOGOUT_DRAIN_TIMEOUT)
            .await;
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(pool.available_sessions(), 1);
    }

    #[tokio::test]
    async fn a_failed_fresh_dial_returns_its_permit_and_extras_at_once() {
        let (pool, _peer, _) = budget_pool(2, 0).await;
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        assert!(
            pool.acquire_fresh(vec![Box::new(Flag(Arc::clone(&flag)))])
                .await
                .is_err()
        );
        assert_eq!(pool.available_sessions(), 1);
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    struct Order(Arc<std::sync::Mutex<Vec<&'static str>>>, &'static str);
    impl Drop for Order {
        fn drop(&mut self) {
            self.0.lock().unwrap().push(self.1);
        }
    }

    struct PendingConnector {
        _handoff: crate::connection::LeaseHandoff,
        _transport: Order,
        entered: Option<tokio::sync::oneshot::Sender<()>>,
    }
    impl std::future::Future for PendingConnector {
        type Output = Result<crate::ImapConnection, Error>;
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if let Some(tx) = self.entered.take() {
                tx.send(()).unwrap();
            }
            std::task::Poll::Pending
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_connector_is_destroyed_before_its_leases() {
        let (pool, peer, _) = budget_pool(2, 0).await;
        drop(peer);
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));
        let transport = Order(Arc::clone(&order), "transport");
        let lease = Order(Arc::clone(&order), "lease");
        let (tx, rx) = tokio::sync::oneshot::channel();
        let permit = Arc::clone(&pool.inner.sessions)
            .try_acquire_owned()
            .unwrap();
        let mut dial = Box::pin(pool.inner.dial_with(
            permit,
            vec![Box::new(lease)],
            move |handoff| PendingConnector {
                _handoff: handoff,
                _transport: transport,
                entered: Some(tx),
            },
        ));
        assert!(futures::poll!(&mut dial).is_pending());
        rx.await.unwrap();
        pool.close().await;
        assert!(matches!(dial.await, Err(Error::Closed { .. })));
        assert_eq!(*order.lock().unwrap(), ["transport", "lease"]);
    }

    #[tokio::test(start_paused = true)]
    async fn replacement_dials_fresh_instead_of_adopting_a_parked_member() {
        let (pool, mut peer, mut dials) = budget_pool(2, 1).await;
        let mut held = pool.checkout_any().await.unwrap();
        let target = folder("Archive");
        let select_script = async {
            let select = read_line(&mut peer).await;
            assert!(select.contains("SELECT"));
            respond(&mut peer, &format!("* FLAGS (\\Seen)\r\n* 1 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 1] valid\r\n{} OK [READ-WRITE] selected\r\n", tag_of(&select))).await;
        };
        let (result, ()) = tokio::join!(
            held.connection().select(&target, Duration::from_secs(5)),
            select_script
        );
        result.unwrap();
        held.set_selected(folder("Archive"));
        let (other, _other_peer) = marked("UNSELECT").await;
        park(&pool, other, Some(folder("Archive")));
        let script = async {
            answer_logout(&mut peer).await;
            answer_dial(&mut dials).await
        };
        let target = folder("Archive");
        let (result, _peer) = tokio::join!(
            held.deselect_target(&target, Duration::from_secs(5)),
            script
        );
        result.unwrap();
        assert!(
            held.connection()
                .server_profile()
                .supports(Capability::Binary)
        );
        assert_eq!(held.selected_affinity(), None);
        assert_eq!(idle_selection(&pool), vec![Some(folder("Archive"))]);
    }

    #[tokio::test(start_paused = true)]
    async fn close_releases_waiters_with_closed() {
        let (pool, peer, dials) = budget_pool(1, 0).await;
        let _held = pool.checkout_any().await.unwrap();
        let mut data = Box::pin(pool.checkout_any());
        let mut fresh = Box::pin(pool.acquire_fresh(Vec::new()));
        assert!(futures::poll!(&mut data).is_pending());
        assert!(futures::poll!(&mut fresh).is_pending());
        drop(peer);
        pool.close().await;
        assert!(matches!(data.await, Err(Error::Closed { .. })));
        assert!(matches!(fresh.await, Err(Error::Closed { .. })));
        assert_eq!(dials.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    /// A waiter cancelled after a member was already handed to it must pass
    /// that member on, or the session is stranded at the cap forever.
    #[tokio::test]
    async fn a_cancelled_waiter_returns_its_handoff_to_the_next_waiter() {
        let (pool, _peer, _) = budget_pool(1, 0).await;
        let held = pool.checkout_any().await.unwrap();
        let mut first = Box::pin(pool.checkout_any());
        let mut next = Box::pin(pool.checkout_any());
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut next).is_pending());
        drop(held);
        drop(first);
        let std::task::Poll::Ready(Ok(held)) = futures::poll!(&mut next) else {
            panic!("a cancelled receiver stranded its handed member");
        };
        assert!(held.connection().server_profile().supports(Capability::Acl));
    }

    /// A cancelled waiter leaves the queue synchronously in its drop. A
    /// stale entry would stop every later checkout from adopting a parked
    /// member (nobody adopts while someone waits).
    #[tokio::test]
    async fn a_cancelled_waiter_is_removed_before_any_member_returns() {
        let (pool, _peer, _) = budget_pool(1, 0).await;
        let held = pool.checkout_any().await.unwrap();
        let mut first = Box::pin(pool.checkout_any());
        assert!(futures::poll!(&mut first).is_pending());
        drop(first);
        assert!(pool.inner.idle.lock().unwrap().waiters.is_empty());
        drop(held);
        assert_eq!(idle_len(&pool), 1);
    }
}
