use std::sync::{Arc, Mutex};

use futures::FutureExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::connection::{DriverTask, ImapConnection};
use crate::error::Error;
use crate::types::MailboxName;

use super::factory::ImapAccountConfig;

struct PoolMember {
    conn: Arc<ImapConnection>,
    selected: Option<MailboxName>,
}

struct SessionRegistration {
    connection: std::sync::Weak<ImapConnection>,
    driver: Arc<DriverTask>,
}

pub(crate) struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<PoolMember>>,
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
    sessions: Mutex<Vec<SessionRegistration>>,
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
        data_cap: usize,
        meter: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_budget: bifrost_net::BandwidthBudget,
    ) -> Self {
        let primed = Arc::new(primed);
        let primed_weak = Arc::downgrade(&primed);
        let primed_driver = primed.driver_task();
        Self {
            inner: Arc::new(PoolInner {
                permits: Arc::new(Semaphore::new(data_cap.max(1))),
                idle: Mutex::new(vec![PoolMember {
                    conn: primed,
                    selected: None,
                }]),
                config,
                meter,
                bandwidth_budget,
                closed: std::sync::atomic::AtomicBool::new(false),
                qresync_policy: std::sync::atomic::AtomicBool::new(false),
                sessions: Mutex::new(vec![SessionRegistration {
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
        let permit = self.inner.permit().await?;
        let member = {
            let mut idle = self.inner.idle.lock().expect("pool lock poisoned");
            idle.retain(|member| member.conn.is_alive());
            let idx = idle
                .iter()
                .position(|member| member.selected.as_ref() == Some(folder));
            idx.map(|idx| idle.swap_remove(idx)).or_else(|| idle.pop())
        };
        let member = match member {
            Some(member) => member,
            None => PoolMember {
                conn: self.inner.dial(Some(Arc::clone(&permit))).await?,
                selected: None,
            },
        };
        Ok(PooledConn {
            member: Some(member),
            pool: Arc::clone(&self.inner),
            _permit: permit,
        })
    }

    /// Check out a pooled connection without imposing a selected-mailbox
    /// affinity. Callers that do not issue mailbox-relative commands can
    /// safely reuse any parked connection, including one left selected by a
    /// folder-scoped operation.
    pub(crate) async fn checkout_any(&self) -> Result<PooledConn, Error> {
        let permit = self.inner.permit().await?;
        let member = {
            let mut idle = self.inner.idle.lock().expect("pool lock poisoned");
            idle.retain(|member| member.conn.is_alive());
            idle.pop()
        };
        let member = match member {
            Some(member) => member,
            None => PoolMember {
                conn: self.inner.dial(Some(Arc::clone(&permit))).await?,
                selected: None,
            },
        };
        Ok(PooledConn {
            member: Some(member),
            pool: Arc::clone(&self.inner),
            _permit: permit,
        })
    }

    /// Dial the dedicated connection used exclusively by the long-lived IDLE
    /// loop. Ordinary account operations must use a checkout method instead.
    /// This does not acquire a data permit: push admission bounds its own
    /// sessions separately. Holding data permits for permanent IDLE workers
    /// would starve checkouts; `pool_cap` is not a shared physical-session cap.
    pub(crate) async fn dial_idle(&self) -> Result<Arc<ImapConnection>, Error> {
        self.inner.dial(None).await
    }

    pub(crate) async fn close(&self) {
        let completion = self
            .inner
            .close_completion
            .get_or_init(|| {
                self.inner
                    .closed
                    .store(true, std::sync::atomic::Ordering::Release);
                self.inner.permits.close();
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
            std::mem::take(&mut *idle)
        };
        // Drain under the same mutex registration re-checks `closed` under.
        // Exclusive dial admission means every admitted connect/authenticate
        // task has finished or terminated its unclaimed session before here.
        let sessions: Vec<_> = self
            .sessions
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

    /// Acquire one data-lane permit, refusing a closed pool on both sides of
    /// the await. The pre-check is the cheap path; the post-check is the
    /// load-bearing one, because `close` can land while this task is parked
    /// on the semaphore and the permit it then receives must not become a
    /// live session after the drain has already run.
    async fn permit(&self) -> Result<Arc<OwnedSemaphorePermit>, Error> {
        if self.is_closed() {
            return Err(Error::closed());
        }
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| Error::closed())?;
        if self.is_closed() {
            return Err(Error::closed());
        }
        Ok(Arc::new(permit))
    }

    /// Dial and authenticate one fresh connection under the pool's meter and
    /// bandwidth cap, then bring it to the account's QRESYNC state. The
    /// single place a pool connection is minted, so a future change to how
    /// connections are metered, capped or initialized lands once.
    ///
    /// A dial that started before `close` and completed after it is torn
    /// down here rather than handed out: `Account::close()` promises no
    /// session of this account is still connected when it returns. The
    /// connection is registered BEFORE authentication and initialization.
    /// An owned dial task retains admission until it hands out the session
    /// or terminates it when its consumer has gone away.
    async fn dial(
        self: &Arc<Self>,
        permit: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<Arc<ImapConnection>, Error> {
        let inner = Arc::clone(self);
        self.dial_with_permit(
            async move {
                // Keep the post-spawn keepalive await inside dial_admitted, where
                // cancellation retains a handle it can terminate and join.
                let mut config = inner.config.imap.clone();
                config.keepalive = None;
                ImapConnection::connect_config_metered(
                    &config,
                    inner.meter.clone(),
                    Some(inner.bandwidth_budget.clone()),
                )
                .await
            },
            permit,
        )
        .await
    }

    #[cfg(test)]
    async fn dial_with<F>(self: &Arc<Self>, connect: F) -> Result<Arc<ImapConnection>, Error>
    where
        F: std::future::Future<Output = Result<ImapConnection, Error>> + Send + 'static,
    {
        self.dial_with_permit(connect, None).await
    }

    async fn dial_with_permit<F>(
        self: &Arc<Self>,
        connect: F,
        permit: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<Arc<ImapConnection>, Error>
    where
        F: std::future::Future<Output = Result<ImapConnection, Error>> + Send + 'static,
    {
        let (mut tx, rx) = tokio::sync::oneshot::channel();
        let inner = Arc::clone(self);
        tokio::spawn(async move {
            // Keep capacity charged until cancellation has terminated the driver.
            let _permit = permit;
            let _dial = inner.dials.read().await;
            let mut connected = None;
            let result = tokio::select! {
                biased;
                _ = inner.cancel_dials.cancelled() => Err(Error::closed()),
                _ = tx.closed() => Err(Error::closed()),
                result = inner.dial_admitted(connect, &mut connected) => result,
            };
            // This handle lives outside the cancellable future: dropping an
            // authentication future alone does not join its driver task.
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
        let mut sessions = self.sessions.lock().expect("pool sessions lock poisoned");
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
    _permit: Arc<OwnedSemaphorePermit>,
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
    /// The old member is logged out and released BEFORE the replacement is
    /// dialed. Dialing first would hold cap+1 physical connections for the
    /// duration of the handshake, and a server that enforces its per-user
    /// connection limit rejects exactly the fallback that a pre-UNSELECT
    /// server needs. It also keeps the old session SELECTed on the target
    /// while the DELETE that follows is issued, which RFC 2683 2.2.2 warns
    /// against. Ordering the teardown first costs one round trip and makes
    /// the fallback work on the servers that require it.
    async fn replace_selected_member(&mut self) -> Result<(), Error> {
        if let Some(old) = self.member.take() {
            let _ = old.conn.logout().await;
        }
        self.member = Some(PoolMember {
            conn: self.pool.dial(Some(Arc::clone(&self._permit))).await?,
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
        // The `closed` re-check happens UNDER the idle lock, together with
        // the push. Reading the flag before taking the lock is a race:
        // `close` stores the flag and only then drains `idle`, so a drop
        // that read `closed == false` and was preempted before its push
        // could park a member on the far side of a completed close - and
        // that member has already been LOGOUT'd and terminated by the
        // drain. Holding the lock across both makes the two orders the only
        // possible ones: this push lands before the drain and is drained,
        // or it observes the flag the drain's own lock acquisition
        // published and parks nothing.
        let mut idle = self.pool.idle.lock().expect("pool lock poisoned");
        if self.pool.is_closed() {
            return;
        }
        idle.push(member);
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

    /// A config whose dial target is unroutable on purpose: every test
    /// below must be served from parked members, so any code path that
    /// decides to dial shows up as a failure instead of silently passing.
    fn config() -> Arc<ImapAccountConfig> {
        Arc::new(ImapAccountConfig::new(
            crate::ImapConfig::plaintext("test.invalid"),
            Credentials::password("user", "pass"),
            AuthPolicy::default(),
        ))
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
        let pool = Pool::new(config(), a, 2, None, budget.clone());
        pool.inner.idle.lock().unwrap().push(PoolMember {
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
        assert!(pool.inner.sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn close_joins_a_detached_healthy_drivers_graceful_logout() {
        let (primed, mut peer) = marked("ACL").await;
        let pool = pool_of(primed, 1);
        let member = pool.inner.idle.lock().unwrap().pop().unwrap();
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
        assert!(pool.inner.sessions.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn close_cancels_a_dial_before_registration() {
        let (primed, primed_peer) = marked("ACL").await;
        let pool = pool_of(primed, 1);
        drop(primed_peer);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut dial = Box::pin(pool.inner.dial_with(async move {
            started_tx.send(()).unwrap();
            std::future::pending().await
        }));
        assert!(futures::poll!(dial.as_mut()).is_pending());
        started_rx.await.unwrap();
        pool.close().await;
        assert!(matches!(dial.await, Err(Error::Closed { .. })));
        assert!(pool.inner.sessions.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn close_cancels_authentication_already_on_the_wire() {
        let (primed, primed_peer) = marked("ACL").await;
        drop(primed_peer);
        let (late, mut peer) =
            driver_pair(b"* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN SASL-IR] ready\r\n").await;
        let mut cfg = (*config()).clone();
        cfg.imap.keepalive = None;
        cfg.auth_policy = AuthPolicy::default().allow_cleartext_without_tls();
        let pool = Pool::new(
            Arc::new(cfg),
            primed,
            1,
            None,
            bifrost_net::BandwidthBudget::default(),
        );
        let mut dial = Box::pin(pool.inner.dial_with(async move { Ok(late) }));
        assert!(futures::poll!(dial.as_mut()).is_pending());
        assert!(read_line(&mut peer).await.contains("AUTHENTICATE PLAIN"));
        let conn = pool
            .inner
            .sessions
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
        let (late, _peer) =
            driver_pair(b"* OK [CAPABILITY IMAP4rev1 AUTH=XOAUTH2 SASL-IR] ready\r\n").await;
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let mut cfg = (*config()).clone();
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
            1,
            None,
            bifrost_net::BandwidthBudget::default(),
        );
        let mut dial = Box::pin(pool.inner.dial_with(async move { Ok(late) }));
        assert!(futures::poll!(dial.as_mut()).is_pending());
        entered.acquire().await.unwrap().forget();
        let conn = pool
            .inner
            .sessions
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
        let pool = pool_of(primed, 1);
        let active = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        for _ in 0..16 {
            let inner = Arc::clone(&pool.inner);
            let dial_active = Arc::clone(&active);
            let dial_entered = Arc::clone(&entered);
            let mut checkout = Box::pin(async move {
                let permit = inner.permit().await?;
                inner
                    .dial_with_permit(
                        async move {
                            assert_eq!(dial_active.fetch_add(1, Ordering::SeqCst), 0);
                            let _active = Active(dial_active);
                            dial_entered.add_permits(1);
                            std::future::pending().await
                        },
                        Some(permit),
                    )
                    .await
            });
            assert!(futures::poll!(checkout.as_mut()).is_pending());
            entered.acquire().await.unwrap().forget();
            drop(checkout);
            // No executor yield: cancellation has not reached the detached task.
            assert_eq!(pool.inner.permits.available_permits(), 0);
            let permit = pool.inner.permit().await.unwrap();
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

    fn pool_of(primed: crate::ImapConnection, data_cap: usize) -> Pool {
        Pool::new(
            config(),
            primed,
            data_cap,
            None,
            bifrost_net::BandwidthBudget::default(),
        )
    }

    /// Park an extra member directly. `Pool::new` accepts only one primed
    /// connection and the only other way in is a real dial, which no
    /// hermetic test may perform.
    fn park(pool: &Pool, conn: crate::ImapConnection, selected: Option<MailboxName>) {
        let conn = Arc::new(conn);
        pool.inner
            .sessions
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
            .push(PoolMember { conn, selected });
    }

    fn set_primed_affinity(pool: &Pool, selected: MailboxName) {
        pool.inner.idle.lock().unwrap()[0].selected = Some(selected);
    }

    fn idle_len(pool: &Pool) -> usize {
        pool.inner.idle.lock().unwrap().len()
    }

    fn idle_selection(pool: &Pool) -> Vec<Option<MailboxName>> {
        pool.inner
            .idle
            .lock()
            .unwrap()
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

    /// Dropping a checkout releases its permit. A leaked permit is
    /// invisible until the pool is saturated, and at `data_cap == 1` it is
    /// a permanent deadlock, so pin it at the smallest cap.
    #[tokio::test]
    async fn dropping_a_checkout_releases_its_permit() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 1);

        drop(pool.checkout_for_folder(&folder("INBOX")).await.unwrap());

        let second = tokio::time::timeout(
            Duration::from_secs(5),
            pool.checkout_for_folder(&folder("INBOX")),
        )
        .await
        .expect("the released permit must be reacquirable");
        assert!(second.is_ok());
    }

    /// `Semaphore::new(0)` never hands out a permit, so a misconfigured
    /// `pool_cap` of zero would hang the first checkout forever rather than
    /// erroring. `Pool::new` clamps to one; pin the clamp.
    #[tokio::test]
    async fn a_zero_data_cap_is_clamped_to_one_permit() {
        let (primed, _primed_server) = marked("ACL").await;
        let pool = pool_of(primed, 0);

        let conn = tokio::time::timeout(
            Duration::from_secs(5),
            pool.checkout_for_folder(&folder("INBOX")),
        )
        .await
        .expect("a zero cap must still yield one permit");
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
        assert!(matches!(pool.dial_idle().await, Err(Error::Closed { .. })));
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

        let idle = pool.inner.idle.lock().unwrap();
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
        let mut idle = idle;
        pool.inner
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        idle.clear();
        drop(idle);

        dropper.join().expect("the dropper thread finished");
        assert_eq!(
            idle_len(&pool),
            0,
            "a member returned after the close drain must not be parked",
        );
    }

    /// A checkout parked on the semaphore when `close` runs must not be
    /// handed the permit and go on to dial. `close` drains `sessions` once;
    /// a session minted after that drain is a live connection the consumer
    /// has already been told is gone, which is exactly the guarantee this
    /// pool now makes. The single permit is held for the whole test, so the
    /// waiter can only ever be released by `close` itself.
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
        // Let the spawned checkout reach the semaphore before closing, so
        // the post-acquire re-check is the thing under test rather than the
        // cheap pre-check.
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
            pool.inner.sessions.lock().unwrap().is_empty(),
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

        let registered = pool.inner.sessions.lock().unwrap().len();
        assert!(
            registered <= 2,
            "completed detached sessions left {registered} registry entries",
        );
        drop(primed_server);
        pool.close().await;
        assert!(pool.inner.sessions.lock().unwrap().is_empty());
    }
}
