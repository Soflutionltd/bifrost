use std::{
    fmt::{self, Debug},
    ops::{Deref, DerefMut},
    sync::{
        Arc, Condvar, Mutex, TryLockError,
        atomic::{AtomicU32, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use super::{
    super::{Error, SmtpConnection},
    PoolConfig, idle_connection_expired, pool_worker_interval,
};
use crate::transport::smtp::{error, transport::SmtpClient};

pub(crate) struct Pool {
    config: PoolConfig,
    connections: Mutex<Option<Vec<ParkedConnection>>>,
    available: Condvar,
    live: AtomicU32,
    thread_terminator: Option<mpsc::SyncSender<()>>,
    client: SmtpClient,
}

struct ParkedConnection {
    conn: SmtpConnection,
    since: Instant,
}

pub(crate) struct PooledConnection {
    conn: Option<SmtpConnection>,
    pool: Arc<Pool>,
}

impl Pool {
    pub(crate) fn new(config: PoolConfig, client: SmtpClient) -> Arc<Self> {
        let (thread_terminator, thread_rx) = if config.min_idle > 0 {
            let (thread_tx, thread_rx) = mpsc::sync_channel(1);
            (Some(thread_tx), Some(thread_rx))
        } else {
            (None, None)
        };

        let pool = Arc::new(Self {
            config,
            connections: Mutex::new(Some(Vec::new())),
            available: Condvar::new(),
            live: AtomicU32::new(0),
            thread_terminator,
            client,
        });

        if let Some(thread_rx) = thread_rx {
            let pool_ = Arc::clone(&pool);

            let min_idle = pool_.config.min_idle;
            let idle_timeout = pool_.config.idle_timeout;
            let pool = Arc::downgrade(&pool_);

            thread::Builder::new()
                .name("bifrost-smtp-connection-pool".into())
                .spawn(move || {
                    while let Some(pool) = pool.upgrade() {
                        #[cfg(feature = "tracing")]
                        tracing::trace!("running cleanup tasks");

                        #[allow(clippy::needless_collect)]
                        let (count, dropped) = {
                            let mut connections = pool.connections.lock().unwrap();
                            let Some(connections) = connections.as_mut() else {
                                // The transport was shut down
                                return;
                            };

                            let to_drop = connections
                                .iter()
                                .enumerate()
                                .rev()
                                .filter(|(_, conn)| {
                                    idle_connection_expired(conn.idle_duration(), idle_timeout)
                                })
                                .map(|(i, _)| i)
                                .collect::<Vec<_>>();
                            let dropped = to_drop
                                .into_iter()
                                .map(|i| connections.remove(i))
                                .collect::<Vec<_>>();

                            (connections.len(), dropped)
                        };

                        #[cfg(feature = "tracing")]
                        let mut created = 0;
                        for _ in count..(min_idle as usize) {
                            if !pool.try_reserve() {
                                break;
                            }
                            let conn = match pool.client.connection() {
                                Ok(conn) => conn,
                                Err(err) => {
                                    pool.release_live();
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!("couldn't create idle connection {}", err);
                                    #[cfg(not(feature = "tracing"))]
                                    let _ = err;

                                    break;
                                }
                            };

                            let mut connections = pool.connections.lock().unwrap();
                            let Some(connections) = connections.as_mut() else {
                                // The transport was shut down
                                return;
                            };

                            connections.push(ParkedConnection::park(conn));
                            // A replenished connection is a newly available
                            // one, exactly like a recycled one.
                            pool.available.notify_one();

                            #[cfg(feature = "tracing")]
                            {
                                created += 1;
                            }
                        }

                        #[cfg(feature = "tracing")]
                        if created > 0 {
                            tracing::debug!("created {} idle connections", created);
                        }

                        if !dropped.is_empty() {
                            #[cfg(feature = "tracing")]
                            tracing::debug!("dropped {} idle connections", dropped.len());

                            for conn in dropped {
                                let mut conn = conn.unpark();
                                conn.abort();
                                pool.release_live();
                            }
                        }

                        drop(pool);

                        match thread_rx.recv_timeout(pool_worker_interval(idle_timeout)) {
                            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                                // The transport was shut down
                                return;
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => {}
                        }
                    }
                })
                .expect("couldn't spawn the Pool thread");
        }

        pool
    }

    /// Park an already-established connection so a test can exercise checkout
    /// and recycle without dialing anything.
    #[cfg(test)]
    pub(crate) fn park_for_test(&self, conn: SmtpConnection) {
        assert!(self.try_reserve(), "test connection fits pool bound");
        self.connections
            .lock()
            .expect("connection pool lock")
            .as_mut()
            .expect("pool is not shut down")
            .push(ParkedConnection::park(conn));
    }

    /// Number of connections currently parked as idle.
    #[cfg(test)]
    pub(crate) fn idle_count_for_test(&self) -> usize {
        self.connections
            .lock()
            .expect("connection pool lock")
            .as_ref()
            .map_or(0, Vec::len)
    }

    pub(crate) fn shutdown(&self) {
        let connections = { self.connections.lock().unwrap().take() };
        if let Some(connections) = connections {
            for conn in connections {
                conn.unpark().abort();
                self.release_live();
            }
        }
        // A waiter parked on `available` is only reachable through this
        // notify. `recycle` also notifies, so a checked-out connection that is
        // eventually returned would wake one - but a connection that is NEVER
        // returned would leave the waiter parked forever without this.
        self.available.notify_all();

        if let Some(thread_terminator) = &self.thread_terminator {
            _ = thread_terminator.try_send(());
        }
    }

    pub(crate) fn connection(self: &Arc<Self>) -> Result<PooledConnection, Error> {
        if self.config.max_size == 0 {
            return Err(error::invalid_input(
                "pool max_size must be greater than zero",
            ));
        }
        loop {
            let (conn, reserved) = {
                let mut connections = self
                    .connections
                    .lock()
                    .map_err(|_| error::internal("connection pool lock poisoned"))?;
                if connections.is_none() {
                    // The transport was shut down
                    return Err(error::transport_shutdown());
                }
                if let Some(conn) = connections.as_mut().expect("pool is open").pop() {
                    (Some(conn), false)
                } else if self.try_reserve() {
                    (None, true)
                } else {
                    drop(
                        self.available
                            .wait(connections)
                            .map_err(|_| error::internal("connection pool lock poisoned"))?,
                    );
                    continue;
                }
            };

            match conn {
                Some(conn) => {
                    if idle_connection_expired(conn.idle_duration(), self.config.idle_timeout) {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("dropping an expired connection");

                        conn.unpark().abort();
                        self.release_live();
                        continue;
                    }
                    let mut conn = conn.unpark();

                    if conn.has_broken() {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("dropping a broken connection");

                        conn.abort();
                        self.release_live();
                        continue;
                    }

                    if self.config.test_on_checkout && !conn.test_connected() {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("dropping a broken connection");

                        conn.abort();
                        self.release_live();
                        continue;
                    }

                    #[cfg(feature = "tracing")]
                    tracing::debug!("reusing a pooled connection");

                    return Ok(PooledConnection::wrap(conn, Arc::clone(self)));
                }
                None => {
                    debug_assert!(reserved);
                    #[cfg(feature = "tracing")]
                    tracing::debug!("creating a new connection");

                    let conn = match self.client.connection() {
                        Ok(conn) => conn,
                        Err(error) => {
                            self.release_live();
                            return Err(error);
                        }
                    };
                    return Ok(PooledConnection::wrap(conn, Arc::clone(self)));
                }
            }
        }
    }

    fn recycle(&self, mut conn: SmtpConnection) {
        // A connection that has served an LMTP final-status drain is retired
        // rather than recycled: a surplus final status below the read buffer
        // cannot be detected without a read that would block on a well-behaved
        // peer, so reuse could silently consume it as the next reply.
        if conn.has_broken() || conn.should_retire() {
            #[cfg(feature = "tracing")]
            tracing::debug!("dropping a broken or retired connection instead of recycling it");

            conn.abort();
            drop(conn);
            self.release_live();
        } else {
            #[cfg(feature = "tracing")]
            tracing::debug!("recycling connection");

            let mut connections_guard = self.connections.lock().unwrap();

            if let Some(connections) = connections_guard.as_mut() {
                if connections.len() >= self.config.max_size as usize {
                    drop(connections_guard);
                    conn.abort();
                    self.release_live();
                } else {
                    let conn = ParkedConnection::park(conn);
                    connections.push(conn);
                    self.available.notify_one();
                }
            } else {
                // The pool has already been shut down
                drop(connections_guard);
                conn.abort();
                self.release_live();
            }
        }
    }

    fn try_reserve(&self) -> bool {
        self.live
            .try_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                (live < self.config.max_size).then_some(live + 1)
            })
            .is_ok()
    }

    fn release_live(&self) {
        let previous = self.live.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
        // The notify must happen under the connections lock. A checkout's
        // failed `try_reserve` and its `wait` are atomic only against
        // notifiers holding the same mutex: notifying lock-free here could
        // fire in the window between the two, and a Condvar notification is
        // not sticky, so the waiter would park with nobody left to wake it.
        // Every caller has already dropped the lock (recycle and shutdown
        // drop their guards before releasing), so acquiring it here cannot
        // deadlock. A poisoned lock still holds the mutex, which is all the
        // notify ordering needs.
        let _guard = self.connections.lock();
        self.available.notify_one();
    }
}

impl Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("config", &self.config)
            .field(
                "connections",
                &match self.connections.try_lock() {
                    Ok(connections) => {
                        if let Some(connections) = connections.as_ref() {
                            format!("{} connections", connections.len())
                        } else {
                            "SHUT DOWN".to_owned()
                        }
                    }

                    Err(TryLockError::WouldBlock) => "LOCKED".to_owned(),
                    Err(TryLockError::Poisoned(_)) => "POISONED".to_owned(),
                },
            )
            .field("client", &self.client)
            .finish()
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        #[cfg(feature = "tracing")]
        tracing::debug!("dropping Pool");

        if let Some(connections) = self.connections.get_mut().unwrap().take() {
            for conn in connections {
                drop(conn);
            }
        }
    }
}

impl ParkedConnection {
    fn park(conn: SmtpConnection) -> Self {
        Self {
            conn,
            since: Instant::now(),
        }
    }

    fn idle_duration(&self) -> Duration {
        self.since.elapsed()
    }

    fn unpark(self) -> SmtpConnection {
        self.conn
    }
}

impl PooledConnection {
    fn wrap(conn: SmtpConnection, pool: Arc<Pool>) -> Self {
        Self {
            conn: Some(conn),
            pool,
        }
    }
}

impl Deref for PooledConnection {
    type Target = SmtpConnection;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("conn hasn't been dropped yet")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("conn hasn't been dropped yet")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let conn = self
            .conn
            .take()
            .expect("SmtpConnection hasn't been taken yet");
        self.pool.recycle(conn);
    }
}
