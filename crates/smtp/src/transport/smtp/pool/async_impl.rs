use std::{
    fmt::{self, Debug},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use futures::FutureExt;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::{
    super::{AsyncSmtpConnection, Error},
    PoolConfig, idle_connection_expired, pool_worker_interval,
};
use crate::{
    Executor,
    executor::{SmtpExecutor, SpawnHandle},
    transport::smtp::{async_transport::AsyncSmtpClient, error},
};

pub(crate) struct Pool<E: Executor> {
    config: PoolConfig,
    connections: Mutex<Option<Vec<ParkedConnection>>>,
    admission: Arc<Semaphore>,
    available: Notify,
    client: AsyncSmtpClient<E>,
    handle: OnceLock<E::Handle>,
    worker_completion: OnceLock<tokio::sync::watch::Receiver<()>>,
    shutdown_completion: OnceLock<futures::future::Shared<futures::future::BoxFuture<'static, ()>>>,
}

struct ParkedConnection {
    conn: AsyncSmtpConnection,
    since: Instant,
    permit: OwnedSemaphorePermit,
}

pub(crate) struct PooledConnection<E: Executor> {
    conn: Option<AsyncSmtpConnection>,
    pool: Arc<Pool<E>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<E: SmtpExecutor> Pool<E> {
    // Pool creation can dial replacement idle connections, so it needs the
    // private SMTP executor extension. Recycling only spawns cleanup work.
    pub(crate) fn new(config: PoolConfig, client: AsyncSmtpClient<E>) -> Arc<Self> {
        let max_size = config.max_size;
        let pool = Arc::new(Self {
            config,
            connections: Mutex::new(Some(Vec::new())),
            admission: Arc::new(Semaphore::new(max_size as usize)),
            available: Notify::new(),
            client,
            handle: OnceLock::new(),
            worker_completion: OnceLock::new(),
            shutdown_completion: OnceLock::new(),
        });

        if pool.config.min_idle > 0 {
            let pool_ = Arc::clone(&pool);

            let min_idle = pool_.config.min_idle;
            let idle_timeout = pool_.config.idle_timeout;
            let pool = Arc::downgrade(&pool_);

            // Sender lifetime covers the entire worker future, including a
            // future aborted before its first poll. No values are ever sent.
            let (worker_lifetime, worker_completion) = tokio::sync::watch::channel(());
            pool_
                .worker_completion
                .set(worker_completion)
                .expect("worker not started");
            let handle = E::spawn(async move {
                let _worker_lifetime = worker_lifetime;
                loop {
                    #[cfg(feature = "tracing")]
                    tracing::trace!("running cleanup tasks");

                    match pool.upgrade() {
                        Some(pool) => {
                            #[allow(clippy::needless_collect)]
                            let (count, dropped) = {
                                let mut connections =
                                    pool.connections.lock().expect("connection pool lock");
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
                                let Ok(permit) = Arc::clone(&pool.admission).try_acquire_owned()
                                else {
                                    break;
                                };
                                let conn = match pool.client.connection().await {
                                    Ok(conn) => conn,
                                    Err(err) => {
                                        #[cfg(feature = "tracing")]
                                        tracing::warn!("couldn't create idle connection {}", err);
                                        #[cfg(not(feature = "tracing"))]
                                        let _ = err;

                                        break;
                                    }
                                };

                                let mut connections =
                                    pool.connections.lock().expect("connection pool lock");
                                let Some(connections) = connections.as_mut() else {
                                    // The transport was shut down
                                    return;
                                };

                                connections.push(ParkedConnection::park(conn, permit));
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

                                abort_concurrent(dropped.into_iter().map(ParkedConnection::unpark))
                                    .await;
                            }
                        }
                        None => {
                            #[cfg(feature = "tracing")]
                            tracing::warn!(
                                "breaking out of task - no more references to Pool are available"
                            );
                            break;
                        }
                    }

                    E::sleep(pool_worker_interval(idle_timeout)).await;
                }
            });
            pool_
                .handle
                .set(handle)
                .expect("handle hasn't been set yet");
        }

        pool
    }

    pub(crate) async fn shutdown(self: &Arc<Self>) {
        let completion = self
            .shutdown_completion
            .get_or_init(|| {
                // The idle set must stay owned until its aborts finish, even when
                // the caller is dropped. Concurrent shutdowns await that same work.
                self.admission.close();
                self.available.notify_waiters();
                let connections = self
                    .connections
                    .lock()
                    .expect("connection pool lock")
                    .take();
                let pool = Arc::clone(self);
                let (done, mut completed) = tokio::sync::watch::channel(false);
                E::spawn(async move {
                    pool.shutdown_inner(connections).await;
                    let _ = done.send(true);
                });
                async move {
                    while !*completed.borrow_and_update() {
                        assert!(
                            completed.changed().await.is_ok(),
                            "SMTP teardown task exited without completing"
                        );
                    }
                }
                .boxed()
                .shared()
            })
            .clone();
        completion.await;
    }

    async fn shutdown_inner(&self, connections: Option<Vec<ParkedConnection>>) {
        if let Some(connections) = connections {
            abort_concurrent(connections.into_iter().map(ParkedConnection::unpark)).await;
        }

        if let Some(handle) = self.handle.get() {
            handle.shutdown().await;
            if let Some(completed) = self.worker_completion.get() {
                // Executor shutdown requests cancellation; sender drop proves
                // the worker has actually released its connections and dials.
                let _ = completed.clone().changed().await;
            }
        }
    }

    pub(crate) async fn connection(self: &Arc<Self>) -> Result<PooledConnection<E>, Error> {
        if self.config.max_size == 0 {
            return Err(error::invalid_input(
                "pool max_size must be greater than zero",
            ));
        }
        loop {
            let notified = self.available.notified();
            let conn = {
                let mut connections = self.connections.lock().expect("connection pool lock");
                let Some(connections) = connections.as_mut() else {
                    // The transport was shut down
                    return Err(error::transport_shutdown());
                };
                connections.pop()
            };

            match conn {
                Some(conn) => {
                    if idle_connection_expired(conn.idle_duration(), self.config.idle_timeout) {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("dropping an expired connection");

                        let (mut conn, parked_permit) = conn.unpark();
                        conn.abort().await;
                        drop(parked_permit);
                        continue;
                    }
                    let (mut conn, parked_permit) = conn.unpark();

                    if conn.has_broken() {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("dropping a broken connection");

                        conn.abort().await;
                        drop(parked_permit);
                        continue;
                    }

                    if self.config.test_on_checkout && !conn.test_connected().await {
                        #[cfg(feature = "tracing")]
                        tracing::debug!("dropping a broken connection");

                        conn.abort().await;
                        drop(parked_permit);
                        continue;
                    }

                    #[cfg(feature = "tracing")]
                    tracing::debug!("reusing a pooled connection");

                    return Ok(PooledConnection::wrap(
                        conn,
                        Arc::clone(self),
                        parked_permit,
                    ));
                }
                None => {
                    tokio::select! {
                        permit = Arc::clone(&self.admission).acquire_owned() => {
                            let permit = permit.map_err(|_| error::transport_shutdown())?;

                            // Winning admission is not permission to dial. The
                            // permit may have been released by a connection
                            // recycled after `shutdown()` took the idle set, and
                            // an idle connection may have been parked while this
                            // checkout waited. Re-read the pool state under the
                            // lock before touching the network.
                            {
                                let connections =
                                    self.connections.lock().expect("connection pool lock");
                                match connections.as_ref() {
                                    None => {
                                        drop(connections);
                                        drop(permit);
                                        return Err(error::transport_shutdown());
                                    }
                                    Some(parked) if !parked.is_empty() => {
                                        drop(connections);
                                        drop(permit);
                                        continue;
                                    }
                                    Some(_) => {}
                                }
                            }

                            #[cfg(feature = "tracing")]
                            tracing::debug!("creating a new connection");

                            let conn = self.client.connection().await?;
                            return Ok(PooledConnection::wrap(conn, Arc::clone(self), permit));
                        }
                        () = notified => continue,
                    }
                }
            }
        }
    }
}

impl<E: Executor> Pool<E> {
    /// Park an already-established connection so a test can exercise checkout
    /// and recycle without dialing anything.
    #[cfg(test)]
    pub(crate) async fn park_for_test(&self, conn: AsyncSmtpConnection) {
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .expect("test connection fits pool bound");
        self.connections
            .lock()
            .expect("connection pool lock")
            .as_mut()
            .expect("pool is not shut down")
            .push(ParkedConnection::park(conn, permit));
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

    fn recycle(&self, conn: AsyncSmtpConnection, permit: OwnedSemaphorePermit) {
        // A connection that has served an LMTP final-status drain is retired
        // rather than recycled: a surplus final status below the read buffer
        // cannot be detected without a read that would block on a well-behaved
        // peer, so reuse could silently consume it as the next reply.
        if conn.has_broken() || conn.should_retire() {
            #[cfg(feature = "tracing")]
            tracing::debug!("dropping a broken or retired connection instead of recycling it");

            drop(conn);
        } else {
            #[cfg(feature = "tracing")]
            tracing::debug!("recycling connection");

            let mut connections_guard = self.connections.lock().expect("connection pool lock");

            if let Some(connections) = connections_guard.as_mut() {
                connections.push(ParkedConnection::park(conn, permit));
                self.available.notify_one();
            } else {
                // The pool has already been shut down
                drop(connections_guard);
                drop(conn);
            }
        }
    }
}

impl<E: Executor> Debug for Pool<E> {
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

                    Err(_) => "LOCKED".to_owned(),
                },
            )
            .field("client", &self.client)
            .field(
                "handle",
                &match self.handle.get() {
                    Some(_) => "Some(JoinHandle)",
                    None => "None",
                },
            )
            .finish()
    }
}

impl<E: Executor> Drop for Pool<E> {
    fn drop(&mut self) {
        #[cfg(feature = "tracing")]
        tracing::debug!("dropping Pool");

        self.admission.close();
        let _ = self
            .connections
            .get_mut()
            .expect("connection pool lock")
            .take();
        let _ = self.handle.take();
    }
}

impl ParkedConnection {
    fn park(conn: AsyncSmtpConnection, permit: OwnedSemaphorePermit) -> Self {
        Self {
            conn,
            since: Instant::now(),
            permit,
        }
    }

    fn idle_duration(&self) -> Duration {
        self.since.elapsed()
    }

    fn unpark(self) -> (AsyncSmtpConnection, OwnedSemaphorePermit) {
        (self.conn, self.permit)
    }
}

impl<E: Executor> PooledConnection<E> {
    fn wrap(conn: AsyncSmtpConnection, pool: Arc<Pool<E>>, permit: OwnedSemaphorePermit) -> Self {
        Self {
            conn: Some(conn),
            pool,
            permit: Some(permit),
        }
    }
}

impl<E: Executor> Deref for PooledConnection<E> {
    type Target = AsyncSmtpConnection;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("conn hasn't been dropped yet")
    }
}

impl<E: Executor> DerefMut for PooledConnection<E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("conn hasn't been dropped yet")
    }
}

impl<E: Executor> Drop for PooledConnection<E> {
    fn drop(&mut self) {
        let conn = self
            .conn
            .take()
            .expect("AsyncSmtpConnection hasn't been taken yet");
        let pool = Arc::clone(&self.pool);
        let permit = self
            .permit
            .take()
            .expect("pool permit hasn't been taken yet");
        pool.recycle(conn, permit);
    }
}

async fn abort_concurrent<I>(iter: I)
where
    I: Iterator<Item = (AsyncSmtpConnection, OwnedSemaphorePermit)>,
{
    futures::future::join_all(iter.map(|(mut conn, _permit)| async move {
        conn.abort().await;
    }))
    .await;
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::abort_concurrent;
    use crate::transport::smtp::{
        Protocol, client::AsyncSmtpConnection, extension::ClientId, test_support::Transcript,
    };

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn abort_concurrent_does_not_serialize_shutdown_timeouts() {
        let hello = ClientId::Domain("client.example".to_owned());
        let mut connections = Vec::new();
        for _ in 0..2 {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
                .stall_shutdown();
            connections.push(
                AsyncSmtpConnection::from_transcript_with_timeout(
                    transcript,
                    &hello,
                    Protocol::Smtp,
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
            );
        }

        let permits = Arc::new(tokio::sync::Semaphore::new(2));
        let connections = connections.into_iter().map(|connection| {
            let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
            (connection, permit)
        });
        let mut aborts = Box::pin(abort_concurrent(connections));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(aborts.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::timeout(Duration::ZERO, aborts)
            .await
            .expect("all shutdown timeouts must elapse concurrently");
    }
}
