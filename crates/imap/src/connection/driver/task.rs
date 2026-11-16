//! Driver task lifetime and retained session resources.

use std::sync::Arc;
use std::time::Duration;

/// Values whose lifetime is tied to a driver task's transport (the pool's
/// session permit, a push slot): released only once the driver future, and
/// with it the socket, has been destroyed. `retain` and `mark_exited` are
/// linearized by `state`, and taken values are dropped outside the lock.
struct DriverExit {
    state: std::sync::Mutex<ExitState>,
}

enum ExitState {
    Running(Vec<Box<dyn Send>>),
    Exited,
}

impl DriverExit {
    fn running(values: Vec<Box<dyn Send>>) -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(ExitState::Running(values)),
        })
    }

    #[cfg(test)]
    fn exited() -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(ExitState::Exited),
        })
    }

    fn retain(&self, value: Box<dyn Send>) {
        let mut state = self.state.lock().expect("driver exit lock poisoned");
        if let ExitState::Running(values) = &mut *state {
            values.push(value);
            return;
        }
        drop(state);
        drop(value);
    }

    fn mark_exited(&self) {
        let previous = {
            let mut state = self.state.lock().expect("driver exit lock poisoned");
            std::mem::replace(&mut *state, ExitState::Exited)
        };
        drop(previous);
    }

    #[cfg(test)]
    fn has_exited(&self) -> bool {
        matches!(
            *self.state.lock().expect("driver exit lock poisoned"),
            ExitState::Exited
        )
    }
}

/// The future actually spawned for a driver. `inner` is destroyed BEFORE the
/// exit is published, on completion (inside `poll`) and in `Drop` (abort,
/// panic unwinding through tokio, runtime teardown). Built outside any async
/// block, so an abort before the first poll still runs the `Drop` path.
struct DriverScope<F> {
    inner: Option<std::pin::Pin<Box<F>>>,
    exit: Arc<DriverExit>,
}

impl<F: std::future::Future<Output = ()>> std::future::Future for DriverScope<F> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let this = self.get_mut();
        let result = this
            .inner
            .as_mut()
            .expect("driver present")
            .as_mut()
            .poll(cx);
        if result.is_ready() {
            this.inner = None;
            this.exit.mark_exited();
        }
        result
    }
}

impl<F> Drop for DriverScope<F> {
    fn drop(&mut self) {
        self.inner = None;
        self.exit.mark_exited();
    }
}

/// Hand-off cell for values that must be owned by a driver from the instant
/// it is spawned. The pool's dial task keeps one clone until its connector
/// future has been destroyed; the connector takes the values at spawn time
/// with no await in between.
#[derive(Clone)]
pub(crate) struct LeaseHandoff(Arc<std::sync::Mutex<Vec<Box<dyn Send>>>>);

impl LeaseHandoff {
    pub(crate) fn new(values: Vec<Box<dyn Send>>) -> Self {
        Self(Arc::new(std::sync::Mutex::new(values)))
    }

    #[cfg(test)]
    pub(crate) fn none() -> Self {
        Self::new(Vec::new())
    }

    pub(crate) fn take(&self) -> Vec<Box<dyn Send>> {
        std::mem::take(&mut *self.0.lock().expect("lease handoff lock poisoned"))
    }
}

pub(crate) struct DriverTask {
    exit: Arc<DriverExit>,
    pub(crate) handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    abort_handle: Option<tokio::task::AbortHandle>,
    inbound_bytes: Arc<std::sync::atomic::AtomicU64>,
}

impl DriverTask {
    /// Spawn `driver` under a `DriverScope` that owns `retained` until the
    /// driver future has been destroyed. The join handle is still kept:
    /// close joins the task, and the exit slot proves transport destruction
    /// without replacing that join.
    pub(crate) fn spawn<F>(
        driver: F,
        inbound_bytes: Arc<std::sync::atomic::AtomicU64>,
        retained: Vec<Box<dyn Send>>,
    ) -> Self
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let exit = DriverExit::running(retained);
        let scope = DriverScope {
            inner: Some(Box::pin(driver)),
            exit: Arc::clone(&exit),
        };
        let handle = tokio::spawn(scope);
        Self {
            abort_handle: Some(handle.abort_handle()),
            handle: tokio::sync::Mutex::new(Some(handle)),
            inbound_bytes,
            exit,
        }
    }

    /// For fakes with no driver: starts exited, so retained values drop at once.
    #[cfg(test)]
    pub(crate) fn absent() -> Self {
        Self {
            handle: tokio::sync::Mutex::new(None),
            abort_handle: None,
            inbound_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            exit: DriverExit::exited(),
        }
    }

    /// Keep `value` until the driver has exited, or drop it now if it has.
    pub(crate) fn retain_until_exit(&self, value: Box<dyn Send>) {
        self.exit.retain(value);
    }

    #[cfg(test)]
    pub(crate) fn has_exited(&self) -> bool {
        self.exit.has_exited()
    }

    pub(in crate::connection) fn bytes_in(&self) -> u64 {
        self.inbound_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn abort(&self) {
        if let Some(handle) = &self.abort_handle {
            handle.abort();
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.handle
            .try_lock()
            .map(|handle| {
                handle
                    .as_ref()
                    .is_none_or(tokio::task::JoinHandle::is_finished)
            })
            .unwrap_or(false)
    }

    /// Wait for graceful driver shutdown, aborting and joining it if the
    /// caller's bound expires.
    pub(crate) async fn finish_or_abort(&self, timeout: Duration) {
        let mut handle = self.handle.lock().await;
        let Some(task) = handle.as_mut() else {
            return;
        };
        if tokio::time::timeout(timeout, &mut *task).await.is_err() {
            task.abort();
            let _ = task.await;
        }
        handle.take();
    }

    pub(crate) async fn abort_and_join(&self) {
        let mut handle = self.handle.lock().await;
        let Some(task) = handle.as_mut() else {
            return;
        };
        task.abort();
        let _ = task.await;
        handle.take();
    }
}

/// Guard around keepalive setup, which submits no wire command. Marks the
/// connection abandoned unless the driver result was received and
/// [`completed`](Self::completed) was called.
pub(in crate::connection) struct InFlightGuard<'a> {
    pub(in crate::connection) flag: &'a std::sync::atomic::AtomicBool,
    pub(in crate::connection) completed: bool,
}

impl InFlightGuard<'_> {
    pub(in crate::connection) fn completed(mut self) {
        self.completed = true;
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.flag.store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{LOGOUT_DRAIN_TIMEOUT, test_support};

    struct ExitFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for ExitFlag {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn a_value_retained_by_a_running_driver_is_released_only_at_driver_exit() {
        let (conn, peer) =
            test_support::driver_pair(&test_support::preauth_greeting("IMAP4rev1")).await;
        let driver = conn.driver_task();
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        driver.retain_until_exit(Box::new(ExitFlag(Arc::clone(&flag))));
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        drop(peer);
        driver.finish_or_abort(LOGOUT_DRAIN_TIMEOUT).await;
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        assert!(driver.has_exited());
    }

    #[test]
    fn a_value_retained_after_driver_exit_is_released_at_once() {
        let driver = DriverTask::absent();
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        driver.retain_until_exit(Box::new(ExitFlag(Arc::clone(&flag))));
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        assert!(driver.has_exited());
    }

    #[tokio::test]
    async fn aborting_a_driver_before_its_first_poll_releases_its_retained_values() {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let driver = DriverTask::spawn(
            std::future::pending(),
            Arc::default(),
            vec![Box::new(ExitFlag(Arc::clone(&flag)))],
        );
        driver.abort();
        driver.abort_and_join().await;
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        assert!(driver.has_exited());
    }

    struct ExitOrder(Arc<std::sync::Mutex<Vec<&'static str>>>, &'static str);
    impl Drop for ExitOrder {
        fn drop(&mut self) {
            self.0.lock().unwrap().push(self.1);
        }
    }

    struct ExitOrderFuture {
        _guard: ExitOrder,
        pending: bool,
    }
    impl std::future::Future for ExitOrderFuture {
        type Output = ();
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if self.pending {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(())
            }
        }
    }

    #[tokio::test]
    async fn the_driver_future_is_destroyed_before_its_retained_values() {
        for abort in [false, true] {
            let order = Arc::new(std::sync::Mutex::new(Vec::new()));
            let guard = ExitOrder(Arc::clone(&order), "driver");
            let driver = DriverTask::spawn(
                ExitOrderFuture {
                    _guard: guard,
                    pending: abort,
                },
                Arc::default(),
                vec![Box::new(ExitOrder(Arc::clone(&order), "lease"))],
            );
            if abort {
                driver.abort_and_join().await;
            } else {
                driver.finish_or_abort(LOGOUT_DRAIN_TIMEOUT).await;
            }
            assert_eq!(*order.lock().unwrap(), ["driver", "lease"]);
        }
    }
}
