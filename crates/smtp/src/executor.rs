use std::fmt::Debug;
use std::time::Duration;

/// Async executor abstraction trait
///
/// Sealed Tokio task and timer services. Transport dialing is owned by the
/// SMTP connection adapter.
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub trait Executor: Debug + Send + Sync + 'static + private::Sealed {
    #[allow(private_bounds)]
    type Handle: SpawnHandle;
    type Sleep: Future<Output = ()> + Send + 'static;

    #[doc(hidden)]
    fn spawn<F>(fut: F) -> Self::Handle
    where
        F: Future<Output = ()> + Send + 'static,
        F::Output: Send + 'static;

    #[doc(hidden)]
    fn sleep(duration: Duration) -> Self::Sleep;
}

#[doc(hidden)]
pub(crate) trait SpawnHandle: Debug + Send + Sync + 'static + private::Sealed {
    fn shutdown(&self) -> impl Future<Output = ()> + Send + '_;
}

/// Async [`Executor`] using Tokio.
///
/// Provides Tokio task and timer services.
#[allow(missing_copy_implementations)]
#[non_exhaustive]
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
#[derive(Debug)]
pub struct TokioExecutor;

#[cfg(feature = "tokio")]
impl Executor for TokioExecutor {
    type Handle = tokio::task::JoinHandle<()>;
    type Sleep = tokio::time::Sleep;

    fn spawn<F>(fut: F) -> Self::Handle
    where
        F: Future<Output = ()> + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(fut)
    }

    fn sleep(duration: Duration) -> Self::Sleep {
        tokio::time::sleep(duration)
    }
}

#[cfg(feature = "tokio")]
impl SpawnHandle for tokio::task::JoinHandle<()> {
    async fn shutdown(&self) {
        self.abort();
    }
}

mod private {
    pub trait Sealed {}

    #[cfg(feature = "tokio")]
    impl Sealed for super::TokioExecutor {}

    #[cfg(feature = "tokio")]
    impl Sealed for tokio::task::JoinHandle<()> {}
}
