//! Names a wait that does not end.
//!
//! The waits a live change has to pass (the scope's drive lease, the
//! scheduler's admission, the provider call itself) are silent while they
//! last. A wait that never ends therefore stops a scope's live changes with
//! nothing in the log to say which one, so each of them is awaited here and
//! warns, with its scope, for as long as it stays pending.

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

/// How long a wait runs before the first warning, and between warnings.
const STALL_WARN_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) async fn warn_while_pending<F: Future>(
    future: F,
    what: &'static str,
    subject: &impl Debug,
) -> F::Output {
    let started = tokio::time::Instant::now();
    tokio::pin!(future);
    loop {
        tokio::select! {
            output = &mut future => return output,
            () = tokio::time::sleep(STALL_WARN_INTERVAL) => {
                tracing::warn!(
                    target: "bifrost.sync.stall",
                    subject = ?subject,
                    waited_secs = started.elapsed().as_secs(),
                    "{what} has not finished"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_slow_wait_still_returns_its_output() {
        let slow = async {
            tokio::time::sleep(STALL_WARN_INTERVAL * 3).await;
            7
        };
        assert_eq!(warn_while_pending(slow, "test wait", &"scope").await, 7);
    }
}
