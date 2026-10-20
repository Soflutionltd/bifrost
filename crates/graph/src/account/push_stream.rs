use bifrost_types::{
    AccountError, AccountStream, HintPayload, InvalidationHint, PushSource, WatchEvent,
};
use futures::stream;

use super::{GraphAccount, PushMode};

/// A broadcast overflow means invalidations were dropped and can never
/// be replayed: the events themselves are the only record that those
/// scopes changed. Swallowing the lag leaves the affected folders
/// waiting for the ordinary poll, so synthesize a coalesced
/// whole-account invalidation instead and let the engine reconcile.
fn coalesced_overflow_event() -> WatchEvent {
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::Coalesced,
            payload: HintPayload::Unknown,
        },
    }
}

/// Store terminal health before broadcasting it. The watch value survives
/// both late subscription and broadcast-ring overflow; the broadcast send
/// still wakes readers that are already waiting for ordinary events.
pub(crate) fn publish_terminal(account: &GraphAccount, error: AccountError) {
    account.push_terminal.send_replace(Some(error.clone()));
    let _ = account.push_tx.send(WatchEvent::Terminated(error));
}

pub(crate) fn push_stream(account: GraphAccount) -> AccountStream<WatchEvent> {
    let receiver = account.push_tx.subscribe();
    let terminal = account.push_terminal.subscribe();
    let shutdown = account.shutdown.clone();
    Box::pin(stream::unfold(
        (receiver, terminal, shutdown, false),
        |(mut receiver, mut terminal, shutdown, terminal_seen)| async move {
            loop {
                if terminal_seen {
                    return None;
                }
                let terminal_error = { terminal.borrow_and_update().clone() };
                if let Some(error) = terminal_error {
                    return Some((
                        WatchEvent::Terminated(error),
                        (receiver, terminal, shutdown, true),
                    ));
                }

                tokio::select! {
                    biased;
                    result = receiver.recv() => {
                        match result {
                            Ok(event) => {
                                let terminal_seen = matches!(&event, WatchEvent::Terminated(_));
                                return Some((event, (receiver, terminal, shutdown, terminal_seen)));
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                                let terminal_error = { terminal.borrow_and_update().clone() };
                                if let Some(error) = terminal_error {
                                    return Some((
                                        WatchEvent::Terminated(error),
                                        (receiver, terminal, shutdown, true),
                                    ));
                                }
                                tracing::warn!(
                                    dropped,
                                    "graph push channel lagged; synthesizing a coalesced invalidation"
                                );
                                return Some((
                                    coalesced_overflow_event(),
                                    (receiver, terminal, shutdown, false),
                                ));
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                        }
                    }
                    changed = terminal.changed() => {
                        if changed.is_err() {
                            return None;
                        }
                    }
                    () = shutdown.cancelled() => return None,
                }
            }
        },
    ))
}

pub(crate) async fn ensure_ews_worker(account: GraphAccount) {
    if account.push_mode != PushMode::EwsStreaming {
        return;
    }
    let worker_account = account.clone();
    super::worker_slot::ensure_worker(&account.ews_worker, move || {
        tokio::spawn(async move {
            super::ews_stream::run_streaming_worker(worker_account).await;
        })
    })
    .await;
}

#[cfg(test)]
mod tests {
    use bifrost_types::{AccountOperation, ProtocolErrorKind, WatchEvent};
    use futures::StreamExt;
    use tokio::sync::broadcast;

    use super::{publish_terminal, push_stream};
    use crate::account::{GraphAccount, PushMode};
    use crate::client::GraphClient;

    fn account() -> GraphAccount {
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions)
    }

    fn terminal_error() -> bifrost_types::AccountError {
        crate::account::graph_error::protocol_violation(
            ProtocolErrorKind::ContractViolation,
            AccountOperation::PushSubscribe,
            None,
            "scripted terminal push error",
        )
    }

    #[tokio::test]
    async fn a_subscriber_after_terminal_health_reads_the_latched_error() {
        let account = account();
        let error = terminal_error();
        publish_terminal(&account, error.clone());

        let mut events = push_stream(account);
        assert!(matches!(
            events.next().await,
            Some(WatchEvent::Terminated(terminal))
                if terminal.message_key() == error.message_key()
        ));
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn a_lagged_subscriber_reads_terminal_health_instead_of_losing_it() {
        let mut account = account();
        let (push_tx, _) = broadcast::channel(1);
        account.push_tx = push_tx;
        let mut events = push_stream(account.clone());
        let mut next = Box::pin(events.next());
        assert!(futures::poll!(next.as_mut()).is_pending());

        let _ = account.push_tx.send(WatchEvent::Disconnected);
        let _ = account.push_tx.send(WatchEvent::Reconnected);
        let error = terminal_error();
        publish_terminal(&account, error.clone());

        assert!(matches!(
            next.await,
            Some(WatchEvent::Terminated(terminal))
                if terminal.message_key() == error.message_key()
        ));
        assert!(events.next().await.is_none());
    }
}
