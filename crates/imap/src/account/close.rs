use std::sync::atomic::Ordering;

use bifrost_types::{AccountError, AccountFuture};
use futures::FutureExt;

use super::ImapAccount;

pub(crate) type Completion =
    futures::future::Shared<futures::future::BoxFuture<'static, Result<(), AccountError>>>;

pub(crate) fn close(account: ImapAccount) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let completion = account
            .close_completion
            .get_or_init(|| {
                // Admission and completion are separate. Spawn before the first await:
                // dropping any caller must not cancel the pool's registry drain, and
                // every caller must observe the same sub-account teardown result.
                account.closed.store(true, Ordering::Release);
                account.shutdown.cancel();
                account.push.stop();
                let account = account.clone();
                let task = tokio::spawn(async move {
                    // Sends admitted before closure finish their side effects before
                    // SMTP shutdown. Later sends check closed while holding the gate.
                    let _send_gate = account.send_gate.write().await;
                    if let Some(submission) = &account.submission {
                        submission.shutdown().await;
                    }
                    // The pool owns registered sessions only. A dial still inside
                    // connect/authentication is not registered yet; that dial must
                    // terminate itself when registration sees the closed pool.
                    // Waiting for such dials requires pool-level lifetime tracking.
                    account.pool.close().await;
                    let mut first_error = None;
                    if let Some(contacts) = &account.contacts
                        && let Err(error) = contacts.close().await
                    {
                        first_error = Some(error);
                    }
                    if let Some(calendars) = &account.calendars
                        && let Err(error) = calendars.close().await
                        && first_error.is_none()
                    {
                        first_error = Some(error);
                    }
                    first_error.map_or(Ok(()), Err)
                });
                async move { task.await.expect("account teardown task panicked") }
                    .boxed()
                    .shared()
            })
            .clone();
        completion.await
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::test_support::scripted_account_parts;
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, tag_of,
    };
    use bifrost_types::{Account, DraftHandle, SendRequest};

    #[tokio::test(start_paused = true)]
    async fn cancelled_close_and_concurrent_close_share_the_session_drain() {
        let (conn, mut peer) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        let account = ImapAccount::new(scripted_account_parts(conn, 1));
        // Retain a checked-out handle: completion must terminate its driver too.
        let held = account.pool.checkout_any().await.unwrap();
        let mut first = account.close();
        assert!(futures::poll!(first.as_mut()).is_pending());
        let logout = read_line(&mut peer).await;
        assert!(logout.contains("LOGOUT"));
        let mut second = account.close();
        assert!(futures::poll!(second.as_mut()).is_pending());
        drop(first);
        assert!(held.connection().is_alive());
        respond(
            &mut peer,
            &format!("* BYE closing\r\n{} OK logout\r\n", tag_of(&logout)),
        )
        .await;
        second.await.unwrap();
        assert!(!held.connection().is_alive());
        account.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_waits_for_admitted_sends_and_refuses_all_send_entry_points() {
        let (conn, peer) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        drop(peer);
        let account = ImapAccount::new(scripted_account_parts(conn, 1));
        let admitted_send = account.send_gate.read().await;
        let mut closing = account.close();
        assert!(futures::poll!(closing.as_mut()).is_pending());
        tokio::task::yield_now().await;
        assert!(futures::poll!(closing.as_mut()).is_pending());
        drop(admitted_send);
        closing.await.unwrap();
        // No submission is configured. The close gate must win even over the
        // unsupported or malformed-input paths, before any rendering or I/O.
        for error in [
            account
                .send_message(SendRequest::default())
                .await
                .unwrap_err(),
            account
                .send_raw_message(bytes::Bytes::new(), None)
                .await
                .unwrap_err(),
            account
                .draft_send(DraftHandle("invalid".into()))
                .await
                .unwrap_err(),
        ] {
            let expected = super::super::account_error_with(
                crate::Error::closed(),
                super::super::error::ImapErrorContext::operation(
                    bifrost_types::AccountOperation::Send,
                ),
            );
            assert_eq!(error.kind(), expected.kind());
        }
    }
}
