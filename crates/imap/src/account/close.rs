use std::panic::AssertUnwindSafe;

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
                account.shutdown.cancel();
                let account = account.clone();
                let task = tokio::spawn(async move {
                    // Sends admitted before closure finish their side effects before
                    // SMTP shutdown. Later sends check the shutdown token
                    // while holding the gate.
                    let _send_gate = account.send_gate.write().await;
                    // A panicking composed provider must neither escape into
                    // engine detach nor prevent the remaining legs from closing.
                    // Catch construction as well as polling of each close future.
                    let mut first_error = None;
                    let stages = [
                        "SMTP shutdown",
                        "IMAP pool close",
                        "CardDAV close",
                        "CalDAV close",
                    ];
                    for (index, stage) in stages.into_iter().enumerate() {
                        let result = contain_panic(stage, async {
                            match index {
                                0 => {
                                    if let Some(submission) = &account.submission {
                                        submission.shutdown().await;
                                    }
                                }
                                1 => account.pool.close().await,
                                2 => {
                                    if let Some(contacts) = &account.contacts {
                                        contacts.close().await?;
                                    }
                                }
                                _ => {
                                    if let Some(calendars) = &account.calendars {
                                        calendars.close().await?;
                                    }
                                }
                            }
                            Ok(())
                        })
                        .await;
                        if let Err(error) = result {
                            first_error.get_or_insert(error);
                        }
                    }
                    first_error.map_or(Ok(()), Err)
                });
                async move {
                    task.await
                        .unwrap_or_else(|_| Err(teardown_error("account teardown task failed")))
                }
                .boxed()
                .shared()
            })
            .clone();
        completion.await
    })
}

fn teardown_error(detail: &str) -> AccountError {
    super::account_error_with(
        crate::Error::Internal {
            kind: crate::error::InternalFailure::Runtime(detail.to_string()),
            mid_exchange: None,
        },
        super::error::ImapErrorContext::operation(bifrost_types::AccountOperation::Close),
    )
}

async fn contain_panic(
    stage: &str,
    future: impl std::future::Future<Output = Result<(), AccountError>>,
) -> Result<(), AccountError> {
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(teardown_error(&format!("{stage} panicked"))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::test_support::scripted_account_parts;
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, tag_of,
    };
    use bifrost_types::{Account, DraftHandle, SendRequest};

    #[tokio::test]
    async fn provider_close_panics_do_not_escape_or_skip_the_next_provider() {
        use crate::account::test_support::StubAccount;
        use std::sync::{Arc, atomic::Ordering};
        for panic in [1, 2] {
            let (conn, peer) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
            drop(peer);
            let mut parts = scripted_account_parts(conn, 1);
            let mut contacts = StubAccount::new(Vec::new());
            contacts.close_panic = panic;
            let contacts = Arc::new(contacts);
            let calendars = Arc::new(StubAccount::new(Vec::new()));
            parts.contacts = Some(Arc::clone(&contacts) as Arc<dyn bifrost_types::Account>);
            parts.calendars = Some(Arc::clone(&calendars) as Arc<dyn bifrost_types::Account>);
            let account = ImapAccount::new(parts);
            let error = account.close().await.unwrap_err();
            assert_eq!(
                error.operation(),
                Some(bifrost_types::AccountOperation::Close)
            );
            assert!(contacts.close_called.load(Ordering::Relaxed));
            assert!(calendars.close_called.load(Ordering::Relaxed));
            assert!(
                account.close().await.is_err(),
                "later callers share the failure"
            );
        }
    }

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
            let expected = bifrost_types::AccountError::account_closed(
                bifrost_types::AccountOperation::Send,
                bifrost_types::Protocol::Imap,
            );
            assert_eq!(error.kind(), expected.kind());
        }
    }
}
