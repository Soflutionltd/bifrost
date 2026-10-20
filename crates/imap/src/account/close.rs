use std::sync::atomic::Ordering;

use bifrost_types::{AccountError, AccountFuture};

use super::ImapAccount;

pub(crate) fn close(account: ImapAccount) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        // This flag records that admission has closed; it cannot establish
        // that teardown completed. Pool::close removes its session registry
        // before awaiting LOGOUT and termination, so a cancelled first close
        // can leave later calls returning here without a completion to await.
        // Fixing that requires shared close-completion state across the account
        // and pool lifetimes, plus shutdown for the owned submission transport.
        if account.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        account.shutdown.cancel();
        account.push.stop();
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
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    })
}
