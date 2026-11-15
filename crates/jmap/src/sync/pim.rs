//! Mail account doors, grouped by their protocol responsibility.

mod body;
mod containers;
mod drafts;
mod mail_mutation;
mod message;
mod search;
mod settings;
mod submission;

pub(crate) use containers::{
    container_create, container_delete, container_move, container_rename, containers_list,
};
pub(crate) use drafts::{attachment_upload, draft_create, draft_discard, draft_update};
pub(crate) use mail_mutation::{
    add_to_container, delete_thread, move_thread, remove_from_container, set_importance,
    set_is_read, set_keyword,
};
pub(crate) use message::{message_hydrate, thread_hydrate};
pub(crate) use search::{search, search_messages};
pub(crate) use settings::{
    identities_list, identity_update, quota_get, vacation_get, vacation_set,
};
pub(crate) use submission::ForeignSubmission;
pub(crate) use submission::{
    cancel_scheduled_send, draft_send, reschedule_send, send_message, send_raw_message,
};

type MailAccount<T> = super::registry::Slot<T>;
type SubmissionAccount<T> = super::registry::Slot<T, crate::core::capability::Submission>;
type VacationAccount<T> = super::registry::Slot<T, crate::core::capability::VacationResponseCap>;
type QuotaAccount<T> = super::registry::Slot<T, crate::core::capability::Quota>;

use bifrost_types::{AccountError, AccountOperation};
use std::time::{SystemTime, UNIX_EPOCH};

/// Convert a crate-internal error to `AccountError` with the correct
/// `AccountOperation` for this call site. Every call site in this
/// module passes the operation that produced the error; `Discover` is
/// no longer used as a default.
#[inline]
fn to_acct_err(op: AccountOperation) -> impl Fn(crate::Error) -> AccountError {
    move |err| super::error::into_account_error(err, super::error::JmapErrorContext::new(op))
}

// Validate at the caller boundary: an Outlook user's requested instant must
// never become the epoch or a saturated date when it cannot be represented.
fn checked_timestamp(
    time: SystemTime,
    operation: AccountOperation,
) -> Result<jiff::Timestamp, AccountError> {
    jiff::Timestamp::try_from(time)
        .map_err(|error| crate::sync::error::invalid_timestamp(operation, error.to_string()))
}

fn unix_to_system_time(timestamp: i64) -> Option<SystemTime> {
    if timestamp >= 0 {
        Some(UNIX_EPOCH + std::time::Duration::from_secs(u64::try_from(timestamp).ok()?))
    } else {
        UNIX_EPOCH.checked_sub(std::time::Duration::from_secs(timestamp.unsigned_abs()))
    }
}

#[cfg(test)]
mod tests;
