//! Gmail mail primitives, grouped by responsibility.

use super::error;
use bifrost_types::{AccountError, AccountFuture, QuotaInfo};

mod labels_single;
pub(crate) use labels_single::{
    add_to_container, delete_thread, move_thread, remove_from_container, set_is_read,
    set_label_membership,
};
mod compose;
pub(crate) use compose::{
    attachment_upload, cancel_scheduled_send_unsupported, draft_create, draft_discard, draft_send,
    draft_update, reschedule_send_unsupported, send_message, send_raw_message,
};
mod search;
pub(crate) use search::{search, search_messages};
mod settings;
pub(crate) use settings::{
    container_create, container_delete, container_move, container_rename, containers_list,
    identities_list, identity_update, vacation_get, vacation_set,
};
mod hydrate;
pub(crate) use hydrate::{message_hydrate, thread_hydrate};

#[cfg(test)]
mod test_support;

pub(crate) fn quota_get() -> AccountFuture<Result<Option<QuotaInfo>, AccountError>> {
    Box::pin(async { Err(unsupported(bifrost_types::AccountOperation::QuotaGet)) })
}

fn non_negative_i64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

fn unsupported(op: bifrost_types::AccountOperation) -> AccountError {
    error::into_account_error(
        crate::error::Error::unsupported(op),
        error::GmailErrorContext::base(op),
    )
}

fn other_error(op: bifrost_types::AccountOperation, detail: impl Into<String>) -> AccountError {
    error::into_account_error(
        crate::error::Error::invalid_request(op, detail),
        error::GmailErrorContext::base(op),
    )
}

/// Translate a `crate::Error` into `AccountError` with the given
/// per-call-site context. Every PIM call site uses this so the
/// operation, scope, and resource are correct for the classification.
fn account_error_for(error: crate::Error, ctx: error::GmailErrorContext) -> AccountError {
    error::into_account_error(error, ctx)
}
