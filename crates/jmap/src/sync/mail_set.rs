//! Email/set execution and per-target answers shared by bulk and single doors.
use super::state_cache::{self, StateMap};
use crate::core::transport::HttpTransport;
use crate::email::{Email, EmailGet, EmailId, EmailSet};
use bifrost_types::{
    AccountOperation, BatchFailure, BatchItemId, BatchSuccess, ErrorScope, ItemOutcome,
    MutationSuccess, ObjectId,
};
type MailAccount<T> = crate::account::Account<T>;

/// Outlook users expect narrow edits to survive unrelated mailbox activity.
/// Whole-property replacements use a cached guard with one refusal-only refresh.
/// Caller versions and read-dependent selections retain their exact guard:
/// refreshing either could overwrite an edit or destroy a message moved out of Trash.
#[derive(Clone, Copy)]
pub(super) enum Guard<'a> {
    Unconditional,
    Replacement,
    Exact(&'a crate::core::id::ObjectState<Email>),
}

impl Guard<'_> {
    pub(super) fn is_guarded(self) -> bool {
        !matches!(self, Self::Unconditional)
    }
}

pub(super) async fn execute<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
    guard: Guard<'_>,
    mut make_set: impl FnMut() -> crate::Result<EmailSet>,
) -> crate::Result<crate::core::set::SetResponse<Email>> {
    let state = match guard {
        Guard::Unconditional => None,
        Guard::Exact(state) => Some(state.clone()),
        Guard::Replacement => Some(current_or_probe_state(mail, email_states, account_id).await?),
    };
    let set = make_set()?;
    let set = match state {
        Some(state) => set.if_in_state(state),
        None => set,
    };
    let first = mail.call(set).await;
    let response = match first {
        Err(error)
            if matches!(guard, Guard::Replacement) && super::error::is_state_mismatch(&error) =>
        {
            let expected = state_cache::guard(email_states, account_id).await;
            let fresh = probe_email_state(mail).await?;
            let fresh =
                state_cache::observe_probe(email_states, account_id, expected.as_ref(), fresh)
                    .await;
            mail.call(make_set()?.if_in_state(fresh)).await?
        }
        result => result?,
    };
    state_cache::observe_set(
        email_states,
        account_id,
        state_cache::set_transition(response.old_state(), response.new_state()),
    )
    .await;
    Ok(response)
}

/// One answer vocabulary for every mail-set front end. A vanished target
/// is Skipped under the shared absent-target policy: an Outlook user expects
/// it gone from view, without an error or a claim that this call moved it.
/// Partial-commit adapters record Applied writes only, never skipped targets.
pub(super) fn classify_set_response(
    raw: crate::Result<()>,
    id: ObjectId,
    operation: AccountOperation,
) -> ItemOutcome<MutationSuccess> {
    match raw {
        Ok(()) => ItemOutcome::Succeeded(BatchSuccess::new(
            BatchItemId(id.0.clone()),
            MutationSuccess::Applied,
        )),
        Err(crate::Error::Set(set_error)) => {
            let ctx = super::error::JmapErrorContext::new(operation);
            let item_scope = Some(ErrorScope::Message {
                id: (id.0.clone()).into(),
            });
            super::error::classify_set_item(set_error, ctx, BatchItemId(id.0.clone()), item_scope)
        }
        Err(err) => ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            super::error::into_account_error(
                err,
                super::error::JmapErrorContext::new(operation).with_scope(ErrorScope::Message {
                    id: (id.0.clone()).into(),
                }),
            ),
        )),
    }
}

async fn current_or_probe_state<T: HttpTransport>(
    mail: &MailAccount<T>,
    email_states: &StateMap,
    account_id: &str,
) -> crate::Result<crate::core::id::ObjectState<Email>> {
    if let Some(state) = state_cache::guard(email_states, account_id).await {
        return Ok(state);
    }
    let state = probe_email_state(mail).await?;
    Ok(state_cache::observe_probe(email_states, account_id, None, state).await)
}

pub(crate) async fn probe_email_state<T: HttpTransport>(
    mail: &MailAccount<T>,
) -> crate::Result<crate::core::id::ObjectState<Email>> {
    let state = mail
        .call(EmailGet::new().ids(Vec::<EmailId>::new()))
        .await?
        .into_state();
    // An Outlook user expects a usable concurrency guard, never an empty token.
    if state.as_str().is_empty() {
        return Err(crate::Error::MissingResponseField(
            "nonempty Email/get state",
        ));
    }
    Ok(state)
}
