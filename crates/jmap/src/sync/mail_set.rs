//! Email/set execution and per-target answers shared by bulk and single doors.
use super::state_cache;
use crate::core::transport::HttpTransport;
use crate::email::{Email, EmailGet, EmailId, EmailSet};
use bifrost_types::{
    AccountOperation, BatchFailure, BatchItemId, BatchSuccess, ErrorScope, ItemOutcome,
    MutationSuccess, ObjectId,
};
type MailAccount<T> = super::registry::Slot<T>;

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
    guard: Guard<'_>,
    mut make_set: impl FnMut() -> crate::Result<EmailSet>,
) -> crate::Result<crate::core::set::SetResponse<Email>> {
    let email_states = &mail.email_states;

    let state = match guard {
        Guard::Unconditional => None,
        Guard::Exact(state) => Some(state.clone()),
        Guard::Replacement => Some(current_or_probe_state(mail).await?),
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
            let expected = email_states.guard().await;
            let fresh = probe_email_state(mail).await?;
            let fresh = email_states.observe_probe(expected.as_ref(), fresh).await;
            mail.call(make_set()?.if_in_state(fresh)).await?
        }
        result => result?,
    };
    email_states
        .observe_set(state_cache::set_transition(
            response.old_state(),
            response.new_state(),
        ))
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
            // Only notFound names the mutation subject. blobNotFound names
            // missing input content and cannot establish that the mail is gone.
            if matches!(
                set_error.error_type(),
                crate::core::set::SetErrorType::NotFound
            ) && operation.skips_absent_target()
            {
                ItemOutcome::Succeeded(BatchSuccess::new(
                    BatchItemId(id.0.clone()),
                    MutationSuccess::Skipped,
                ))
            } else {
                ItemOutcome::Failed(BatchFailure::new(
                    BatchItemId(id.0.clone()),
                    super::error::set_error_to_account_error(set_error, ctx, item_scope),
                ))
            }
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
) -> crate::Result<crate::core::id::ObjectState<Email>> {
    let email_states = &mail.email_states;

    if let Some(state) = email_states.guard().await {
        return Ok(state);
    }
    let state = probe_email_state(mail).await?;
    Ok(email_states.observe_probe(None, state).await)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_subject_policy_distinguishes_mail_edits_from_missing_inputs() {
        for (operation, error_type, skipped) in [
            (AccountOperation::UpdateFlags, "notFound", true),
            (AccountOperation::BulkMove, "notFound", true),
            (AccountOperation::SetKeyword, "notFound", true),
            (AccountOperation::DraftSend, "notFound", false),
            (AccountOperation::UpdateFlags, "blobNotFound", false),
        ] {
            let error = serde_json::from_value(serde_json::json!({"type": error_type})).unwrap();
            let outcome = classify_set_response(
                Err(crate::Error::Set(error)),
                ObjectId("message".into()),
                operation,
            );
            if skipped {
                assert!(matches!(outcome, ItemOutcome::Succeeded(success)
                    if matches!(success.output, MutationSuccess::Skipped)));
            } else {
                assert!(matches!(outcome, ItemOutcome::Failed(_)));
            }
        }
    }
}
