//! The `Account` push doors: per-scope eligibility, mode dispatch, and the
//! three-lane outcome ledger every subscription request answers with.

use bifrost_types::{AccountError, AccountOperation, CursorScope, SubscriptionHandle};

use crate::account::graph_error::{GraphErrorContext, into_account_error, invalid_account_error};
use crate::account::{GraphAccount, PushMode};
use crate::error::GraphError;

use super::common::{
    DecodedHandle, decode_handle, no_subscribable_push_scopes, unsupported_push_error,
    unsupported_push_scope_error,
};
use super::ews::subscribe_ews;
use super::ews::unsubscribe_ews;
use super::webhook::{subscribe_graph, unsubscribe_graph};

pub(crate) async fn push_subscribe(
    account: GraphAccount,
    scopes: Vec<CursorScope>,
) -> Result<bifrost_types::PushSubscription, AccountError> {
    // A subscription covering nothing is not a subscription. Registering it
    // anyway minted a handle over an empty group: teardown had no server id
    // to walk, so the group was never retired and kept the renewal lane
    // believing it had a registration. The engine already skips an empty scope
    // list at its own reattach boundary, so this can only be a caller bug.
    if scopes.is_empty() {
        return Err(invalid_account_error(
            AccountOperation::PushSubscribe,
            "push_subscribe requires at least one scope",
        ));
    }
    let expected = push_item_ids(scopes.len());
    let mut outcomes = bifrost_types::BatchOutcomeBuilder::new();
    let mut eligible = Vec::with_capacity(scopes.len());
    for (item, scope) in expected.iter().cloned().zip(scopes) {
        // Public folders are poll-only in v1: a bare `CursorScope::Folder`
        // has no push surface (EWS streaming notifications do not cover the
        // public-folder hierarchy mailbox). It is refused per scope, not per
        // request: one stale or poll-only scope in a mixed list must not
        // disable push for every valid sibling, which is what bailing out
        // before dispatch did.
        if matches!(scope, CursorScope::Folder(_)) {
            outcomes.push_failed(item, unsupported_push_scope_error(scope));
            continue;
        }
        eligible.push((item, scope));
    }
    // Nothing in the request was ever subscribable. There is no partial
    // success to report and no handle to hold, so this stays a whole-request
    // refusal the caller can act on rather than an empty success.
    if eligible.is_empty() {
        return Err(unsupported_push_error());
    }
    subscribe_eligible(account, eligible, outcomes, &expected).await
}

/// What an arm answers once its ledger is closed: the handle it registered
/// (none when every eligible scope failed) and the finalized outcomes.
pub(super) type ArmOutcome = (
    Option<SubscriptionHandle>,
    bifrost_types::BatchOutcome<CursorScope>,
);

/// Dispatch the eligible scopes to the configured arm and shape its answer.
///
/// Each arm closes the ledger ITSELF, immediately before it registers
/// anything: a `finalize` failure must be raised while nothing is live yet.
/// Finalizing here, after the arm returned, meant the handle was already
/// registered and visible to push sessions when the `Err` - carrying no handle -
/// went back to a caller who could therefore never unsubscribe it.
///
/// Split out of `push_subscribe` so a test can hand it a ledger the arm
/// cannot close, which no real request can produce.
pub(super) async fn subscribe_eligible(
    account: GraphAccount,
    eligible: Vec<(bifrost_types::BatchItemId, CursorScope)>,
    outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope>,
    expected: &[bifrost_types::BatchItemId],
) -> Result<bifrost_types::PushSubscription, AccountError> {
    let (handle, outcomes) = match account.push_mode {
        PushMode::GraphSubscriptions => {
            subscribe_graph(account, eligible, outcomes, expected).await?
        }
        PushMode::EwsStreaming => subscribe_ews(account, eligible, outcomes, expected).await?,
    };
    // The arm produced no handle, which it does only when every eligible
    // scope ended up on the failed lane. That is the same "no scope was
    // subscribable at all" whole-request fault `push_subscribe`'s
    // pre-dispatch filter answers with `Err`, and it must answer the same way: a `PushSubscription`
    // with no handle is not a subscription, and returning one on the `Ok` arm
    // hands a caller matching on success an all-failed ledger and nothing to
    // unsubscribe.
    let Some(handle) = handle else {
        return Err(no_subscribable_push_scopes(outcomes.failed()));
    };
    bifrost_types::PushSubscription::new(Some(handle), outcomes).map_err(|failure| {
        into_account_error(
            GraphError::Internal {
                message: format!("push subscription invariant failed: {failure}"),
            },
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        )
    })
}

/// Positional ids for one `push_subscribe` request's scopes. The lanes are
/// keyed on submission position because `CursorScope` is not an id and two
/// requested scopes can legitimately name the same folder.
pub(super) fn push_item_ids(len: usize) -> Vec<bifrost_types::BatchItemId> {
    (0..len)
        .map(|index| bifrost_types::BatchItemId(index.to_string()))
        .collect()
}

/// Close the per-scope ledger, or report that this crate's accounting broke.
///
/// The lane ids are the positional ids `push_item_ids` mints, and every arm
/// files each one exactly once while walking its OWN list of scopes; server
/// answers (`translateExchangeIds`, subscription creates) are looked up by
/// key, never iterated into the ledger. Nothing server-supplied can therefore
/// make `finalize` fail, and a failure is `Internal(InvariantViolated)`, not
/// the provider contract violation it used to be filed as.
///
/// Called by each arm BEFORE it registers a handle or wakes a session, never
/// after: see `subscribe_eligible`.
pub(super) fn finalize_push_outcomes(
    outcomes: bifrost_types::BatchOutcomeBuilder<CursorScope>,
    expected: &[bifrost_types::BatchItemId],
) -> Result<bifrost_types::BatchOutcome<CursorScope>, AccountError> {
    outcomes.finalize(expected).map_err(|error| {
        into_account_error(
            GraphError::Internal {
                message: format!("push scope accounting invariant failed: {error}"),
            },
            GraphErrorContext::graph(AccountOperation::PushSubscribe),
        )
    })
}

pub(crate) async fn push_unsubscribe(
    account: GraphAccount,
    handle: SubscriptionHandle,
) -> Result<(), AccountError> {
    // Routed by what the handle says it is, not by this instance's mode: a
    // reopen can change the push mode, and a webhook handle retried on an EWS
    // instance would otherwise be dropped with its Graph subscription alive.
    // A handle of neither shape falls back to the mode.
    match (decode_handle(&handle), account.push_mode) {
        (DecodedHandle::Graph { .. }, _)
        | (DecodedHandle::Unrecognized, PushMode::GraphSubscriptions) => {
            unsubscribe_graph(account, handle).await
        }
        (DecodedHandle::Ews, _) | (DecodedHandle::Unrecognized, PushMode::EwsStreaming) => {
            unsubscribe_ews(account, handle).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{finalize_push_outcomes, push_item_ids};
    use bifrost_types::{
        AccountErrorKind, BatchOutcomeBuilder, CursorScope, InternalErrorKind, RecoveryClass,
    };

    /// A scope the ledger never filed is this crate's bug. It was
    /// `Protocol(ContractViolation)`, telling the operator to contact
    /// Microsoft about accounting only bifrost can get wrong.
    #[test]
    fn broken_push_accounting_is_internal_not_a_provider_fault() {
        let expected = push_item_ids(2);
        let mut outcomes = BatchOutcomeBuilder::new();
        outcomes.push_succeeded(expected[0].clone(), CursorScope::Account);
        let error = finalize_push_outcomes(outcomes, &expected)
            .expect_err("an unfiled scope must fail finalize");
        assert_eq!(
            error.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::InvariantViolated)
        );
        assert_eq!(error.recovery(), &RecoveryClass::InternalFailure);
    }
}
