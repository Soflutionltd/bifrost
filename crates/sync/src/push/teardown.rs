//! Provider deletion and settlement of one attachment's push obligations.

use bifrost_types::{Account, AccountError, AccountId, SubscriptionHandle};
use tokio::time::Instant;

use super::subscription::{RegisteredSubscription, SubscriptionRegistry};
use crate::throttle::ThrottleBucket;

/// Refusal retains both the provider error and any operation-local hold. Shared
/// holds are recorded by `delete` exactly once, even for detached cleanup.
pub(crate) struct TeardownRefusal {
    pub(crate) error: AccountError,
    pub(crate) local_deadline: Option<Instant>,
}

pub(crate) struct SubscriptionTeardown<'a> {
    registry: &'a SubscriptionRegistry,
    account_id: &'a AccountId,
    throttles: &'a ThrottleBucket,
}

impl SubscriptionRegistry {
    pub(crate) fn teardown<'a>(
        &'a self,
        account_id: &'a AccountId,
        throttles: &'a ThrottleBucket,
    ) -> SubscriptionTeardown<'a> {
        SubscriptionTeardown {
            registry: self,
            account_id,
            throttles,
        }
    }
}

impl SubscriptionTeardown<'_> {
    /// The caller keeps its record until this future returns. Cancellation must
    /// leave that obligation with its existing guard or registered orphan.
    /// Only timer retries impose a timeout; replacement cleanup must retain its
    /// close and subscription obligations until their provider calls answer.
    pub(crate) async fn delete(
        &self,
        account: &dyn Account,
        handle: &SubscriptionHandle,
    ) -> Result<(), TeardownRefusal> {
        account
            .push_unsubscribe(handle.clone())
            .await
            .map_err(|error| {
                let local_deadline = self.throttles.observe(self.account_id, &error);
                tracing::debug!(
                    target: "bifrost.sync.push",
                    account = ?self.account_id,
                    error = %error,
                    "push teardown refused"
                );
                TeardownRefusal {
                    error,
                    local_deadline,
                }
            })
    }

    /// Settle a registered handle while preserving any consumer desire. A
    /// reattach refusal must not make wanted push coverage disappear.
    pub(crate) fn settle_registered(
        &self,
        record: &RegisteredSubscription,
        result: &Result<(), TeardownRefusal>,
    ) {
        if record.desired() {
            if result.is_ok() {
                self.registry.mark_torn_down(&record.handle);
            } else {
                self.registry.mark_unconfirmed(&record.handle);
            }
        } else if result.is_ok() {
            // A timer snapshot may race replacement. Retire only an orphan;
            // it cannot withdraw coverage a consumer still wants.
            self.registry.remove_orphan(&record.handle);
        }
    }

    /// Delete an owned handle whose coverage is no longer wanted, then settle
    /// it without another suspension point. Its caller retains the original
    /// record so a replacement guard still owns cancellation before the reply.
    pub(crate) async fn delete_or_orphan(
        &self,
        account: &dyn Account,
        record: &RegisteredSubscription,
    ) -> Result<(), TeardownRefusal> {
        let result = self.delete(account, &record.handle).await;
        if result.is_ok() {
            self.registry.remove_orphan(&record.handle);
        } else {
            self.retain_orphans(vec![record.clone()]);
        }
        result
    }

    /// Callers restoring after a snapshot/replace acquire the reopen lock first.
    /// The registry's own shutdown check orders this transfer against detach.
    pub(crate) fn retain_orphans(&self, records: Vec<RegisteredSubscription>) {
        let count = records.len();
        let orphans = records
            .into_iter()
            .map(RegisteredSubscription::into_orphan)
            .collect();
        if self.registry.restore_orphans(orphans) {
            if count > 0 {
                tracing::warn!(
                    target: "bifrost.sync.push",
                    account = ?self.account_id,
                    retained = count,
                    "push teardown unconfirmed; retaining handles for retry"
                );
            }
        } else {
            tracing::warn!(
                target: "bifrost.sync.push",
                account = ?self.account_id,
                stranded = count,
                "slot detached before refused push cleanup could be retained; leaving handles to provider expiry"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn an_orphan_snapshot_cannot_retire_a_desired_subscription() {
        let registry = SubscriptionRegistry::new(CancellationToken::new());
        let handle = SubscriptionHandle("reused".into());
        assert!(registry.record(handle, vec![]));
        let orphan_snapshot = registry.snapshot()[0].clone().into_orphan();
        let account = AccountId("account".into());
        let throttles = ThrottleBucket::default();
        registry
            .teardown(&account, &throttles)
            .settle_registered(&orphan_snapshot, &Ok(()));
        let retained = registry.snapshot();
        assert_eq!(retained.len(), 1);
        assert!(retained[0].desired());
        assert!(!retained[0].torn_down());
    }
}
