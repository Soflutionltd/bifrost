//! The slot's wire-start ordering, shared by independent producer lanes.

use super::*;
use crate::scheduler::{BudgetPermit, WorkKind};
use crate::types::Incarnation;

/// Admission overflow is pressure, not a failed protocol attempt. One second
/// prevents a full scheduler lane from turning many accounts into a hot retry
/// loop, while keeping foreground work responsive to a brief burst. Provider
/// retry budgets and poll cadence do not control this engine-local refusal.
pub(crate) const ADMISSION_RETRY: Duration = Duration::from_secs(1);

pub(crate) struct DriveStart<'a> {
    pub current: &'a ArcSwap<Incarnation>,
    pub control: &'a SyncControl,
    pub scheduler: &'a Scheduler,
    pub throttles: &'a crate::throttle::ThrottleBucket,
    pub account_id: &'a AccountId,
    pub shutdown: &'a CancellationToken,
}

pub(crate) struct Admitted {
    pub incarnation: Arc<Incarnation>,
    pub activity: SyncActivityGuard,
    pub permit: BudgetPermit,
}

/// Recovery already owns its pinned account under the reopen lock. Do not
/// return an installed incarnation as though it were that replacement.
pub(crate) struct RecoveryAdmission {
    pub activity: SyncActivityGuard,
    pub permit: BudgetPermit,
}

#[non_exhaustive]
pub(crate) enum StartRefused {
    Held {
        scope_local: bool,
        until: tokio::time::Instant,
    },
    Paused,
    ShuttingDown,
}

impl DriveStart<'_> {
    /// Check immediately after an external wait (a drive lease or writer read).
    /// The caller releases its admission on refusal before reacting to the hold.
    pub fn recheck(
        &self,
        keys: impl FnOnce(&dyn Account) -> Vec<bifrost_types::ThrottleKey>,
    ) -> Result<Arc<Incarnation>, StartRefused> {
        if self.shutdown.is_cancelled() {
            return Err(StartRefused::ShuttingDown);
        }
        // Transactional recovery supplies admitted_by(outer_activity): its
        // delete-then-establish sequence must finish even if Pause lands. A
        // new drive supplies the ordinary control and is refused instead.
        if self.control.drive_boundary().peek() == BoundaryRequest::Stop {
            return Err(StartRefused::ShuttingDown);
        }
        let activity = self.control.begin_activity().ok_or(StartRefused::Paused)?;
        let incarnation = self.current.load_full();
        let result = self.check_holds(incarnation.as_ref().as_ref(), keys);
        drop(activity);
        result.map(|()| incarnation)
    }

    fn check_holds(
        &self,
        account: &dyn Account,
        keys: impl FnOnce(&dyn Account) -> Vec<bifrost_types::ThrottleKey>,
    ) -> Result<(), StartRefused> {
        if let Some(wait) = crate::throttle::account_throttle_wait(self.throttles, self.account_id)
        {
            return Err(StartRefused::Held {
                scope_local: false,
                until: tokio::time::Instant::now() + wait,
            });
        }
        if let Some(wait) =
            crate::throttle::throttle_wait(self.throttles, self.account_id, &keys(account))
        {
            return Err(StartRefused::Held {
                scope_local: true,
                until: tokio::time::Instant::now() + wait,
            });
        }
        Ok(())
    }

    pub async fn admit_drive(
        &self,
        work: bifrost_types::ThrottleWork<'_>,
        kind: WorkKind,
    ) -> Result<Admitted, StartRefused> {
        self.admit_keys(kind, |account| {
            crate::throttle::declared_keys(account, self.account_id, work)
        })
        .await
    }

    /// Multiple inputs (repair scopes or a move's objects and memberships) are
    /// one operation; ask the freshly loaded incarnation about every input.
    pub async fn admit_keys(
        &self,
        kind: WorkKind,
        keys: impl Fn(&dyn Account) -> Vec<bifrost_types::ThrottleKey>,
    ) -> Result<Admitted, StartRefused> {
        self.admit_inner(kind, keys, true, None).await
    }

    /// Recovery must charge hold waits to its cumulative allowance and keep
    /// the account it owns under the reopen lock, including a replacement not
    /// yet installed in current. Return holds to RepairPacing without sleeping.
    pub async fn admit_recovery(
        &self,
        kind: WorkKind,
        account: Option<&dyn Account>,
        keys: impl Fn(&dyn Account) -> Vec<bifrost_types::ThrottleKey>,
    ) -> Result<RecoveryAdmission, StartRefused> {
        self.admit_inner(kind, keys, false, account)
            .await
            .map(|admitted| RecoveryAdmission {
                activity: admitted.activity,
                permit: admitted.permit,
            })
    }

    async fn admit_inner(
        &self,
        kind: WorkKind,
        keys: impl Fn(&dyn Account) -> Vec<bifrost_types::ThrottleKey>,
        wait_account: bool,
        pinned: Option<&dyn Account>,
    ) -> Result<Admitted, StartRefused> {
        let mut boundary = self.control.drive_boundary();
        loop {
            if self.shutdown.is_cancelled() || boundary.peek() == BoundaryRequest::Stop {
                return Err(StartRefused::ShuttingDown);
            }
            let activity = self.control.begin_activity().ok_or(StartRefused::Paused)?;
            let before = self.current.load_full();
            let holds = self.check_holds(pinned.unwrap_or_else(|| before.as_ref().as_ref()), &keys);
            drop(before);
            match holds {
                Err(StartRefused::Held {
                    scope_local: false,
                    until,
                }) if wait_account => {
                    drop(activity);
                    tokio::select! {
                        biased;
                        () = self.shutdown.cancelled() => return Err(StartRefused::ShuttingDown),
                        _ = boundary.changed() => {},
                        () = tokio::time::sleep_until(until) => {}
                    }
                    // Re-register activity and reload both account and keys.
                    continue;
                }
                Err(refusal) => return Err(refusal),
                Ok(()) => {}
            }
            let result = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Err(StartRefused::ShuttingDown),
                _ = boundary.changed() => {
                    drop(activity);
                    continue;
                }
                permit = self.scheduler.admit(self.account_id.clone(), self.control.priority_snapshot(), kind) => permit,
            };
            let permit = match result {
                Ok(permit) => permit,
                // Admission abandoned: the scheduler's dispatcher is gone, so
                // retrying would hang until shutdown. Surface it to the caller
                // instead of looping; only a full lane is backpressure.
                Err(crate::Error::ShuttingDown) => return Err(StartRefused::ShuttingDown),
                Err(crate::Error::Admission(crate::AdmissionFailure::LaneFull { .. })) => {
                    drop(activity);
                    tokio::select! {
                        biased;
                        () = self.shutdown.cancelled() => return Err(StartRefused::ShuttingDown),
                        _ = boundary.changed() => {},
                        () = tokio::time::sleep(ADMISSION_RETRY) => {}
                    }
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, "scheduler admission cannot recover on this attachment");
                    return Err(StartRefused::ShuttingDown);
                }
            };
            // Admission is an internal wait. A hold, replacement, pause or Stop
            // arriving in it must be seen before opening any provider stream.
            let checked = self.recheck(|installed| keys(pinned.unwrap_or(installed)));
            match checked {
                Ok(incarnation) => {
                    return Ok(Admitted {
                        incarnation,
                        activity,
                        permit,
                    });
                }
                Err(refusal) => {
                    drop(permit);
                    drop(activity);
                    return Err(refusal);
                }
            }
        }
    }
}

impl crate::types::AccountSlot {
    pub(crate) fn drive_start<'a>(&'a self, account_id: &'a AccountId) -> DriveStart<'a> {
        DriveStart {
            current: &self.current,
            control: &self.control,
            scheduler: &self.scheduler,
            throttles: &self.throttles,
            account_id,
            shutdown: &self.shutdown,
        }
    }
}
