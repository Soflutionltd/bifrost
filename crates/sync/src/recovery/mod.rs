//! Shared recovery classification and stream-failure dispatch.

mod plan;
pub(crate) use plan::{
    HINTLESS_RETRY_WAIT, REOPEN_WAIT, RecoveryPlan, dispatch_stream_failure,
    observe_failure_throttles, plan_recovery, reconcile_delay, retry_delay,
};
