//! Session dispatch; the engine owns recovery and pacing.
use super::{GraphAccount, PushConfig};
use bifrost_types::{
    AccountStream, BackoffPolicy, PushGate, PushLane, PushLaneKind, PushLaneSpec, PushSessionEvent,
    SessionStop,
};
pub(crate) fn push_lanes(account: &GraphAccount) -> Vec<PushLaneSpec> {
    let (kind, backoff) = match &account.push_config {
        PushConfig::EwsStreaming => (
            PushLaneKind::Connection,
            BackoffPolicy::new(
                std::time::Duration::from_secs(5),
                std::time::Duration::from_secs(320),
                true,
            ),
        ),
        PushConfig::Webhook(_) => (
            PushLaneKind::Registration,
            BackoffPolicy::new(
                super::push::renewal::RENEWAL_CHECK_INTERVAL,
                super::push::renewal::RENEWAL_CHECK_INTERVAL,
                false,
            ),
        ),
        _ => return Vec::new(),
    };
    vec![PushLaneSpec {
        lane: PushLane(0),
        kind,
        backoff,
    }]
}
pub(crate) fn push_stream(
    account: GraphAccount,
    lane: PushLane,
    stop: SessionStop,
    gate: PushGate,
) -> AccountStream<PushSessionEvent> {
    if lane != PushLane(0) || push_lanes(&account).is_empty() {
        return Box::pin(futures::stream::once(async move {
            PushSessionEvent::Failed(bifrost_types::push_lane_contract_error(
                bifrost_types::Protocol::Graph,
                lane,
                "not a lane of this account",
            ))
        }));
    }
    match &account.push_config {
        PushConfig::EwsStreaming => super::ews_stream::streaming_session(account, stop),
        _ => super::push::renewal::renewal_session(account, stop, gate),
    }
}
