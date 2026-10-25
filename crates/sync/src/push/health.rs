//! Current push coverage, retained for late subscribers.
use bifrost_types::{AccountError, PushLane, PushLaneKind};

/// Push coverage of one account generation, one entry per lane in
/// `Account::push_lanes` order. Read through `SyncEngine::push_health`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PushHealth {
    pub generation: u64,
    pub lanes: Vec<PushLaneHealth>,
}
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PushLaneHealth {
    pub lane: PushLane,
    pub kind: PushLaneKind,
    pub state: PushLaneState,
}
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PushLaneState {
    /// No established coverage and no unrecovered failure: connecting,
    /// rebuilding after a reconfigure, or idle with nothing to cover. Never
    /// down.
    Starting,
    /// The current session established coverage (Connection), or the
    /// registration is believed live with no failure or retired unit
    /// outstanding (Registration).
    Up,
    /// The last outcome was a failure, or (Registration) a unit is still
    /// retired by a terminal failure, and no healthy progress has followed.
    Down {
        consecutive_failures: u32,
        last_error: Option<AccountError>,
    },
    /// A Connection lane stopped on a terminal session failure and waits for
    /// a push topology change or a new account generation.
    Parked { error: AccountError },
    /// This generation is retiring.
    Draining,
}
impl PushHealth {
    /// True iff some lane is `Down` or `Parked`; `Starting`, `Up` and
    /// `Draining` are not down.
    #[must_use]
    pub fn any_down(&self) -> bool {
        self.lanes.iter().any(|l| {
            matches!(
                l.state,
                PushLaneState::Down { .. } | PushLaneState::Parked { .. }
            )
        })
    }
}
