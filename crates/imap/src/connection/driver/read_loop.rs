//! One read boundary for every active driver exchange.
use bifrost_types::TransmissionState;

use super::super::{NotifyFlags, state::ProtocolState, wire::WireReader};
use super::event_sink::DriverEventSink;
use crate::types::response::StatusKind;
use crate::{error::Error, types::Response};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::connection) enum Framing {
    Untouched,
    Synchronized,
    Desynchronized,
}

pub(super) struct LoopPolicy<'a> {
    /// Known tags: None means unsent, Some says whether completion ends the
    /// entire exchange. Pipelines retain sent tags after completion, but only
    /// their last outstanding command can synchronize the batch.
    pub tags: &'a [(&'a str, Option<bool>)],
    pub early_ok_tag: Option<&'a str>,
    pub command_kind: Option<crate::types::CommandKind>,
    pub route_untagged: bool,
}

pub(super) struct ReadFrame {
    pub response: Response,
    pub notify_before: NotifyFlags,
    pub code_emitted: bool,
    pub had_bye: bool,
    pub framing: Framing,
}

pub(super) struct ReadLoop;

impl ReadLoop {
    /// Cancellation before a complete frame leaves WireReader's buffered
    /// bytes in place. There is no await between consuming a frame and applying
    /// its effects, including when this future races DONE during IDLE.
    pub async fn next(
        reader: &mut WireReader,
        state: &mut ProtocolState,
        events: &mut DriverEventSink,
        literal_budget: Option<usize>,
        policy: LoopPolicy<'_>,
    ) -> Result<ReadFrame, Error> {
        state.framing = Framing::Desynchronized;
        let response = reader
            .read_one_limited(super::utf8_mode(state), literal_budget)
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let notify_before = state.notify();
        if let Response::Tagged(t) = &response {
            // Deliver critical codes even on the foreign-tag failure path.
            super::emit_tagged_response_code_events(t, events);
            let Some((_, terminal)) = policy.tags.iter().find(|(tag, _)| *tag == t.tag) else {
                // Enterprise users expect a reconnect when command ownership
                // is lost, including during IDLE, rather than stale mail on
                // a connection whose next completion cannot be trusted.
                return Err(Error::Protocol(format!(
                    "unexpected tag {:?} in active exchange",
                    t.tag
                )));
            };
            let Some(terminal) = terminal else {
                return Err(Error::Protocol(format!(
                    "tagged response for unsent pipelined command: {:?}",
                    t.tag,
                )));
            };
            if *terminal {
                state.framing = Framing::Synchronized;
            }
            if policy.early_ok_tag == Some(t.tag.as_str()) && t.status == StatusKind::Ok {
                state.disarm_effect(&t.tag);
            }
        }
        let digest = state.apply_side_effects(&response);
        let had_bye = digest.had_bye();
        let mut code_emitted = false;
        match &response {
            Response::Greeting(_) => {
                return Err(Error::Protocol(
                    "unexpected greeting in active exchange".into(),
                ));
            }
            Response::Untagged(u) => {
                code_emitted = super::process_untagged_prefix(
                    digest,
                    u,
                    events,
                    TransmissionState::InFlight,
                    policy.command_kind,
                )?;
            }
            _ => {}
        }
        if !policy.route_untagged
            && !code_emitted
            && let Response::Untagged(u) = &response
        {
            let _ = events.emit((**u).clone().into());
        }
        // Return after each event: IDLE must re-enter its biased DONE
        // select even when the peer continuously supplies buffered events.
        // There is no await between consuming a frame and returning it.
        Ok(ReadFrame {
            response,
            notify_before,
            code_emitted,
            had_bye,
            framing: state.framing,
        })
    }
}
