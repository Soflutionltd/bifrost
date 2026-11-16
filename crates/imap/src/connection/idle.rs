//! IDLE command implementation (RFC 2177, RFC 5465).
//!
//! IDLE is implemented on top of the driver task's typed event queue.
//! The driver enters IDLE-reading mode where it reads wire responses and
//! publishes them via [`DriverEventSink`](super::driver::event_sink::DriverEventSink).
//! The handle-side `idle()` loops over events from
//! [`next_event`](super::ImapConnection::next_event), filtering
//! transparent protocol events (capability changes, server keepalive
//! responses), and returns the first meaningful event as an
//! [`IdleEvent`](super::IdleEvent). It then signals DONE to end the
//! IDLE session.
//!
//! # RFC references
//! - RFC 2177 Sections 2-4 (IDLE command, continuation, DONE)
//! - RFC 9051 Section 6.3.13 (IDLE in `IMAP4rev2`)
//! - RFC 5465 Sections 3-5.8 (NOTIFY event delivery during IDLE,
//!   NOTIFICATIONOVERFLOW, NOTIFY NONE)

use std::time::Duration;

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::trace;

use super::ImapConnection;
use super::driver::{DriverCommand, IdleTermination};
use super::typed_event::TypedEvent;
use crate::error::Error;
use bifrost_types::TransmissionState;

/// Outcome of an IDLE wait. Server data retains the connection's event type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum IdleEvent {
    Event(TypedEvent),
    Timeout,
    Cancelled,
    ServerTerminated,
}

impl ImapConnection {
    /// Enter IDLE mode and wait for the first server event (RFC 2177).
    ///
    /// Sends the IDLE command to the driver task, then waits for
    /// the first event, a timeout, or cancellation. On any of these,
    /// sends DONE to end the IDLE session and returns the result.
    /// Direct API counterpart to Account push streams: callers receive
    /// protocol-native IDLE events and own resync policy.
    ///
    /// # Cancellation safety
    ///
    /// Dropping the future before the driver claims IDLE withdraws it without
    /// writing it and leaves the connection reusable. Dropping after claim
    /// without a buffered answer marks it abandoned, as does a DONE
    /// handshake that times out: the driver may still be inside the IDLE
    /// exchange, so the connection is not reusable, and dropping its last
    /// handle aborts the driver. Use the `cancel` token to end IDLE and keep
    /// the connection.
    ///
    /// # Filtered events
    ///
    /// Some server responses are handled transparently and do not
    /// interrupt the IDLE wait:
    ///
    /// - **Capability changes** (`* OK [CAPABILITY ...]`)  -  already
    ///   applied by [`ProtocolState::apply_side_effects`]; observe via
    ///   `state_rx` (RFC 3501 Section 7.2.1).
    /// - **Informational keepalives** (`* OK text` with no response
    ///   code)  -  purely informational per RFC 3501 Section 7.1; servers
    ///   like Dovecot send these periodically to keep the TCP connection
    ///   alive.
    ///
    /// # Arguments
    ///
    /// * `timeout`  -  maximum time to wait for an event.
    /// * `done_timeout`  -  bound on the DONE handshake once the wait ends:
    ///   how long the driver gets to send DONE, drain, and confirm the
    ///   tagged OK. Use the same `command_timeout` every other command has.
    /// * `cancel`  -  cancellation token; firing this exits IDLE early.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::TransportFailure::DriverGone`] if the driver task has exited,
    /// [`crate::error::TransportFailure::Timeout`] if the DONE handshake does not complete within
    /// `done_timeout`, or any I/O or protocol error from the IDLE session.
    pub async fn idle(
        &self,
        timeout: Duration,
        done_timeout: Duration,
        cancel: CancellationToken,
    ) -> Result<IdleEvent, Error> {
        // RFC 2177 / RFC 9051 Section 6.3.13: IDLE is valid in Authenticated
        // and Selected states. Refused here as caller sequencing
        // (`InvalidState`); the driver re-checks at the head of its queue.
        let admitted = self.admit_command(&crate::types::Command::Idle)?;

        use super::reply::{Expired, Withdrawal, may_send, on_expiry, reply_channel};
        // Strict admission: the event deadline exists before anything is
        // submitted, and channel capacity is raced against cancellation and
        // that deadline, so a full channel cannot hold `idle()` past either.
        let deadline = tokio::time::Instant::now() + timeout;
        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(IdleEvent::Cancelled),
            () = tokio::time::sleep_until(deadline) => return Ok(IdleEvent::Timeout),
            reserved = self.cmd_tx.reserve() => match reserved {
                Ok(permit) => permit,
                Err(_) => return Err(self.observe_driver_panic(TransmissionState::Unsent).await),
            },
        };
        let cancelled = cancel.is_cancelled();
        if !may_send(Some(deadline), tokio::time::Instant::now(), cancelled) {
            return Ok(if cancelled {
                IdleEvent::Cancelled
            } else {
                IdleEvent::Timeout
            });
        }
        let (done_tx, done_rx) = oneshot::channel();
        let (reply, mut pending) = reply_channel(done_rx, &self.abandoned);
        permit.send(DriverCommand::Idle {
            reply,
            _admitted: admitted,
        });
        // Submitted, but not yet the driver's: only its commit, just before
        // the IDLE line's first byte, transfers the exchange. Until then every
        // departure
        // withdraws the IDLE (never written, DONE receiver released), and a
        // dropped future does the same through `Pending`'s drop.

        // Loop until we get a meaningful event, timeout, or
        // cancellation. Some events (e.g., CapabilityChange) are
        // handled transparently by the state machine and should not
        // interrupt IDLE  -  typed_event_to_idle_event returns None
        // for those, and we continue waiting.
        let idle_event: IdleEvent = loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break IdleEvent::Timeout;
            }

            tokio::select! {
                biased;
                // P1: Cancellation always wins.
                () = cancel.cancelled() => break IdleEvent::Cancelled,
                // P2: Event from the server (or timeout).
                maybe_ev = self.next_event(remaining) => {
                    match maybe_ev {
                        Ok(Some(ev)) => {
                            if let Some(e) = typed_event_to_idle_event(ev) {
                                break e;
                            }
                        }
                        Ok(None) => break IdleEvent::Timeout,
                        Err(
                            driver_death @ (Error::Transport { kind: crate::error::TransportFailure::DriverGone, .. }
                            | Error::Internal { kind: crate::error::InternalFailure::Panicked(_), .. }),
                        ) => {
                            // Driver exited  -  don't try to send DONE.
                            //
                            // The driver answers the IDLE command BEFORE it
                            // exits whenever it can, so by the time the event
                            // channel reads closed the real outcome (a BYE,
                            // an I/O failure) is usually already waiting in
                            // `pending`. This arm is polled first, so
                            // without this look it replaced that outcome with
                            // a generic `DriverGone`. `try_take`, not an
                            // await: nothing guarantees the answer exists.
                            //
                            // With no answer the phase is the commit's, not
                            // `next_event`'s (which stamps `Unsent` because IT
                            // sent nothing): an IDLE the dying driver never
                            // committed is withdrawn here and was never
                            // written; a committed one may have been. A
                            // `try_take` that saw the reply dropped settled the
                            // pending, so a dead driver is not marked abandoned.
                            return match pending.try_take() {
                                Some(Ok(_)) => Ok(IdleEvent::ServerTerminated),
                                Some(Err(e)) => Err(e),
                                None => {
                                    let attempt = match pending.withdraw() {
                                        Withdrawal::Withdrawn => TransmissionState::Unsent,
                                        Withdrawal::Committed => TransmissionState::InFlight,
                                    };
                                    Err(driver_death.with_attempt(attempt))
                                }
                            };
                        }
                        Err(e) => return Err(e),
                    }
                }
                // P3: Driver exited IDLE on its own (server-terminated
                // or error). Lower priority than events so that events
                // emitted before the tagged OK are delivered first.
                idle_result = pending.recv() => {
                    return match idle_result {
                        Ok(Ok(IdleTermination::ServerTerminated)) => {
                            Ok(IdleEvent::ServerTerminated)
                        }
                        Ok(Ok(IdleTermination::ClientDone)) => {
                            // Shouldn't happen  -  we haven't sent DONE.
                            // Treat as server-terminated.
                            Ok(IdleEvent::ServerTerminated)
                        }
                        Ok(Err(e)) => {
                            Err(e)
                        }
                        Err(phase) => Err(self.observe_driver_panic(phase.attempt()).await),
                    };
                }
            }
        };

        // Every departure that would send DONE withdraws first. An IDLE the
        // driver never committed was never written: there is nothing to end,
        // and the departure's value (an event already taken from the
        // connection-wide queue is a real server event) is returned as is.
        if pending.withdraw() == Withdrawal::Withdrawn {
            return Ok(idle_event);
        }
        // A committed IDLE is the driver's exchange from here, and must
        // complete DONE to preserve framing.
        let _ = done_tx.send(());
        trace!("idle handle: sent DONE signal");

        // Wait for the driver to send DONE on the wire, drain remaining
        // responses, and confirm the tagged OK - under the same bound every
        // other command carries. The driver's drain is a bare read loop, so
        // a peer whose TCP is alive but which never answers the tagged OK
        // would otherwise park this future forever, past cancellation, past
        // account shutdown, and past `close()` (the IDLE connection is
        // dialed outside the pool, so the pool's drain never sees it).
        let answer = match tokio::time::timeout(done_timeout, pending.recv()).await {
            Ok(Ok(answer)) => answer,
            Ok(Err(phase)) => return Err(self.observe_driver_panic(phase.attempt()).await),
            Err(_) => match on_expiry(&mut pending) {
                Expired::Answer(answer) => answer,
                Expired::Committed => return Err(Error::timeout_inflight()),
                Expired::Withdrawn => {
                    debug_assert!(false, "DONE requires a committed IDLE");
                    return Err(Error::timeout_inflight());
                }
            },
        };
        answer?;

        Ok(idle_event)
    }
}

/// IDLE wraps the original event, without normalizing SEARCH or discarding
/// extension fields. Sync meaning belongs to the account push adapter.
fn typed_event_to_idle_event(event: TypedEvent) -> Option<IdleEvent> {
    event.ends_idle_wait().then_some(IdleEvent::Event(event))
}

#[cfg(test)]
#[path = "idle_tests.rs"]
mod tests;
