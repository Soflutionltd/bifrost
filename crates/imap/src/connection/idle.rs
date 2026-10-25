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

use super::driver::{DriverCommand, IdleTermination};
use super::typed_event::TypedEvent;
use super::{IdleEvent, ImapConnection};
use crate::error::Error;
use crate::types::response::{UntaggedResponse, UntaggedStatus};
use bifrost_types::TransmissionState;

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
    /// Returns [`Error::DriverGone`] if the driver task has exited,
    /// [`Error::Timeout`] if the DONE handshake does not complete within
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
        self.require_state(crate::types::CommandKind::Idle)?;

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
        permit.send(DriverCommand::Idle { reply });
        // Submitted, but not yet the driver's: only its claim at the head of
        // the queue transfers the exchange. Until then every departure
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
                            driver_death @ (Error::DriverGone { .. }
                            | Error::DriverPanicked { .. }),
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
                            // With no answer the phase is the claim's, not
                            // `next_event`'s (which stamps `Unsent` because IT
                            // sent nothing): an IDLE still queued in the dying
                            // driver's channel is withdrawn here and was never
                            // written; a claimed one may have been. A
                            // `try_take` that saw the reply dropped settled the
                            // pending, so a dead driver is not marked abandoned.
                            return match pending.try_take() {
                                Some(Ok(_)) => Ok(IdleEvent::ServerTerminated),
                                Some(Err(e)) => Err(e),
                                None => {
                                    let attempt = match pending.withdraw() {
                                        Withdrawal::Withdrawn => TransmissionState::Unsent,
                                        Withdrawal::Claimed => TransmissionState::InFlight,
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
        // driver never claimed was never written: there is nothing to end,
        // and the departure's value (an event already taken from the
        // connection-wide queue is a real server event) is returned as is.
        if pending.withdraw() == Withdrawal::Withdrawn {
            return Ok(idle_event);
        }
        // A claimed IDLE is the driver's exchange from here, and must
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
                Expired::Claimed => return Err(Error::timeout_inflight()),
                Expired::Withdrawn => {
                    debug_assert!(false, "DONE requires a claimed IDLE");
                    return Err(Error::timeout_inflight());
                }
            },
        };
        answer?;

        Ok(idle_event)
    }
}

/// Convert a [`TypedEvent`] into an [`IdleEvent`], if meaningful.
///
/// Maps the internal event representation to the public IDLE-specific
/// event type. Returns `None` for events that are handled transparently
/// by the protocol state machine and should not interrupt IDLE
/// (e.g., capability changes  -  already applied by `apply_side_effects`,
/// observable via `state_rx`).
///
/// Handles all `TypedEvent` variants, extracting IDLE-relevant data
/// from `Extension` variants where needed (e.g., `Metadata`,
/// `Search`/`Esearch`).
fn typed_event_to_idle_event(ev: TypedEvent) -> Option<IdleEvent> {
    match ev {
        TypedEvent::Exists(n) => Some(IdleEvent::Exists(n)),
        TypedEvent::Recent(n) => Some(IdleEvent::Recent(n)),
        TypedEvent::Expunge(n) => Some(IdleEvent::Expunge(n)),
        TypedEvent::FetchUpdate(f) => Some(IdleEvent::Fetch(f)),
        TypedEvent::Alert(text) => Some(IdleEvent::Alert(text)),
        TypedEvent::MailboxEvent(info) => Some(IdleEvent::MailboxEvent(info)),
        TypedEvent::MailboxStatus { mailbox, items } => {
            Some(IdleEvent::MailboxStatus { mailbox, items })
        }
        TypedEvent::Vanished { earlier, uids } => Some(IdleEvent::Vanished { earlier, uids }),
        TypedEvent::NotificationOverflow { code, text } => Some(IdleEvent::NotificationOverflow {
            code_text: code,
            resp_text: text,
        }),
        TypedEvent::CapabilityChange(_) => {
            // Capability changes during IDLE are handled transparently
            // by apply_side_effects  -  the protocol state is already
            // updated and the caller can observe it via state_rx.
            // Surfacing this as an IdleEvent would prematurely end
            // the IDLE wait (RFC 2177 Section 3  -  the client should
            // remain in IDLE until a meaningful mailbox event, timeout,
            // or cancellation occurs).
            trace!("idle: suppressing CapabilityChange (handled by state machine)");
            None
        }
        TypedEvent::Bye { code, text } => Some(IdleEvent::Bye { code, text }),
        TypedEvent::QueueOverflow {
            dropped_count,
            since: _,
        } => Some(IdleEvent::Alert(format!(
            "event queue overflow: {dropped_count} events dropped"
        ))),
        TypedEvent::MetadataChange {} | TypedEvent::ServerMetadataChange {} => {
            // Metadata changes during IDLE  -  no detailed info in
            // the TypedEvent variant. Map to a generic MetadataChange.
            // The caller can re-query metadata if needed.
            Some(IdleEvent::ExtensionEvent(
                "METADATA change during IDLE".into(),
            ))
        }
        TypedEvent::Extension(resp) => untagged_to_idle_event(*resp),
    }
}

/// Convert a raw [`UntaggedResponse`] (wrapped in `TypedEvent::Extension`)
/// into an [`IdleEvent`], if meaningful.
///
/// Returns `None` for purely informational `* OK` keepalives (no response
/// code)  -  these are server-generated heartbeats that should not interrupt
/// IDLE (RFC 3501 Section 7.1).
///
/// Handles response types that `TypedEvent::From<UntaggedResponse>`
/// routes to `Extension`  -  notably `Metadata`, `Search`, and
/// `Esearch`. The `MailboxStatus` arm remains for callers that
/// construct raw extension events directly.
fn untagged_to_idle_event(resp: UntaggedResponse) -> Option<IdleEvent> {
    match resp {
        UntaggedResponse::MailboxStatus { mailbox, items } => {
            Some(IdleEvent::MailboxStatus { mailbox, items })
        }
        UntaggedResponse::Metadata { mailbox, entries } => {
            Some(IdleEvent::MetadataChange { mailbox, entries })
        }
        UntaggedResponse::Search { uids, mod_seq: _ } => {
            // Legacy `* SEARCH` during IDLE  -  wrap in
            // EsearchResponse for the SearchUpdate variant.
            Some(IdleEvent::SearchUpdate(Box::new(
                crate::types::EsearchResponse {
                    tag: None,
                    uid: false,
                    all: uids
                        .into_iter()
                        .map(crate::types::UidRange::single)
                        .collect(),
                    min: None,
                    max: None,
                    count: None,
                    mod_seq: None,
                },
            )))
        }
        UntaggedResponse::Esearch(e) => Some(IdleEvent::SearchUpdate(Box::new(e))),
        UntaggedResponse::Status {
            status,
            code: Some(code),
            text,
        } => Some(IdleEvent::StatusUpdate { status, code, text }),
        // RFC 3501 Section 7.1: `* OK text` with no response code is
        // purely informational. Servers like Dovecot send these as
        // periodic keepalives (e.g., `* OK Still here`) to prevent
        // TCP timeouts. They carry no actionable data and should not
        // interrupt the IDLE wait.
        UntaggedResponse::Status {
            status: UntaggedStatus::Ok,
            code: None,
            text,
        } => {
            trace!("idle: suppressing informational OK keepalive: {text:?}");
            None
        }
        // Everything else: wrap as extension event.
        other => Some(IdleEvent::ExtensionEvent(format!("{other:?}"))),
    }
}

#[cfg(test)]
#[path = "idle_tests.rs"]
mod tests;
