//! Driver task. Owns the `WireReader` and `ProtocolState` exclusively.
//! The public API submits commands over mpsc and awaits oneshot replies.
//!
//! The driver task is the only code that touches the wire reader and
//! protocol state directly. All command execution flows through
//! `run_one_command`, which runs the dispatch loop on decomposed
//! primitives (`WireReader`, `ProtocolState`, tag generator,
//! `DriverEventSink`).

use tokio::sync::{mpsc, oneshot, watch};
use tracing::trace;

use bifrost_types::TransmissionState;

use crate::codec::classification::{self, ClassificationContext, SolicitationRule};
use crate::codec::encode::{EncodeOptions, LiteralMode, WireCommand, encode_command};
use crate::error::Error;
use crate::types::Command;
use crate::types::response::{Capability, UntaggedResponse, UntaggedStatus};
use crate::types::validated::MailboxName;

use super::NotifyFlags;
use super::dispatch::{
    BackpressureState, Consumer, ConsumerContext, ContinuationConsumer, ContinuationReply,
    Finalized, StreamingConsumer,
};
mod events;
mod idle;
mod pipeline;
mod upgrade;
mod wire_send;

use events::{
    emit_tagged_response_code_events, emit_untagged_response_code_events,
    has_critical_response_code,
};
use idle::run_idle;
#[cfg(test)]
use pipeline::group_into_sub_batches;
use pipeline::{PipelineEnd, run_pipeline};
use upgrade::{logout_best_effort, run_upgrade};
use wire_send::{SendOutcome, send_wire_command};

pub(in crate::connection) use upgrade::run_starttls_upgrade;

/// Per-command result type for pipelined batch execution.
///
/// Each element corresponds to one command in the pipeline: `Ok` holds
/// the consumer's type-erased output, `Err` holds a per-command error
/// (e.g., server `NO` response).
pub(super) type PipelineResults = Vec<Result<Box<dyn std::any::Any + Send>, Error>>;

/// A command submitted to the driver task over `cmd_tx`.
///
/// Either a regular protocol command (with a type-erased consumer for
/// response routing) or a stream upgrade (STARTTLS / COMPRESS) that
/// the driver handles atomically without a consumer.
pub(super) enum DriverCommand {
    /// Regular command  -  the driver checks, encodes and sends it from live
    /// state when it reaches the head of the queue, and routes responses to
    /// the consumer via the classification-based dispatcher.
    Run {
        command: Command,
        consumer: DriverConsumer,
        result_tx: oneshot::Sender<Result<Box<dyn std::any::Any + Send>, Error>>,
    },
    /// Stream upgrade  -  the driver sends the protocol command, awaits
    /// the tagged OK, then atomically swaps the stream using the
    /// `Poisoned` sentinel (I9, I10). No consumer needed.
    Upgrade {
        payload: UpgradePayload,
        result_tx: oneshot::Sender<Result<Box<dyn std::any::Any + Send>, Error>>,
    },
    /// Pipelined batch  -  multiple commands submitted in one write,
    /// responses routed to each consumer by tag.
    Pipeline {
        /// Commands to encode and send as a batch.
        commands: Vec<Command>,
        /// Per-command consumers, parallel to `commands`.
        consumers: Vec<Box<dyn ConsumerErased>>,
        /// Channel for the per-command results.
        result_tx: oneshot::Sender<Result<PipelineResults, Error>>,
    },
    /// Set TCP keepalive socket options (OS-level, not IMAP protocol).
    ///
    /// Delegates to `WireReader::set_keepalive` which sets `setsockopt(2)`
    /// options on the underlying TCP socket. No data is sent on the wire.
    SetKeepalive {
        /// Keepalive configuration to apply.
        keepalive: super::TcpKeepalive,
        /// Channel for the result.
        result_tx: oneshot::Sender<Result<(), Error>>,
    },
    /// Read the peer certificate DER from the owned stream (no wire I/O).
    PeerCertificate {
        result_tx: oneshot::Sender<Option<Vec<u8>>>,
    },
    /// IDLE session  -  the driver enters IDLE mode on the wire, reads
    /// events and publishes them via the event sink, and exits when
    /// `done_rx` fires or the server terminates IDLE (RFC 2177).
    Idle {
        /// Handle signals DONE via this channel.
        done_rx: oneshot::Receiver<()>,
        /// Driver reports how IDLE ended.
        result_tx: oneshot::Sender<Result<IdleTermination, Error>>,
    },
}

/// How an IDLE session ended (RFC 2177 Section 3).
#[derive(Debug)]
pub(super) enum IdleTermination {
    /// Client sent DONE (normal exit).
    ClientDone,
    /// Server sent tagged OK (server-terminated IDLE).
    ServerTerminated,
}

/// Payload for a [`DriverCommand::Upgrade`].
///
/// Stream upgrades are handled atomically by the driver task using the
/// `Poisoned` sentinel pattern (I9, I10). The driver sends the protocol
/// command (STARTTLS / COMPRESS), awaits the tagged OK, then swaps the
/// stream without any `.await` between the buffer check and the swap.
pub(super) enum UpgradePayload {
    /// STARTTLS upgrade (RFC 3501 Section 6.2.1 / RFC 9051 Section 6.2.1).
    ///
    /// The driver sends STARTTLS, awaits OK, verifies the buffer is empty,
    /// mem-replaces the stream with `Poisoned`, performs the TLS handshake,
    /// and installs a fresh `WireReader` on the TLS stream. Capabilities
    /// are re-fetched after the upgrade.
    StartTls {
        /// TLS configuration for the handshake.
        tls_connector: native_tls::TlsConnector,
        /// Server name for TLS SNI and certificate verification.
        server_name: String,
        /// Shared with the connection handle so encryption state changes at
        /// the stream swap, even if the post-upgrade CAPABILITY fails.
        tls_active: std::sync::Arc<std::sync::atomic::AtomicBool>,
    },
    /// COMPRESS=DEFLATE upgrade (RFC 4978).
    ///
    /// The driver sends COMPRESS, awaits OK, takes remaining buffer bytes
    /// (already compressed), wraps the stream in a `CompressedStream`,
    /// and installs a fresh `WireReader`.
    Compress,
}

/// Object-safe wrapper for [`Consumer`] that erases the `Output` type.
///
/// The blanket impl below bridges any `Consumer` whose `Output` is
/// `Send + 'static` into this trait by boxing the output.
pub(super) trait ConsumerErased: Send {
    fn literal_budget(&self) -> Option<usize>;
    /// Forwards to [`Consumer::on_response`].
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    );

    /// Forwards to [`Consumer::finalize`], boxing the output.
    /// Infallible, like [`Consumer::finalize`]. Only the SUCCESS value is
    /// erased into `Any`; the error stays a typed `Error` in
    /// `Finalized::output` so the driver can still see whether it is
    /// connection-fatal.
    fn finalize_erased(
        self: Box<Self>,
        tagged: crate::types::response::TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<Box<dyn std::any::Any + Send>>;
}

/// Object-safe wrapper for streaming consumers that can await
/// downstream capacity before the driver reads more wire data.
pub(super) trait StreamingConsumerErased: ConsumerErased {
    fn backpressure_state_erased(&self) -> BackpressureState;

    fn reserve_capacity_erased(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>>;
}

impl<C: StreamingConsumer + 'static> StreamingConsumerErased for C
where
    C::Output: 'static,
{
    fn backpressure_state_erased(&self) -> BackpressureState {
        <C as StreamingConsumer>::backpressure_state(self)
    }

    fn reserve_capacity_erased(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>> {
        <C as StreamingConsumer>::reserve_capacity(self)
    }
}

/// Blanket impl that erases `C::Output` to `Box<dyn Any + Send>`.
impl<C: Consumer + 'static> ConsumerErased for C
where
    C::Output: 'static,
{
    fn literal_budget(&self) -> Option<usize> {
        <C as Consumer>::literal_budget(self)
    }

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        <C as Consumer>::on_response(self, resp, notify_snapshot, ctx);
    }

    fn finalize_erased(
        self: Box<Self>,
        tagged: crate::types::response::TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<Box<dyn std::any::Any + Send>> {
        let finalized = <C as Consumer>::finalize(self, tagged, ctx);
        Finalized {
            // Only the SUCCESS value is erased. Boxing the error too would put
            // it behind a downcast the driver cannot perform before it decides
            // whether the failure is connection-fatal.
            output: finalized
                .output
                .map(|value| Box::new(value) as Box<dyn std::any::Any + Send>),
            reclassified_as_events: finalized.reclassified_as_events,
        }
    }
}

/// Object-safe wrapper for [`ContinuationConsumer`] that extends
/// [`ConsumerErased`] with continuation handling.
///
/// Used by AUTHENTICATE and future multi-round SASL commands that
/// expect `+` continuations during the response loop.
pub(super) trait ContinuationConsumerErased: ConsumerErased {
    /// Forwards to [`ContinuationConsumer::on_continuation`].
    fn on_continuation_erased(
        &mut self,
        cont: crate::types::response::ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error>;
}

/// Blanket impl that bridges any [`ContinuationConsumer`] into
/// [`ContinuationConsumerErased`].
impl<C: ContinuationConsumer + 'static> ContinuationConsumerErased for C
where
    C::Output: 'static,
{
    fn on_continuation_erased(
        &mut self,
        cont: crate::types::response::ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        <C as ContinuationConsumer>::on_continuation(self, cont, ctx)
    }
}

/// Type-erased consumer submitted to the driver task.
///
/// Either a regular consumer (errors on unexpected `+`) or a
/// continuation-aware consumer (routes `+` to `on_continuation`).
pub(super) enum DriverConsumer {
    /// Regular consumer  -  unexpected continuations are a protocol error.
    Regular(Box<dyn ConsumerErased>),
    /// Streaming consumer  -  regular command with async pre-read
    /// backpressure.
    StreamingRegular(Box<dyn StreamingConsumerErased>),
    /// Continuation consumer  -  `+` responses are routed to the
    /// consumer's `on_continuation` handler (AUTHENTICATE, SASL).
    WithContinuations(Box<dyn ContinuationConsumerErased>),
}

impl DriverConsumer {
    fn literal_budget(&self) -> Option<usize> {
        match self {
            Self::Regular(c) => c.literal_budget(),
            Self::StreamingRegular(c) => c.literal_budget(),
            Self::WithContinuations(c) => c.literal_budget(),
        }
    }

    /// Forwards to [`ConsumerErased::on_response`].
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match self {
            Self::Regular(c) => c.on_response(resp, notify_snapshot, ctx),
            Self::StreamingRegular(c) => c.on_response(resp, notify_snapshot, ctx),
            Self::WithContinuations(c) => c.on_response(resp, notify_snapshot, ctx),
        }
    }

    /// Forwards to [`ConsumerErased::finalize_erased`].
    fn finalize_erased(
        self,
        tagged: crate::types::response::TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<Box<dyn std::any::Any + Send>> {
        match self {
            Self::Regular(c) => c.finalize_erased(tagged, ctx),
            Self::StreamingRegular(c) => c.finalize_erased(tagged, ctx),
            Self::WithContinuations(c) => c.finalize_erased(tagged, ctx),
        }
    }

    async fn prepare_to_read(&mut self) -> Result<(), Error> {
        if let Self::StreamingRegular(c) = self
            && matches!(
                c.backpressure_state_erased(),
                BackpressureState::NeedsCapacity
            )
        {
            c.reserve_capacity_erased().await?;
        }
        Ok(())
    }

    /// Handle a `+` continuation. Returns `Err` for regular consumers
    /// (unexpected continuation is a protocol error per RFC 3501 Section7.5).
    fn on_continuation(
        &mut self,
        cont: crate::types::response::ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        match self {
            Self::Regular(_) => Err(Error::Protocol(
                "unexpected continuation during command that does not expect one".into(),
            )),
            Self::StreamingRegular(_) => Err(Error::Protocol(
                "unexpected continuation during streaming command".into(),
            )),
            Self::WithContinuations(c) => c.on_continuation_erased(cont, ctx),
        }
    }
}

/// Read-only snapshot of connection state published by the driver
/// via `watch::Sender`.
///
/// Holds session state, capabilities, notify flags, selected mailbox,
/// and enabled extensions (RFC 3501 Section3, Section7.2.1; RFC 5161 Section3.2;
/// RFC 5465 Section5.1-5.8).
#[derive(Debug, Clone)]
pub(super) struct ConnectionStateSnapshot {
    /// Current session state (RFC 3501 Section3).
    pub session_state: super::SessionState,
    /// Cached server capabilities (RFC 3501 Section7.2.1).
    pub capabilities: Vec<Capability>,
    /// Successfully `ENABLE`d extensions (RFC 5161 Section3.2).
    pub enabled: Vec<String>,
    /// Whether any mailbox has been selected in this session, even if it
    /// has since been closed (RFC 5161 Section 3.1).
    pub mailbox_selected: bool,
}

impl Default for ConnectionStateSnapshot {
    fn default() -> Self {
        Self {
            session_state: super::SessionState::NotAuthenticated,
            capabilities: Vec::new(),
            enabled: Vec::new(),
            mailbox_selected: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Driver task body
// ---------------------------------------------------------------------------

/// Driver-task body. Owns the wire reader, protocol state, and tag generator.
///
/// Receives `DriverCommand`s from the mpsc, runs each command using
/// the classification-based dispatch loop, and publishes events via
/// [`DriverEventSink`](event_sink::DriverEventSink). Publishes state
/// snapshots via `watch::Sender` after every command.
///
/// The caller (typically `ImapConnection::connect`) creates the wire
/// reader and protocol state, reads the greeting, fetches initial
/// capabilities, and then passes the pre-initialized components here.
pub(super) async fn driver_task(
    mut wire_reader: super::wire::WireReader,
    mut state: super::state::ProtocolState,
    mut tag_gen: super::tag::TagGenerator,
    mut cmd_rx: mpsc::Receiver<DriverCommand>,
    state_tx: watch::Sender<ConnectionStateSnapshot>,
    mut event_sink: event_sink::DriverEventSink,
) {
    loop {
        // Opportunistic drain of pending critical events (D7).
        // Ignore CallerGone  -  the cmd_rx.recv() below will also
        // observe the caller being gone via channel close.
        let _ = event_sink.drain_pending_nonblocking();

        tokio::select! {
            biased;
            maybe_cmd = cmd_rx.recv() => {
                let Some(cmd) = maybe_cmd else { break; };
                match cmd {
                    DriverCommand::Run { command, consumer, result_tx } => {
                        // Whether a failure can have touched the wire. A
                        // refusal made before the first byte leaves the
                        // framing intact, so it must not retire the
                        // connection whatever its error variant.
                        let mut refused_before_send = false;
                        let result = match prepare_command(&state, &mut tag_gen, &command) {
                            Ok(prepared) => {
                                run_prepared_command(
                                    &mut wire_reader,
                                    &mut state,
                                    &mut event_sink,
                                    &command,
                                    prepared,
                                    consumer,
                                ).await
                            }
                            Err(refusal) => {
                                refused_before_send = true;
                                Err(refusal)
                            }
                        };
                        if !refused_before_send
                            && result.as_ref().is_err_and(Error::is_connection_fatal)
                        {
                            state.apply_infrastructure_failure();
                            cmd_rx.close();
                        }
                        publish_then_answer(
                            || {
                                let _ = state_tx.send_replace(state.snapshot());
                            },
                            result_tx,
                            result,
                        );
                    }
                    DriverCommand::Upgrade { payload, result_tx } => {
                        let result = run_upgrade(
                            &mut wire_reader,
                            &mut state,
                            &mut tag_gen,
                            &mut event_sink,
                            payload,
                        ).await;
                        if result.as_ref().is_err_and(Error::is_connection_fatal)
                            || (result.is_err()
                                && state.session_state() == super::SessionState::Logout)
                        {
                            state.apply_infrastructure_failure();
                            cmd_rx.close();
                        }
                        let result = result.map(|()| Box::new(()) as Box<dyn std::any::Any + Send>);
                        publish_then_answer(
                            || {
                                let _ = state_tx.send_replace(state.snapshot());
                            },
                            result_tx,
                            result,
                        );
                    }
                    DriverCommand::Pipeline { commands, consumers, result_tx } => {
                        let result = match run_pipeline(
                            &mut wire_reader,
                            &mut state,
                            &mut tag_gen,
                            &mut event_sink,
                            commands,
                            consumers,
                        ).await {
                            // Refused before the first byte: the framing is
                            // intact whatever the error, as for a single
                            // command refused in `prepare_command`.
                            PipelineEnd::Refused(refusal) => Err(refusal),
                            PipelineEnd::Ran { results, failure } => {
                                // A batch that ended early can leave commands
                                // on the wire whose responses nobody will
                                // read, so the connection retires whatever
                                // the failure was. The per-command slots
                                // carry it to the caller.
                                if let Some(failure) = failure {
                                    trace!(error = %failure, "driver: pipeline ended early");
                                    state.apply_infrastructure_failure();
                                    cmd_rx.close();
                                }
                                Ok(results)
                            }
                        };
                        publish_then_answer(
                            || {
                                let _ = state_tx.send_replace(state.snapshot());
                            },
                            result_tx,
                            result,
                        );
                    }
                    DriverCommand::SetKeepalive { keepalive, result_tx } => {
                        let result = wire_reader.set_keepalive(&keepalive);
                        let _ = result_tx.send(result);
                        // No protocol state changed  -  skip snapshot publish.
                        continue;
                    }
                    DriverCommand::PeerCertificate { result_tx } => {
                        let _ = result_tx.send(wire_reader.peer_certificate_der());
                        // No protocol state changed  -  skip snapshot publish.
                        continue;
                    }
                    DriverCommand::Idle { done_rx, result_tx } => {
                        let result = run_idle(
                            &mut wire_reader,
                            &mut state,
                            &mut tag_gen,
                            &mut event_sink,
                            done_rx,
                        ).await;
                        if result.as_ref().is_err_and(Error::is_connection_fatal) {
                            state.apply_infrastructure_failure();
                            cmd_rx.close();
                        }
                        publish_then_answer(
                            || {
                                let _ = state_tx.send_replace(state.snapshot());
                            },
                            result_tx,
                            result,
                        );
                    }
                }

                // RFC 3501 Section3.4: once the session reaches Logout state
                // (either via BYE or tagged OK for LOGOUT), the
                // connection is closing. Exit the driver loop so
                // cmd_rx is dropped and subsequent submit calls
                // observe DriverGone instead of hanging.
                if state.session_state() == super::SessionState::Logout {
                    break;
                }
            }
        }
    }

    // Graceful shutdown: send LOGOUT if still authenticated.
    // Best-effort; ignore errors. The drain is bounded: this runs on a
    // detached task after the last handle dropped, so nothing can abort
    // it, and an unbounded read loop against a stalled or half-open peer
    // would leak the task and its socket for the process lifetime (the
    // same hazard the IDLE DONE handshake bounds one layer up). On
    // timeout the socket is simply dropped, which is an acceptable
    // teardown for a peer that is not answering LOGOUT.
    let _ = tokio::time::timeout(
        LOGOUT_DRAIN_TIMEOUT,
        logout_best_effort(&mut wire_reader, &mut state, &mut tag_gen, &mut event_sink),
    )
    .await;
}

/// Upper bound on the terminal LOGOUT write + BYE/OK drain.
const LOGOUT_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Execute a single command through the classification-based dispatcher:
/// [`prepare_command`], then [`run_prepared_command`].
///
/// For the driver's own internal commands (connection setup's CAPABILITY, the
/// STARTTLS and COMPRESS upgrades and their post-upgrade CAPABILITY). Their
/// admission follows from state this task owns rather than from a handle-side
/// check: setup runs on the greeting's session state, and an upgrade payload
/// was admitted by its handle. The legality check in `prepare_command` still
/// runs for them, uniformly; a refusal there means that invariant broke.
pub(in crate::connection) async fn run_one_command(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    cmd: Command,
    consumer: DriverConsumer,
) -> Result<Box<dyn std::any::Any + Send>, Error> {
    let prepared = prepare_command(state, tag_gen, &cmd)?;
    run_prepared_command(wire_reader, state, event_sink, &cmd, prepared, consumer).await
}

/// A command checked and encoded against live state, not yet written.
pub(in crate::connection) struct PreparedCommand {
    pub(super) tag: String,
    pub(super) wire: WireCommand,
}

/// The refusal for a command the session may not carry, judged against LIVE
/// state at the head of the queue - or `None` when `state` permits it.
///
/// Two facts decide it: the session state (`CommandKind::legal_states`) and
/// the session's selection history (`CommandKind::refused_after_selection`).
/// The handle refused anything it could see was illegal (`InvalidState`), so a
/// refusal here means the session moved while the command waited:
/// `StateChangedBeforeSend`, which the caller may re-issue after a state
/// refresh. A session that reached Logout is gone, and the honest answer is
/// `Closed` with `Unsent` evidence. Both leave the framing intact.
pub(in crate::connection) fn live_state_refusal(
    state: &super::state::ProtocolState,
    kind: crate::types::CommandKind,
) -> Option<Error> {
    let session = state.session_state();
    let legal = kind.legal_states();
    if !legal.contains(&session) {
        if session == super::SessionState::Logout {
            return Some(Error::closed().with_attempt(TransmissionState::Unsent));
        }
        return Some(Error::StateChangedBeforeSend(format!(
            "session moved to {session:?}, out of {legal:?}, before {kind:?} could be sent"
        )));
    }
    if kind.refused_after_selection() && state.mailbox_selected() {
        return Some(Error::StateChangedBeforeSend(format!(
            "a mailbox was selected in this session before {kind:?} could be sent \
             (RFC 5161 Section 3.1)"
        )));
    }
    None
}

/// Check and encode `cmd` from the state this task owns, writing nothing and
/// arming nothing.
///
/// Every decision the wire bytes depend on is made here, at the moment the
/// command reaches the head of the queue: session legality (the
/// `CommandKind::legal_states` table), capability gates, literal markers and
/// literal8 eligibility, the mailbox encoding, and for APPEND the RFC 6855
/// wrapper and the BINARY requirement. That is what a handle-side encoding
/// lacked: decisions frozen on the handle went stale while the command waited
/// behind another, and the handle's snapshot is republished only when a
/// command COMPLETES.
///
/// Every `Err` is a refusal before the first byte, so the framing is intact
/// and the connection stays usable; the driver loop does not apply
/// `is_connection_fatal` to them. Local refusals carry no transmission
/// evidence of their own (`MissingCapability`, `InvalidInput` and their kin
/// read as `Unsent`), and `StateChangedBeforeSend` reports `Unsent` by
/// construction. No pending state effect is armed until this has succeeded,
/// so a refused SELECT or LOGIN cannot leave `in_select` / `in_auth` behind
/// for the next command's tagged OK to complete.
pub(in crate::connection) fn prepare_command(
    state: &super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
    cmd: &Command,
) -> Result<PreparedCommand, Error> {
    if let Some(refusal) = live_state_refusal(state, cmd.kind()) {
        return Err(refusal);
    }
    let tag = tag_gen.next();
    // `Command`'s Debug never prints a message body or a credential.
    trace!(tag, ?cmd, "driver: encoding IMAP command from live state");
    let wire = encode_command(&tag, cmd, &build_encode_options(state))?;
    Ok(PreparedCommand { tag, wire })
}

/// Arm the command's pending state effects, send it, and run the
/// classification-based response loop: each untagged response is classified
/// via [`classify`](crate::codec::classification::classify) and routed to the
/// consumer (solicited / ambiguous) or the event sink (unsolicited /
/// impossible). An unexpected continuation is an error unless the consumer
/// takes continuations (RFC 3501 Section 7.5).
pub(in crate::connection) async fn run_prepared_command(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    cmd: &Command,
    prepared: PreparedCommand,
    consumer: DriverConsumer,
) -> Result<Box<dyn std::any::Any + Send>, Error> {
    let PreparedCommand { tag, wire } = prepared;
    let cmd_kind = cmd.kind();
    let cmd_target: Option<MailboxName> = cmd.mailbox_target().cloned();

    // RFC 3501 Section6.1.3: tell apply_tagged to transition to Logout on
    // the tagged OK. Must be set before the dispatch loop so the
    // side-effect handler sees it when the tagged response arrives.
    if matches!(cmd, Command::Logout) {
        state.set_in_logout(true);
    }

    // RFC 3501 Section6.2.2, Section6.2.3: tell apply_tagged to transition to
    // Authenticated on the tagged OK for LOGIN or AUTHENTICATE.
    if matches!(cmd, Command::Login { .. } | Command::Authenticate { .. }) {
        state.set_in_auth(true);
    }

    // RFC 3501 Section6.3.1-Section6.3.2: tell apply_tagged to transition to
    // Selected on tagged OK, or Authenticated on tagged NO, for
    // SELECT or EXAMINE.
    if matches!(cmd, Command::Select { .. } | Command::Examine { .. }) {
        state.set_in_select(cmd_target.clone());
    }

    // RFC 3501 Section6.4.2, RFC 3691 Section3, RFC 9051 Section6.4.2: tell apply_tagged
    // to transition to Authenticated on tagged OK for CLOSE or UNSELECT.
    if matches!(cmd, Command::Close | Command::Unselect) {
        state.set_in_close(true);
    }

    // RFC 8437 Section2: tell apply_tagged to transition to NotAuthenticated
    // on tagged OK for UNAUTHENTICATE.
    if matches!(cmd, Command::Unauthenticate) {
        state.set_in_unauthenticate(true);
    }

    // RFC 5465 Section3: tell apply_tagged to update NOTIFY per-type flags
    // on tagged OK. NOTIFY SET computes flags from the registration
    // params; NOTIFY NONE resets to default (no notifications).
    if let Command::NotifySet(params) = cmd {
        let (list, status, metadata) = super::extensions::compute_notify_flags(params);
        state.set_in_notify_set(Some(super::NotifyFlags {
            list,
            status,
            metadata,
        }));
    }
    if matches!(cmd, Command::NotifyNone) {
        state.set_in_notify_set(Some(super::NotifyFlags::default()));
    }

    // Send. Exactly one command is on the wire, so the continuation wait has
    // no routing context; a refused literal comes back as that command's own
    // error. Write failures are stamped from socket progress since here.
    let baseline = wire_reader.written();
    match send_wire_command(wire_reader, state, event_sink, &wire, &tag, baseline, None).await? {
        SendOutcome::Sent => {}
        SendOutcome::Rejected => {
            // `send_wire_command` reports `Rejected` only with routing.
            return Err(Error::internal_mid_exchange(
                "literal continuation refused outside a pipeline",
                TransmissionState::Acknowledged,
            ));
        }
    }

    dispatch_response_loop(
        wire_reader,
        state,
        event_sink,
        &tag,
        cmd_kind,
        cmd_target,
        consumer,
    )
    .await
}

async fn dispatch_response_loop(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    tag: &str,
    cmd_kind: crate::types::CommandKind,
    cmd_target: Option<MailboxName>,
    mut consumer: DriverConsumer,
) -> Result<Box<dyn std::any::Any + Send>, Error> {
    // After a successful send, any transport failure is InFlight: the
    // command bytes crossed the side-effect boundary.
    let mut continuation_error = None;
    loop {
        let notify_before = state.notify();
        let utf8 = utf8_mode(state);
        consumer
            .prepare_to_read()
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let resp = wire_reader
            .read_one_limited(utf8, consumer.literal_budget())
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let digest = state.apply_side_effects(&resp);

        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => {
                // (I13): emit alert/notification overflow from
                // tagged response codes before finalization.
                emit_tagged_response_code_events(&t, event_sink);
                let cancellation_acknowledged = t.status == crate::types::response::StatusKind::No;
                let ctx = build_consumer_context(state, cmd_target.as_ref(), tag);
                // Tagged response received: the server acknowledged the
                // command. Errors from finalization (NO/BAD status) carry
                // Acknowledged so recovery can distinguish them from
                // in-flight drops.
                let finalized = consumer.finalize_erased(t, &ctx);
                // Re-emit any responses the consumer marked as events, on the
                // FAILURE path as much as the success one. That ordering is the
                // fix: finalization used to be able to return `Err` before this
                // loop ran, so a command that failed took its consumer's
                // buffered `Either` responses down with it.
                //
                // Skip those whose critical code (ALERT/NOTIFICATIONOVERFLOW)
                // was already emitted in the pre-classification pass
                //  -  re-emitting would double-deliver the alert.
                for resp in finalized.reclassified_as_events {
                    if !has_critical_response_code(&resp) {
                        let _ = event_sink.emit(resp.into());
                    }
                }
                let finalized_output = finalized.output;
                if let Some(error) = continuation_error.take() {
                    if cancellation_acknowledged {
                        // Preserve a structured auth code from the terminal
                        // NO when the consumer recognized it. Without one,
                        // the continuation error carries the more useful
                        // server-final explanation.
                        let error = if matches!(&error, Error::Auth { .. }) {
                            match finalized_output {
                                Err(completion_error @ Error::Auth { code: Some(_), .. }) => {
                                    completion_error
                                }
                                _ => error,
                            }
                        } else {
                            error
                        };
                        // A tagged NO closes a cancelled SASL exchange. Return
                        // the auth error only after consuming that completion,
                        // so the next command starts on a synchronized stream.
                        // Error::Auth has no attempt field in the current
                        // error shape, so with_attempt cannot record the NO.
                        return Err(error.with_attempt(TransmissionState::Acknowledged));
                    }
                    return Err(Error::Protocol(
                        "SASL cancellation did not end with the command's tagged NO".into(),
                    ));
                }
                return finalized_output
                    .map_err(|e| e.with_attempt(TransmissionState::Acknowledged));
            }
            crate::types::Response::Tagged(t) => {
                return Err(Error::Protocol(format!(
                    "unexpected tag {:?} (expected {:?})",
                    t.tag, tag,
                )));
            }
            crate::types::Response::Untagged(u) => {
                // (I13): emit alert/notification overflow before
                // classification so they reach the event queue even
                // when the response is routed to a consumer.
                // The command is on the wire: a BYE here is InFlight.
                let expected_logout_bye =
                    cmd_kind == crate::types::CommandKind::Logout && digest.had_bye();
                let code_emitted = process_untagged_prefix(
                    digest,
                    &u,
                    event_sink,
                    TransmissionState::InFlight,
                    Some(cmd_kind),
                )?;

                // RFC 3501 Section 6.1.3 requires LOGOUT's BYE before its
                // tagged OK. It belongs to the LOGOUT consumer and is not a
                // server-initiated disconnect in this one command path.
                if expected_logout_bye {
                    let ctx = build_consumer_context(state, cmd_target.as_ref(), tag);
                    consumer.on_response(*u, notify_before, &ctx);
                    continue;
                }

                let class_ctx = ClassificationContext {
                    notify: notify_before,
                    command_target: cmd_target.as_ref(),
                };
                let rule = classification::classify(cmd_kind, &u, &class_ctx);
                match rule {
                    SolicitationRule::OnlySolicited | SolicitationRule::Either => {
                        let ctx = build_consumer_context(state, cmd_target.as_ref(), tag);
                        consumer.on_response(*u, notify_before, &ctx);
                    }
                    SolicitationRule::OnlyUnsolicited | SolicitationRule::Impossible => {
                        // Skip event_sink.emit for responses whose
                        // critical content (ALERT / NOTIFICATIONOVERFLOW)
                        // was already emitted above  -  avoid double-emit.
                        if !code_emitted {
                            let _ = event_sink.emit((*u).into());
                        }
                    }
                }
            }
            crate::types::Response::Continuation(c) => {
                if continuation_error.is_some() {
                    return Err(Error::Protocol(
                        "server sent another SASL continuation after client cancellation".into(),
                    ));
                }
                // A regular or streaming consumer refuses a continuation as a
                // protocol error; only a continuation consumer (AUTHENTICATE)
                // answers one.
                let ctx = build_consumer_context(state, cmd_target.as_ref(), tag);
                let reply = match consumer.on_continuation(c, &ctx) {
                    Ok(reply) => reply,
                    Err(error) if error.is_connection_fatal() => {
                        return Err(error.with_attempt(TransmissionState::InFlight));
                    }
                    Err(error) => {
                        // A non-fatal SASL failure still occurs while the
                        // server is waiting for this exchange's response.
                        // Cancel it and consume the tagged NO before making
                        // the connection available to another command.
                        wire_reader
                            .write_all(b"*\r\n")
                            .await
                            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
                        continuation_error = Some(error);
                        continue;
                    }
                };
                let ContinuationReply::Write(bytes) = reply;
                // The command is already on the wire and in an exchange, so a
                // failed reply is InFlight whatever the socket counter says:
                // this is not a new command's first byte.
                wire_reader
                    .write_all(&bytes)
                    .await
                    .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol("unexpected greeting mid-command".into()));
            }
        }
    }
}

/// Publish the state snapshot before waking the command caller.
///
/// A oneshot send may schedule the caller on another runtime worker
/// immediately, so the watch snapshot must be visible first.
///
/// The ordering is a convention every completion arm must follow, not a
/// type-level guarantee: nothing stops a future arm from calling
/// `result_tx.send` directly and answering a caller that then reads a stale
/// snapshot. Making it unrepresentable (a result oneshot that cannot be
/// answered without publishing) was considered and ruled not worth its
/// machinery while every completion arm lives in the one `match cmd` in
/// `driver_task` directly above, where the deviation is visible by reading a
/// single function. Revisit if a new state-changing driver command lane is
/// added somewhere else.
fn publish_then_answer<T>(publish: impl FnOnce(), result_tx: oneshot::Sender<T>, result: T) {
    publish();
    let _ = result_tx.send(result);
}

/// Turn a side-effect digest for an untagged response into the fatal BYE
/// result every active driver read loop must honor, except command dispatch
/// for LOGOUT, whose consumer owns the required BYE.
///
/// Private on purpose: the ordering constraint below (response-code events
/// first, so an ALERT carried by BYE reaches the event queue before the
/// command exits) is only safe if every read loop applies it the same way.
/// `process_untagged_prefix` is that single application point, and keeping
/// this unexported is what stops a fifth hand-rolled copy of the pair from
/// drifting out of order again.
///
/// `phase` is how far the active command had got when the read happened.
/// An untagged BYE does not complete that command, so its outcome is unknown
/// once any of it is on the wire: every read loop today reads only after
/// writing, and passes `InFlight`. The BYE used to carry no attempt at all,
/// which reads as `Unsent` and let a non-idempotent command the server may
/// have executed be retried blind.
fn short_circuit_on_bye(
    digest: super::state::SideEffectDigest,
    response: &UntaggedResponse,
    phase: bifrost_types::TransmissionState,
    command_kind: Option<crate::types::CommandKind>,
) -> Result<(), Error> {
    if !digest.had_bye() {
        return Ok(());
    }

    let UntaggedResponse::Status { status, text, code } = response else {
        debug_assert!(false, "only an untagged BYE may set the BYE digest");
        // Raised mid-read, with the state machine already marked for BYE:
        // the connection retires.
        return Err(Error::internal_mid_exchange(
            "BYE digest without a status response",
            phase,
        ));
    };
    debug_assert!(matches!(status, UntaggedStatus::Bye));
    if command_kind == Some(crate::types::CommandKind::Logout) {
        return Ok(());
    }
    Err(Error::bye_with_code(text.clone(), code.clone()).with_attempt(phase))
}

/// The shared prologue every driver read loop runs on an untagged response:
/// emit response-code events, then fail on BYE unless regular command dispatch
/// is servicing LOGOUT.
/// Returns whether a code event was emitted, which the caller uses to decide
/// whether the response still needs a plain event of its own.
///
/// `phase` stamps the BYE error (see `short_circuit_on_bye`); each loop
/// states how far its command had got rather than leaving it unstamped.
/// `command_kind` is `Some` only for single-command dispatch; pipeline and
/// event-only loops cannot make the LOGOUT exception.
pub(super) fn process_untagged_prefix(
    digest: super::state::SideEffectDigest,
    response: &UntaggedResponse,
    event_sink: &mut event_sink::DriverEventSink,
    phase: bifrost_types::TransmissionState,
    command_kind: Option<crate::types::CommandKind>,
) -> Result<bool, Error> {
    let code_emitted = emit_untagged_response_code_events(response, event_sink);
    short_circuit_on_bye(digest, response, phase, command_kind)?;
    Ok(code_emitted)
}

/// The whole untagged arm of a read loop that has no consumer to route to:
/// run the shared prologue, then forward the response as a typed event unless
/// the prologue already published its critical code as one.
///
/// Four read loops always answer an untagged response this way - IDLE's
/// wait for its `+` grant, the IDLE loop, the post-DONE IDLE drain, and the
/// best-effort LOGOUT drain - and a fifth, the synchronizing-literal
/// continuation wait, does so only WHEN IT HAS NO ROUTING CONTEXT, i.e. for
/// single-command dispatch. Under a pipelined batch
/// that same wait does have consumers to route to - earlier commands in the
/// batch are still outstanding - and takes the router instead; answering those
/// responses here is what silently truncated a batch's results. The five
/// differ in how they terminate and in what they do with a tagged response,
/// but not here, so the "prologue, then forward iff no code event" pairing
/// lives once. Loops that DO have a consumer (command dispatch, the pipeline
/// batch, and the pipelined continuation wait) cannot use this: for them the
/// forward is conditional on classification, and they call
/// `process_untagged_prefix` directly.
pub(super) fn process_untagged_as_event(
    digest: super::state::SideEffectDigest,
    response: Box<UntaggedResponse>,
    event_sink: &mut event_sink::DriverEventSink,
    phase: bifrost_types::TransmissionState,
) -> Result<(), Error> {
    let code_emitted = process_untagged_prefix(digest, &response, event_sink, phase, None)?;
    if !code_emitted {
        let _ = event_sink.emit((*response).into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pure helpers  -  derive protocol context from ProtocolState
// ---------------------------------------------------------------------------

/// Derive whether the connection is in `IMAP4rev2` mode.
///
/// RFC 9051 Section6.3.1, via the single authority in `crate::types::profile`.
/// The rule is NOT restated here. This is the driver's view of it, and it is
/// the one that decides wire bytes - `utf8_mode`, `literal_mode` and the
/// `allow_literal8` choice all read it - so a copy that drifted here would send
/// bytes the server's active revision does not accept.
fn is_rev2(state: &super::state::ProtocolState) -> bool {
    crate::types::profile::imap4rev2_active(state.capabilities(), state.enabled())
}

/// Derive the UTF-8 wire mode from protocol state.
///
/// True when `UTF8=ACCEPT` (RFC 6855 Section3) has been enabled or `IMAP4rev2`
/// is active (RFC 9051 Section7).
fn utf8_mode(state: &super::state::ProtocolState) -> bool {
    state
        .enabled()
        .iter()
        .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"))
        || is_rev2(state)
}

/// Derive the literal negotiation mode from capabilities.
///
/// RFC 7888 Section4 (LITERAL+), Section5 (LITERAL-), RFC 3501 Section4.3 (synchronizing).
fn literal_mode(state: &super::state::ProtocolState) -> LiteralMode {
    let usable = |capability: &Capability| {
        crate::types::profile::supports(state.capabilities(), state.enabled(), capability)
    };
    // Both arms ask the authority. Its rev2 baseline folds in LITERAL- and
    // not LITERAL+ (RFC 9051 Appendix E), so pure rev2 lands on the
    // 4096-octet LITERAL- behaviour and unbounded LITERAL+ needs the token.
    if usable(&Capability::LiteralPlus) {
        LiteralMode::LiteralPlus
    } else if usable(&Capability::LiteralMinus) {
        LiteralMode::LiteralMinus
    } else {
        LiteralMode::Synchronizing
    }
}

/// Build an [`EncodeOptions`] snapshot from protocol state.
///
/// The driver builds encode options from the state it owns; there is no
/// second builder on the connection handle.
fn build_encode_options(state: &super::state::ProtocolState) -> EncodeOptions {
    EncodeOptions {
        utf8_mode: utf8_mode(state),
        literal_mode: literal_mode(state),
        capabilities: state.capabilities().to_vec(),
        enabled: state.enabled().to_vec(),
    }
}

/// Build a [`ConsumerContext`] from protocol state.
///
/// The driver constructs this inline from the state it owns; there is no
/// method on `ImapConnection` that builds one.
fn build_consumer_context<'a>(
    state: &'a super::state::ProtocolState,
    command_target: Option<&'a MailboxName>,
    command_tag: &'a str,
) -> ConsumerContext<'a> {
    ConsumerContext {
        capabilities: state.capabilities(),
        enabled: state.enabled(),
        command_target,
        command_tag,
    }
}

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

pub(super) mod event_sink;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
