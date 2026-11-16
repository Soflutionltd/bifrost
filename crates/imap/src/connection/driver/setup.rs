//! Bootstrap is the first phase of the socket-owning driver task.

use std::sync::{Arc, atomic::AtomicBool};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};

use crate::error::Error;
use crate::types::{Command, Response};

use super::super::{SessionState, TlsMode, lifecycle, reply::CommitGate, state, tag, wire};
use super::{ConnectionStateSnapshot, DriverCommand, event_sink::DriverEventSink};

pub(in crate::connection) struct Setup {
    pub(in crate::connection) host: String,
    pub(in crate::connection) tls_mode: TlsMode,
    pub(in crate::connection) tls_connector: native_tls::TlsConnector,
    pub(in crate::connection) timeout: Duration,
    pub(in crate::connection) tls_active: Arc<AtomicBool>,
}

pub(in crate::connection) async fn setup_then_drive(
    mut reader: wire::WireReader,
    setup: Setup,
    cmd_rx: mpsc::Receiver<DriverCommand>,
    state_tx: watch::Sender<ConnectionStateSnapshot>,
    mut events: DriverEventSink,
    ready: oneshot::Sender<Result<(), Error>>,
) {
    let mut state = state::ProtocolState::new();
    state.set_tls_active(setup.tls_mode.uses_implicit_tls());
    let mut tags = tag::TagGenerator::new();
    // One absolute deadline and one cumulative peer allowance span greeting,
    // CAPABILITY, STARTTLS and its refresh. Outlook users expect a stalled dial
    // to return for retry, with evidence of any setup command already written.
    reader.set_peer_timeout(Some(setup.timeout));
    let baseline = reader.written();
    let result = tokio::time::timeout(
        setup.timeout,
        initialize(&mut reader, &mut state, &mut tags, &mut events, setup),
    )
    .await
    .unwrap_or_else(|_| {
        Err(Error::timeout()
            .with_attempt(super::wire_send::write_failure_evidence(&reader, baseline)))
    });
    if let Err(error) = result {
        let error = if error.attempt().is_none() {
            error.with_attempt(super::wire_send::write_failure_evidence(&reader, baseline))
        } else {
            error
        };
        // Return ends this task and destroys the reader before DriverScope
        // releases retained leases. A failed setup never enters LOGOUT drain.
        let _ = ready.send(Err(error));
        return;
    }
    reader.set_peer_timeout(None);
    state_tx.send_replace(state.snapshot());
    if ready.send(Ok(())).is_err() {
        return;
    }
    super::driver_task(reader, state, tags, cmd_rx, state_tx, events).await;
}

async fn initialize(
    reader: &mut wire::WireReader,
    state: &mut state::ProtocolState,
    tags: &mut tag::TagGenerator,
    events: &mut DriverEventSink,
    setup: Setup,
) -> Result<(), Error> {
    let greeting = reader
        .read_greeting()
        .await
        .map_err(|error| error.with_attempt(bifrost_types::TransmissionState::Unsent))?;
    let Response::Greeting(greeting) = greeting else {
        return Err(Error::Provider(crate::error::ProviderFailure::Protocol(
            "expected greeting from server (RFC 3501 Section 7.1)".into(),
        )));
    };
    if let Some(alert) = state.apply_greeting(&greeting)? {
        let _ = events.emit(super::super::typed_event::TypedEvent::Alert(alert));
    }
    if setup.tls_mode.uses_starttls() {
        ensure_starttls_session(state)?;
    }
    if state.capabilities().is_empty() {
        super::upgrade::required_capability_refresh(reader, state, tags, events).await?;
    }
    if setup.tls_mode.uses_starttls() {
        ensure_starttls_available(state)?;
        lifecycle::validate_tls_server_name(&setup.host)?;
        super::upgrade::run_starttls_upgrade(
            reader,
            state,
            tags,
            events,
            setup.tls_connector,
            setup.host,
            setup.tls_active,
            &CommitGate::committed(),
        )
        .await?;
    }
    Ok(())
}

fn ensure_starttls_available(state: &state::ProtocolState) -> Result<(), Error> {
    super::super::admission::admit(
        &Command::StartTls,
        &super::super::admission::SessionView {
            snapshot: &state.snapshot(),
            tls_active: state.tls_active(),
        },
    )
}

fn ensure_starttls_session(state: &state::ProtocolState) -> Result<(), Error> {
    // RFC 9051: a cleartext PREAUTH must close before even CAPABILITY fallback.
    if state.session_state() != SessionState::NotAuthenticated {
        return Err(Error::LocalRefusal(
            crate::error::LocalRefusal::StartTlsUnavailable,
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "setup_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "setup_policy_tests.rs"]
mod policy_tests;
