//! IMAP client connection.
//!
//! `ImapConnection` is a plain struct (no typestate generics). Callers manage
//! session lifecycle themselves.
//!
//! Connection and authentication are defined in RFC 3501 Sections 6.1-6.2 /
//! RFC 9051 Sections 6.1-6.2.

// This is the account transport. Unused production connection surface is a bug.
// Test-only command adapters preserve hermetic protocol transcripts without
// retaining a second production API beside the account.
#![warn(dead_code)]

use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use lifecycle::validate_tls_server_name;
use tracing::{debug, warn};

use crate::error::Error;
use crate::types::{
    AppendMessage, Capability, Command, CopyResult, ExpungeResult, FetchAttr, FetchResponse, Flag,
    MailboxInfo, MailboxName, MoveResult, NamespaceResponse, NotifySetParams,
    QuotaRootResponse, ResponseCode, SelectOptions, SelectedMailbox, SequenceSet, StatusItem,
    StatusResult, StoreOperation, StoreResult, ThreadNode,
};

#[cfg(test)]
use crate::types::{
    AclEntry, EsearchResponse, ListRightsResponse, MailboxAttribute, MetadataResult, QresyncParams,
    QuotaResource, Response, UidRange, UntaggedResponse, UntaggedStatus,
};

pub(crate) const LOGOUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

mod admission;
mod append;
mod auth;
mod config;
pub(super) mod dispatch;
pub(super) mod driver;
mod ergonomics;
mod extensions;
mod helpers;
mod idle;
mod lifecycle;
mod mailbox;
pub(super) mod pipeline;
mod reply;
mod request;
mod search_validation;
// Sequence-number command adapters are only needed by protocol transcripts.
#[cfg(test)]
mod seq_ops;
mod sort_thread;
pub(super) mod state;
mod stream;
mod tag;
pub(crate) mod typed_event;
mod uid_ops;
pub(super) mod wire;

// ImapConfig is re-exported for account construction.
pub(crate) use crate::types::{SearchResult, SessionState};
pub use config::ImapConfig;
pub(crate) use config::{TcpKeepalive, TlsMode};
#[cfg(test)]
use dispatch::{
    expand_uid_ranges, filter_store_flags, is_notify_list_event, is_notify_selection_mismatch,
};
use driver::task::InFlightGuard;
pub(crate) use driver::task::{DriverTask, LeaseHandoff};
pub(crate) use idle::IdleEvent;
use stream::ImapStream;
#[cfg(test)]
use stream::{CompressedStream, InnerStream};

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

// ---------------------------------------------------------------------------
// ImapConnection
// ---------------------------------------------------------------------------

/// An IMAP client connection (RFC 3501 Section 2 / RFC 9051 Section 2).
///
/// Manages a single TCP (or TLS) connection to an IMAP server. All operations
/// are async and require a caller-supplied timeout  -  there are no hardcoded
/// defaults and no infinite waits.
///
/// # Connection state
///
/// RFC 3501 Section 3 defines four session states: Not Authenticated,
/// Authenticated, Selected, and Logout. Each command method validates that
/// the connection is in an allowed state before sending, returning
/// [`crate::error::LocalRefusal::InvalidState`] if not ([`crate::error::TransportFailure::Closed`] once the session is in
/// Logout). For example, [`uid_fetch()`](Self::uid_fetch)
/// requires the Selected state and will fail if called before
/// [`select()`](Self::select).
pub(crate) struct ImapConnection {
    /// Channel for submitting commands to the driver task.
    cmd_tx: tokio::sync::mpsc::Sender<driver::DriverCommand>,
    /// Watch receiver for observing connection state snapshots.
    state_rx: tokio::sync::watch::Receiver<driver::ConnectionStateSnapshot>,
    /// Receiver for asynchronous server events (ALERTs, EXISTS, etc.).
    /// Wrapped in `Mutex` so event consumption does not require `&mut self`.
    events_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<typed_event::TypedEvent>>,
    /// Shared owner of the driver task. The pool retains this separately
    /// from the weak connection reference so account close can join a
    /// healthy driver's detached graceful shutdown.
    driver_handle: Arc<DriverTask>,
    /// Server hostname, retained for STARTTLS upgrade (RFC 3501 Section6.2.1).
    ///
    /// Needed to construct the `ServerName` for TLS SNI and certificate
    /// verification when the caller invokes `starttls()`.
    #[cfg(test)]
    host: String,
    /// Whether the current transport is encrypted.
    tls_active: Arc<std::sync::atomic::AtomicBool>,
    /// Set when a claimed command's caller leaves without an answer.
    /// The driver is still executing that command, so the next
    /// command submitted on this connection queues behind it and is very
    /// likely to time out in turn. The flag makes the checkout unreusable
    /// so the cascade stops at one connection.
    abandoned: std::sync::atomic::AtomicBool,
}

impl ImapConnection {
    /// Stop the driver task and drop its transport immediately.
    pub(crate) async fn terminate(&self) {
        self.driver_handle.abort_and_join().await;
    }

    pub(crate) fn driver_task(&self) -> Arc<DriverTask> {
        Arc::clone(&self.driver_handle)
    }
}

/// The last handle of an ABANDONED connection aborts its driver.
///
/// An abandoned connection's driver is still inside an exchange whose caller
/// is gone (a timeout fired, or the future was dropped). Dropping the handle
/// closes the command channel, but a driver blocked inside that exchange never
/// returns to its loop to see it, so against a stalled peer it - and its
/// socket - would live for the rest of the process, unreachable by `close()`
/// once no strong handle remains. At last drop nobody can consume the result
/// or reuse the session, so finishing the exchange buys nothing; aborting it
/// does not change the transmission evidence the caller already received.
///
/// A connection that is not abandoned keeps the graceful path: the closed
/// channel lets the driver run its bounded best-effort LOGOUT. `abort` only
/// requests cancellation; `terminate().await` remains the guarantee that the
/// transport is gone, which `Account::close()` relies on.
impl Drop for ImapConnection {
    fn drop(&mut self) {
        if *self.abandoned.get_mut() {
            self.driver_handle.abort();
        }
    }
}

pub(crate) use crate::types::notify::NotifyFlags;

// Compile-time proof that ImapConnection is Send  -  required for
// holding the connection across `.await` points in async tasks.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<ImapConnection>();
};

impl ImapConnection {
    /// Whether the driver command channel still has a receiver.
    ///
    /// Session state only describes the IMAP protocol state. A connection
    /// can remain Selected after its socket or driver task has died, so
    /// pool reuse must consult the command-channel liveness separately.
    pub(crate) fn is_alive(&self) -> bool {
        !self.cmd_tx.is_closed()
    }

    /// Inbound IMAP protocol octets delivered by the reader after TLS and
    /// COMPRESS decoding, including the connection setup exchange.
    pub(crate) fn bytes_in(&self) -> u64 {
        self.driver_handle.bytes_in()
    }

    /// Whether this connection may be returned to the pool.
    ///
    /// Liveness is necessary but not sufficient: a connection whose caller
    /// walked away from an in-flight command still has the driver busy on
    /// the wire, and parking it hands the next caller a queue it will
    /// almost certainly time out behind.
    pub(crate) fn is_reusable(&self) -> bool {
        self.is_alive() && !self.abandoned.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Guard cancellation during non-wire keepalive setup.
    fn in_flight(&self) -> InFlightGuard<'_> {
        InFlightGuard {
            flag: &self.abandoned,
            completed: false,
        }
    }

    /// Drain all pending events from the typed event queue.
    ///
    /// Returns every [`TypedEvent`] that has accumulated since the last
    /// call to `drain_events` or `next_event`. Non-blocking  -  returns an
    /// empty `Vec` when no events are pending.
    ///
    /// This is the primary way to observe asynchronous server data
    /// (ALERTs, EXISTS/EXPUNGE, NOTIFY events, BYE, etc.) outside of an
    /// active command or IDLE session.
    #[cfg(test)]
    pub async fn drain_events(&self) -> Vec<typed_event::TypedEvent> {
        let mut rx = self.events_rx.lock().await;
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    /// Wait for the next event from the typed event queue, with a
    /// timeout.
    ///
    /// Returns `Ok(Some(event))` when an event arrives, `Ok(None)` when
    /// `timeout` elapses without an event, or `Err(crate::error::TransportFailure::DriverGone)` /
    /// `Err(crate::error::InternalFailure::Panicked)` when the driver task has exited (channel
    /// closed).
    ///
    /// The driver death is stamped `Unsent`: this method submits no command,
    /// so nothing of ITS OWN can have reached the wire. A caller that owns a
    /// command the driver was executing (IDLE) knows a later phase and must
    /// restamp the error with it.
    ///
    /// RFC 3501 Section5.3: servers may send untagged data at any time. This
    /// method surfaces that data as [`TypedEvent`]s, enabling callers to
    /// react to mailbox state changes, ALERTs, and NOTIFY events.
    pub async fn next_event(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<typed_event::TypedEvent>, crate::error::Error> {
        let received = {
            let mut rx = self.events_rx.lock().await;
            tokio::time::timeout(timeout, rx.recv()).await
        };
        match received {
            Ok(Some(ev)) => Ok(Some(ev)),
            Ok(None) => Err(self
                .observe_driver_panic(bifrost_types::TransmissionState::Unsent)
                .await),
            Err(_) => Ok(None), // timeout
        }
    }
}

impl std::fmt::Debug for ImapConnection {
    /// Prints connection metadata useful for logging and diagnostics,
    /// without exposing internal stream state or buffers.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.state_rx.borrow();
        f.debug_struct("ImapConnection")
            .field("state", &snapshot.session_state)
            .field("capabilities_count", &snapshot.capabilities.len())
            .field("encrypted", &self.is_encrypted())
            .field("cmd_tx_closed", &self.cmd_tx.is_closed())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
use dispatch::{build_selected_mailbox, selected_mailbox_effective_responses};

#[cfg(test)]
pub(crate) fn admit_for_encoding(
    command: &Command,
    opts: &crate::codec::encode::EncodeOptions,
) -> Result<(), Error> {
    let mut capabilities = opts.capabilities.clone();
    capabilities.retain(|cap| !matches!(cap, Capability::LiteralPlus | Capability::LiteralMinus));
    match opts.literal_mode {
        crate::codec::encode::LiteralMode::LiteralPlus => {
            capabilities.push(Capability::LiteralPlus);
        }
        crate::codec::encode::LiteralMode::LiteralMinus => {
            capabilities.push(Capability::LiteralMinus);
        }
        crate::codec::encode::LiteralMode::Synchronizing => {}
    }
    let snapshot = driver::ConnectionStateSnapshot {
        session_state: command.kind().legal_states()[0],
        session: std::sync::Arc::new(crate::types::profile::SessionCapabilities {
            capabilities,
            enabled: opts.enabled.clone(),
        }),
        mailbox_selected: false,
        selected: None,
    };
    admission::admit(
        command,
        &admission::SessionView {
            snapshot: &snapshot,
            tls_active: false,
        },
    )
}
