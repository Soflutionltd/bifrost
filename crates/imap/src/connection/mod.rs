//! IMAP client connection.
//!
//! `ImapConnection` is a plain struct (no typestate generics). Callers manage
//! session lifecycle themselves.
//!
//! Connection and authentication are defined in RFC 3501 Sections 6.1-6.2 /
//! RFC 9051 Sections 6.1-6.2.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use crate::error::Error;
use crate::types::{
    AclEntry, AppendMessage, Capability, Command, CopyResult, EsearchResponse, ExpungeResult,
    FetchAttr, FetchResponse, Flag, ListRightsResponse, MailboxAttribute, MailboxFilter,
    MailboxInfo, MailboxName, MetadataResult, MoveResult, NamespaceResponse, NotifySetParams,
    QresyncParams, QuotaResource, QuotaRootResponse, Response, ResponseCode, SelectOptions,
    SelectedMailbox, SequenceSet, StatusItem, StatusResult, StoreOperation, StoreResult,
    ThreadNode, UidRange,
};

#[cfg(test)]
use crate::types::{UntaggedResponse, UntaggedStatus};

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
pub(crate) mod literals;
mod mailbox;
pub(super) mod pipeline;
mod reply;
mod search_validation;
mod seq_ops;
mod sort_thread;
pub(super) mod state;
mod stream;
mod tag;
pub(crate) mod typed_event;
use typed_event::TypedEvent;
mod uid_ops;
pub(super) mod wire;

// pub: ImapConfig is re-exported at the crate root for direct connections.
pub use config::ImapConfig;
use stream::{CompressedStream, ImapStream, InnerStream};

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

/// TLS policy for an IMAP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TlsMode {
    /// Connect directly over TLS.
    Implicit,
    /// Connect in cleartext, then upgrade via STARTTLS.
    StartTls,
    /// Connect without TLS.
    None,
}

impl TlsMode {
    fn uses_implicit_tls(self) -> bool {
        matches!(self, Self::Implicit)
    }

    fn uses_starttls(self) -> bool {
        matches!(self, Self::StartTls)
    }
}

/// TCP keepalive configuration for the underlying socket.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TcpKeepalive {
    /// Time before the first keepalive probe.
    pub(crate) time: Duration,
    /// Interval between subsequent probes.
    pub(crate) interval: Duration,
}

impl TcpKeepalive {
    /// Create a TCP keepalive configuration with the given time and interval.
    pub(crate) fn new(time: Duration, interval: Duration) -> Self {
        Self { time, interval }
    }
}

/// IMAP session state (RFC 3501 Section 3 / RFC 9051 Section 3).
///
/// Tracks the current protocol state of the connection. State transitions
/// are managed automatically by `ImapConnection` methods.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SessionState {
    /// Not Authenticated  -  client must authenticate (RFC 3501 Section 3.1).
    NotAuthenticated,
    /// Authenticated  -  client may select a mailbox (RFC 3501 Section 3.2).
    Authenticated,
    /// Selected  -  a mailbox is open (RFC 3501 Section 3.3).
    Selected,
    /// Logout  -  connection is being closed (RFC 3501 Section 3.4).
    Logout,
}

/// Outcome of an IDLE wait. Server data retains the connection's event type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum IdleEvent {
    Event(TypedEvent),
    Timeout,
    Cancelled,
    ServerTerminated,
}

/// Result of a SEARCH or UID SEARCH command.
///
/// Contains both the matching sequence numbers/UIDs and the optional
/// highest mod-sequence value (RFC 7162 Section 3.1.5).
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub(crate) struct SearchResult {
    /// Matching message sequence numbers (SEARCH) or UIDs (UID SEARCH).
    pub(crate) ids: Vec<u32>,
    /// Highest mod-sequence of matching messages, if MODSEQ was used
    /// in the search criteria (RFC 7162 Section 3.1.5).
    pub(crate) mod_seq: Option<u64>,
}

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
/// [`Error::InvalidState`] if not ([`Error::Closed`] once the session is in
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

/// Values whose lifetime is tied to a driver task's transport (the pool's
/// session permit, a push slot): released only once the driver future, and
/// with it the socket, has been destroyed. `retain` and `mark_exited` are
/// linearized by `state`, and taken values are dropped outside the lock.
pub(crate) struct DriverExit {
    state: std::sync::Mutex<ExitState>,
}

enum ExitState {
    Running(Vec<Box<dyn Send>>),
    Exited,
}

impl DriverExit {
    fn running(values: Vec<Box<dyn Send>>) -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(ExitState::Running(values)),
        })
    }

    fn exited() -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(ExitState::Exited),
        })
    }

    fn retain(&self, value: Box<dyn Send>) {
        let mut state = self.state.lock().expect("driver exit lock poisoned");
        if let ExitState::Running(values) = &mut *state {
            values.push(value);
            return;
        }
        drop(state);
        drop(value);
    }

    fn mark_exited(&self) {
        let previous = {
            let mut state = self.state.lock().expect("driver exit lock poisoned");
            std::mem::replace(&mut *state, ExitState::Exited)
        };
        drop(previous);
    }

    #[cfg(test)]
    fn has_exited(&self) -> bool {
        matches!(
            *self.state.lock().expect("driver exit lock poisoned"),
            ExitState::Exited
        )
    }
}

/// The future actually spawned for a driver. `inner` is destroyed BEFORE the
/// exit is published, on completion (inside `poll`) and in `Drop` (abort,
/// panic unwinding through tokio, runtime teardown). Built outside any async
/// block, so an abort before the first poll still runs the `Drop` path.
struct DriverScope<F> {
    inner: Option<std::pin::Pin<Box<F>>>,
    exit: Arc<DriverExit>,
}

impl<F: std::future::Future<Output = ()>> std::future::Future for DriverScope<F> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let this = self.get_mut();
        let result = this
            .inner
            .as_mut()
            .expect("driver present")
            .as_mut()
            .poll(cx);
        if result.is_ready() {
            this.inner = None;
            this.exit.mark_exited();
        }
        result
    }
}

impl<F> Drop for DriverScope<F> {
    fn drop(&mut self) {
        self.inner = None;
        self.exit.mark_exited();
    }
}

/// Hand-off cell for values that must be owned by a driver from the instant
/// it is spawned. The pool's dial task keeps one clone until its connector
/// future has been destroyed; the connector takes the values at spawn time
/// with no await in between.
#[derive(Clone)]
pub(crate) struct LeaseHandoff(Arc<std::sync::Mutex<Vec<Box<dyn Send>>>>);

impl LeaseHandoff {
    pub(crate) fn new(values: Vec<Box<dyn Send>>) -> Self {
        Self(Arc::new(std::sync::Mutex::new(values)))
    }

    pub(crate) fn none() -> Self {
        Self::new(Vec::new())
    }

    pub(crate) fn take(&self) -> Vec<Box<dyn Send>> {
        std::mem::take(&mut *self.0.lock().expect("lease handoff lock poisoned"))
    }
}

pub(crate) struct DriverTask {
    exit: Arc<DriverExit>,
    pub(crate) handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    abort_handle: Option<tokio::task::AbortHandle>,
    inbound_bytes: Arc<std::sync::atomic::AtomicU64>,
}

impl DriverTask {
    /// Spawn `driver` under a `DriverScope` that owns `retained` until the
    /// driver future has been destroyed. The join handle is still kept:
    /// close joins the task, and the exit slot proves transport destruction
    /// without replacing that join.
    pub(crate) fn spawn<F>(
        driver: F,
        inbound_bytes: Arc<std::sync::atomic::AtomicU64>,
        retained: Vec<Box<dyn Send>>,
    ) -> Self
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let exit = DriverExit::running(retained);
        let scope = DriverScope {
            inner: Some(Box::pin(driver)),
            exit: Arc::clone(&exit),
        };
        let handle = tokio::spawn(scope);
        Self {
            abort_handle: Some(handle.abort_handle()),
            handle: tokio::sync::Mutex::new(Some(handle)),
            inbound_bytes,
            exit,
        }
    }

    /// For fakes with no driver: starts exited, so retained values drop at once.
    pub(crate) fn absent() -> Self {
        Self {
            handle: tokio::sync::Mutex::new(None),
            abort_handle: None,
            inbound_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            exit: DriverExit::exited(),
        }
    }

    /// Keep `value` until the driver has exited, or drop it now if it has.
    pub(crate) fn retain_until_exit(&self, value: Box<dyn Send>) {
        self.exit.retain(value);
    }

    #[cfg(test)]
    pub(crate) fn has_exited(&self) -> bool {
        self.exit.has_exited()
    }

    fn bytes_in(&self) -> u64 {
        self.inbound_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn abort(&self) {
        if let Some(handle) = &self.abort_handle {
            handle.abort();
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.handle
            .try_lock()
            .map(|handle| {
                handle
                    .as_ref()
                    .is_none_or(tokio::task::JoinHandle::is_finished)
            })
            .unwrap_or(false)
    }

    /// Wait for graceful driver shutdown, aborting and joining it if the
    /// caller's bound expires.
    pub(crate) async fn finish_or_abort(&self, timeout: Duration) {
        let mut handle = self.handle.lock().await;
        let Some(task) = handle.as_mut() else {
            return;
        };
        if tokio::time::timeout(timeout, &mut *task).await.is_err() {
            task.abort();
            let _ = task.await;
        }
        handle.take();
    }

    pub(crate) async fn abort_and_join(&self) {
        let mut handle = self.handle.lock().await;
        let Some(task) = handle.as_mut() else {
            return;
        };
        task.abort();
        let _ = task.await;
        handle.take();
    }
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

/// Guard around keepalive setup, which submits no wire command. Marks the
/// connection abandoned unless the driver result was received and
/// [`completed`](Self::completed) was called.
pub(super) struct InFlightGuard<'a> {
    flag: &'a std::sync::atomic::AtomicBool,
    completed: bool,
}

impl InFlightGuard<'_> {
    pub(super) fn completed(mut self) {
        self.completed = true;
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.flag.store(true, std::sync::atomic::Ordering::Release);
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
    pub(super) fn in_flight(&self) -> InFlightGuard<'_> {
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
    /// `timeout` elapses without an event, or `Err(Error::DriverGone)` /
    /// `Err(Error::DriverPanicked)` when the driver task has exited (channel
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

/// Build the default native-tls connector.
fn build_default_tls_connector() -> Result<native_tls::TlsConnector, Error> {
    native_tls::TlsConnector::new().map_err(|e| Error::Io {
        source: Arc::new(std::io::Error::other(e)),
        attempt: None,
    })
}

fn validate_tls_server_name(host: &str) -> Result<(), Error> {
    if host.is_empty() {
        return Err(Error::InvalidInput(
            "TLS server name must not be empty".into(),
        ));
    }
    if host.bytes().any(|b| b == 0 || b.is_ascii_whitespace()) {
        return Err(Error::InvalidInput(
            format!("invalid TLS server name: {host:?}").into(),
        ));
    }
    Ok(())
}

/// Filter flags not valid in STORE flag lists (RFC 3501 Section 2.3.2).
///
/// `\Recent` is server-only and `\*` (wildcard) is not valid in STORE.
fn filter_store_flags(flags: &[Flag]) -> Vec<Flag> {
    flags.iter().filter(|f| f.is_writable()).cloned().collect()
}

/// Expand a slice of [`UidRange`] into individual UIDs.
///
/// Used for backward-compatible conversion of ESEARCH ALL uid-set results
/// into a flat `Vec<u32>` matching the legacy SEARCH response format.
///
/// Expansion is capped at [`MAX_EXPANDED_UIDS`] to prevent OOM when a
/// server returns extremely large ranges (e.g. `1:4294967295`).
///
/// `*` is special wherever it appears in a sequence-set, not only as a range
/// endpoint (RFC 9051 Section 9:
/// `seq-number = nz-number / "*"`).  It means "the highest UID in the mailbox",
/// a value unknown from the ESEARCH response alone, so any set naming it
/// cannot be expanded at all.  Rather than return a partial result as if it
/// were complete, this reports the incomplete expansion to the caller.
///
/// A sequence-set may legally repeat or overlap its elements, so the ranges
/// are sorted and merged before both the cap check and the expansion: the
/// result is ascending and duplicate-free, and `1:600000,1:600000` names
/// 600000 UIDs, not 1200000.
fn expand_uid_ranges(ranges: &[UidRange]) -> Result<Vec<u32>, Error> {
    /// Safety cap to prevent OOM from malicious or buggy server responses.
    const MAX_EXPANDED_UIDS: usize = 1_000_000;

    let mut saw_star = false;
    let mut bounds: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
    for range in ranges {
        let crate::types::SeqBound::Number(start) = range.start_bound() else {
            saw_star = true;
            continue;
        };
        let end = match range.end_bound() {
            Some(crate::types::SeqBound::Number(end)) => end,
            Some(crate::types::SeqBound::Star) => {
                saw_star = true;
                continue;
            }
            None => start,
        };
        // The decoder normalizes descending ranges (Postel), so a range that
        // still arrives inverted is not a legal sequence-set element and
        // names no messages.
        if end >= range.start {
            bounds.push((range.start, end));
        }
    }

    bounds.sort_unstable();
    let mut merged: Vec<(u32, u32)> = Vec::with_capacity(bounds.len());
    for (start, end) in bounds {
        match merged.last_mut() {
            // Adjacent ranges merge too: it changes no member of the set and
            // keeps the count exact.
            Some(last) if start <= last.1.saturating_add(1) => last.1 = last.1.max(end),
            _ => merged.push((start, end)),
        }
    }

    let requested: u64 = merged
        .iter()
        .map(|&(start, end)| u64::from(end) - u64::from(start) + 1)
        .sum();

    if saw_star {
        return Err(Error::SearchResultTruncated {
            returned: usize::try_from(requested).unwrap_or(usize::MAX),
            omitted: None,
        });
    }

    let max_expanded = u64::try_from(MAX_EXPANDED_UIDS).unwrap_or(u64::MAX);
    if requested > max_expanded {
        warn!(
            requested,
            "UID ranges exceed the {MAX_EXPANDED_UIDS}-UID expansion limit"
        );
        return Err(Error::SearchResultTruncated {
            returned: MAX_EXPANDED_UIDS,
            omitted: Some(requested.saturating_sub(max_expanded)),
        });
    }

    let mut uids = Vec::with_capacity(usize::try_from(requested).unwrap_or(MAX_EXPANDED_UIDS));
    for (start, end) in merged {
        uids.extend(start..=end);
    }
    Ok(uids)
}

/// Check whether a LIST response's attributes conflict with the selection
/// options of a LIST-EXTENDED command (RFC 5258 Section 3).
///
/// When NOTIFY is active, a concurrent create event may match the wildcard
/// but lack the attributes required by the selection options (SUBSCRIBED,
/// REMOTE, SPECIAL-USE). Returns `true` if the response does NOT satisfy
/// the selection criteria and should be treated as a NOTIFY event.
pub(super) fn is_notify_selection_mismatch(
    info: &MailboxInfo,
    selection_options: &[crate::types::command_options::ListSelect],
) -> bool {
    let has_recursivematch = selection_options
        .iter()
        .any(|o| matches!(o, crate::types::command_options::ListSelect::RecursiveMatch));

    for opt in selection_options {
        if matches!(opt, crate::types::command_options::ListSelect::Subscribed) {
            let has_subscribed = info
                .attributes
                .iter()
                .any(|a| matches!(a, MailboxAttribute::Subscribed));
            // RFC 5258 Section 3.5: with RECURSIVEMATCH, a mailbox can be
            // returned without \Subscribed if it has children with matching
            // subscriptions (indicated by non-empty CHILDINFO extended data).
            let has_childinfo = has_recursivematch && !info.child_info.is_empty();
            if !has_subscribed && !has_childinfo {
                return true;
            }
        }
        if matches!(opt, crate::types::command_options::ListSelect::Remote)
            && !info
                .attributes
                .iter()
                .any(|a| matches!(a, MailboxAttribute::Remote))
        {
            return true;
        }
        // RFC 6154 Section 3: SPECIAL-USE requests only mailboxes with
        // a special-use attribute.
        if matches!(opt, crate::types::command_options::ListSelect::SpecialUse)
            && !info.attributes.iter().any(MailboxAttribute::is_special_use)
        {
            return true;
        }
    }
    false
}

/// Check whether a LIST response carries markers that identify it as a
/// NOTIFY event rather than a solicited LIST result (RFC 5465 Section 5.4).
///
/// NOTIFY delivers `MailboxName` events (rename, delete, ACL change,
/// subscription change) as LIST responses. Reliable markers:
///
/// - **OLDNAME** (RFC 9051 Section 6.3.9.7): present on rename events.
///   A solicited LIST lists current mailbox state and never includes OLDNAME.
///   Always treated as a NOTIFY marker.
/// - **`\NonExistent`** (RFC 9051 Section 7.2.2): present on delete events.
///   A solicited plain LIST only returns existing mailboxes so `\NonExistent`
///   is a reliable NOTIFY marker in that context. However, LIST-EXTENDED with
///   SUBSCRIBED can legitimately return subscribed-but-deleted mailboxes with
///   `\NonExistent` (RFC 5258 Section 3).
/// - **`\NoAccess`** (RFC 5465 Section 5.9): present when the logged-in
///   user loses the `l` ACL right on a monitored mailbox. A solicited plain
///   LIST typically does not return inaccessible mailboxes. However,
///   LIST-EXTENDED with SUBSCRIBED may return subscribed-but-inaccessible
///   mailboxes with `\NoAccess`.
///
/// Callers must set `filter_extended_markers` to `false` when
/// `\NonExistent` / `\NoAccess` can legitimately appear on solicited
/// results (i.e., LIST-EXTENDED with SUBSCRIBED).
///
/// Create events carry no distinguishing marker and are not filtered.
// SELECT has no SUBSCRIBED selection option, so its callers always filter
// extended markers. LIST-EXTENDED derives this policy from its own options.
pub(super) fn is_notify_list_event(info: &MailboxInfo, filter_extended_markers: bool) -> bool {
    // Rename event: OLDNAME extended data item  -  always a NOTIFY marker.
    if info.old_name.is_some() {
        return true;
    }
    // Delete (\NonExistent) and ACL-loss (\NoAccess) events. Only
    // treated as NOTIFY markers when the caller confirms that these
    // attributes cannot appear on solicited results (i.e., plain LIST
    // or LIST-EXTENDED without SUBSCRIBED).
    if filter_extended_markers
        && info.attributes.iter().any(|a| {
            matches!(
                a,
                MailboxAttribute::NonExistent | MailboxAttribute::NoAccess
            )
        })
    {
        return true;
    }
    false
}

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
