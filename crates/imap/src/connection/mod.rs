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
    MailboxInfo, MailboxName, MetadataEntry, MetadataResult, MoveResult, NamespaceResponse,
    NotifyEvent, NotifySetParams, QresyncParams, QuotaResource, QuotaRootResponse, Response,
    ResponseCode, SelectOptions, SelectedMailbox, SequenceSet, StatusItem, StatusResult,
    StoreOperation, StoreResult, TaggedResponse, ThreadNode, UidRange, UntaggedResponse,
    UntaggedStatus, format_fetch_attrs,
};

pub(crate) const LOGOUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

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
mod search_validation;
mod seq_ops;
mod sort_thread;
pub(super) mod state;
mod stream;
mod tag;
pub(crate) mod typed_event;
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

/// Event received during an IDLE session (RFC 2177).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum IdleEvent {
    /// New message(s) arrived  -  `* <n> EXISTS` (RFC 3501 Section 7.3.1).
    Exists(u32),
    /// Message expunged  -  `* <n> EXPUNGE` (RFC 3501 Section 7.4.1).
    Expunge(u32),
    /// Messages vanished  -  `* VANISHED [EARLIER] uid-set` (RFC 7162 Section 3.2.10.2).
    ///
    /// After `ENABLE QRESYNC`, servers send VANISHED instead of EXPUNGE.
    Vanished {
        /// `true` if this was a `VANISHED (EARLIER)` response (initial sync).
        earlier: bool,
        /// UIDs of vanished messages.
        uids: Vec<UidRange>,
    },
    /// Message data changed  -  `* n FETCH ...` (RFC 3501 Section 7.4.2, RFC 2177 Section 3).
    ///
    /// During IDLE, the server may send unsolicited FETCH responses when message
    /// attributes change (e.g., flags updated by another session). The full
    /// `FetchResponse` is preserved so callers can inspect the sequence number,
    /// UID, flags, and any other returned data items.
    Fetch(Box<crate::types::FetchResponse>),
    /// Recent message count changed  -  `* <n> RECENT` (RFC 3501 Section 7.3.2).
    ///
    /// RFC 2177 allows the server to send mailbox size messages during IDLE;
    /// `* n RECENT` is one such message.
    Recent(u32),
    /// Server sent an ALERT that MUST be presented to the user
    /// (RFC 3501 Section 7.1).
    ///
    /// RFC 3501 Section 7.1 mandates: "The human-readable text contains a
    /// special alert that MUST be presented to the user in a fashion that
    /// calls the user's attention to the message." RFC 2177 (IDLE) does not
    /// exempt this requirement, so alerts received during IDLE are surfaced
    /// immediately rather than buffered.
    Alert(String),
    /// The idle timed out (caller-supplied timeout elapsed).
    Timeout,
    /// The idle was cancelled via the `CancellationToken`.
    Cancelled,
    /// Mailbox created, deleted, renamed, or access changed  -  `* LIST ...`
    /// (RFC 5465 Sections 5.4-5.5).
    ///
    /// When NOTIFY is active with `MailboxName` (Section5.4) or
    /// `SubscriptionChange` (Section5.5) events, the server delivers
    /// mailbox-level notifications as LIST responses during IDLE.
    MailboxEvent(MailboxInfo),
    /// Non-selected mailbox status changed  -  `* STATUS "mailbox" (...)`
    /// (RFC 5465 Section 4, Sections 5.1-5.3).
    ///
    /// When NOTIFY is active with message events (Section5.1-5.3) on
    /// non-selected mailboxes, the server delivers status changes (new
    /// messages, expunges, flag changes) as STATUS responses during IDLE.
    /// The initial snapshot is delivered per the STATUS indicator (Section4).
    MailboxStatus {
        /// The mailbox whose status changed.
        mailbox: MailboxName,
        /// The status items that changed.
        items: Vec<StatusItem>,
    },
    /// Mailbox or server metadata changed  -  `* METADATA "mailbox" (...)`
    /// (RFC 5465 Sections 5.6-5.7).
    ///
    /// When NOTIFY is active with `MailboxMetadataChange` (Section5.6) or
    /// `ServerMetadataChange` (Section5.7) events, the server delivers metadata
    /// notifications as METADATA responses during IDLE.
    MetadataChange {
        /// The mailbox whose metadata changed (empty string for server-level).
        mailbox: MailboxName,
        /// The metadata entries that changed.
        entries: Vec<MetadataEntry>,
    },
    /// Search context update  -  `* ESEARCH ...` or `* SEARCH ...`
    /// (RFC 5267 Sections 2.4 / RFC 5465 Sections 5.1-5.3).
    ///
    /// When a search context is active (RFC 5267) or NOTIFY triggers
    /// search-related notifications, the server may deliver ESEARCH
    /// updates during IDLE.
    ///
    /// **Note on legacy `* SEARCH` conversion:** When the server sends a
    /// legacy `* SEARCH n1 n2 ...` update (instead of ESEARCH), the numbers
    /// are wrapped in an `EsearchResponse` with `uid: false`. However, IMAP
    /// uses the same wire form for both sequence-number and UID results  -
    /// the `uid` field may be inaccurate for legacy SEARCH. Callers should
    /// use the `tag` field (if present) to correlate with the original search
    /// context and determine the number semantics.
    SearchUpdate(Box<crate::types::EsearchResponse>),
    /// Extension-defined untagged response not recognized by the parser
    /// (RFC 9051 Section 2.2.2).
    ///
    /// When `NotifyEvent::Other(...)` is registered, the server may deliver
    /// extension-defined notifications using response types this client does
    /// not implement. The raw response line is preserved so callers can
    /// parse extension data themselves.
    ExtensionEvent(String),
    /// Unsolicited status update with a response code  -  `* OK [code]` or
    /// `* NO [code]` (RFC 3501 Section 7.1).
    ///
    /// Covers `[PERMANENTFLAGS]`, `[UIDVALIDITY]`, and any other response
    /// code not handled by a more specific variant. Surfaced during IDLE
    /// so the caller can react to state changes (e.g., updated writable
    /// flags, NOTIFY-driven metadata updates).
    StatusUpdate {
        /// The original response condition (RFC 3501 Section 7.1).
        /// OK, NO, and BAD are semantically distinct and must be preserved.
        status: UntaggedStatus,
        /// The response code.
        code: ResponseCode,
        /// Human-readable text.
        text: String,
    },
    /// The server discarded the NOTIFY registration due to overflow
    /// (RFC 5465 Section 5.8).
    ///
    /// The client MUST behave as if `NOTIFY NONE` was received  -  the
    /// registration is gone and no further notifications will be delivered.
    /// Callers should re-issue `notify_set()` to re-establish monitoring.
    NotificationOverflow {
        /// The response-code payload from `[NOTIFICATIONOVERFLOW ...]`
        /// (RFC 5465 Section 5.8). `None` when the server omits the
        /// optional argument.
        code_text: Option<String>,
        /// Human-readable text from the untagged status line
        /// (RFC 3501 Section 7.1).
        resp_text: String,
    },
    /// The server sent `* BYE`  -  the connection is closing
    /// (RFC 3501 Section 7.1.5).
    ///
    /// Unlike [`Alert`], this signals that the server is terminating
    /// the connection. After receiving this event, further commands
    /// will fail.
    Bye {
        /// Optional response code from the BYE response.
        code: Option<ResponseCode>,
        /// Human-readable reason for disconnection.
        text: String,
    },
    /// The server terminated the IDLE session by sending the tagged OK
    /// response (RFC 2177 Section 3).
    ///
    /// Some servers (Exchange, Zimbra) have short IDLE limits and terminate
    /// IDLE from the server side by sending the tagged OK rather than
    /// waiting for the client's DONE. When this happens, the client MUST NOT
    /// send DONE because the IDLE command is already complete.
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
    /// Set when a submitted command's future is dropped before its result
    /// arrives - the caller's `tokio::time::timeout` fired, or it was
    /// cancelled. The driver is still executing that command, so the next
    /// command submitted on this connection queues behind it and is very
    /// likely to time out in turn. The flag makes the checkout unreusable
    /// so the cascade stops at one connection.
    abandoned: std::sync::atomic::AtomicBool,
}

pub(crate) struct DriverTask {
    pub(crate) handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    abort_handle: Option<tokio::task::AbortHandle>,
    inbound_bytes: Arc<std::sync::atomic::AtomicU64>,
}

impl DriverTask {
    pub(crate) fn new(handle: Option<tokio::task::JoinHandle<()>>) -> Self {
        Self::with_inbound_counter(handle, Arc::new(std::sync::atomic::AtomicU64::new(0)))
    }

    pub(crate) fn with_inbound_counter(
        handle: Option<tokio::task::JoinHandle<()>>,
        inbound_bytes: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        let abort_handle = handle.as_ref().map(tokio::task::JoinHandle::abort_handle);
        Self {
            handle: tokio::sync::Mutex::new(handle),
            abort_handle,
            inbound_bytes,
        }
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

/// Guard around one submitted command. Marks the connection abandoned
/// unless [`completed`](Self::completed) is called, which happens only on
/// the path where the driver's result has actually been received.
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

/// Per-type NOTIFY flags (RFC 5465 Sections 5.1-5.8).
///
/// Tracks which response types the current NOTIFY registration can produce
/// so that IDLE event classification and LIST filtering during LIST-family
/// commands know which types to expect.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NotifyFlags {
    /// `MailboxName` or `SubscriptionChange` events registered
    /// (RFC 5465 Sections 5.4-5.5  -  delivered as LIST).
    pub(crate) list: bool,
    /// `MessageNew`/`MessageExpunge` on non-selected mailboxes or STATUS
    /// indicator (RFC 5465 Sections 4, 5.1-5.2  -  delivered as STATUS).
    pub(crate) status: bool,
    /// `MailboxMetadataChange` or `ServerMetadataChange` events registered
    /// (RFC 5465 Sections 5.6-5.7  -  delivered as METADATA).
    pub(crate) metadata: bool,
}

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

    /// Open an in-flight guard for one submitted command.
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
        return Err(Error::InvalidInput(format!(
            "invalid TLS server name: {host:?}"
        )));
    }
    Ok(())
}

/// Filter flags not valid in STORE flag lists (RFC 3501 Section 2.3.2).
///
/// `\Recent` is server-only and `\*` (wildcard) is not valid in STORE.
fn filter_store_flags(flags: &[Flag]) -> Vec<Flag> {
    flags
        .iter()
        .filter(|f| !matches!(f, Flag::Recent | Flag::Wildcard))
        .cloned()
        .collect()
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

/// Build a `SelectedMailbox` from collected untagged and tagged responses.
///
/// Extracts EXISTS, RECENT, FLAGS, UIDVALIDITY, UIDNEXT, PERMANENTFLAGS,
/// HIGHESTMODSEQ, and UIDNOTSTICKY from the responses per RFC 3501 Section 7.
fn build_selected_mailbox(
    untagged: &[UntaggedResponse],
    tagged: &TaggedResponse,
    read_only: bool,
) -> SelectedMailbox {
    let mut exists = 0;
    let mut recent = 0;
    let mut uid_validity: Option<u32> = None;
    let mut uid_next = Option::None;
    let mut flags = Vec::new();
    let mut permanent_flags = Vec::new();
    let mut highest_mod_seq = Option::None;
    let mut no_mod_seq = false;
    let mut unseen: Option<u32> = None;
    // RFC 8474 Section 5.1: unique mailbox identifier from [MAILBOXID (<id>)].
    let mut mailbox_id: Option<String> = None;
    // RFC 4315 Section 2 / RFC 9051 Section 7.1: [UIDNOTSTICKY]  -  UIDs are not persistent.
    let mut uid_not_sticky = false;
    // RFC 7162 Section 3.2.5.2: QRESYNC SELECT responses include
    // VANISHED (EARLIER) with UIDs removed since last sync, and
    // FETCH responses with changed flags.
    let mut vanished = Vec::new();
    let mut changed_messages = Vec::new();

    let effective_responses = selected_mailbox_effective_responses(untagged);

    for resp in effective_responses {
        match resp {
            UntaggedResponse::Exists(n) => exists = *n,
            UntaggedResponse::Recent(n) => recent = *n,
            UntaggedResponse::Flags(f) => flags.clone_from(f),
            UntaggedResponse::Status {
                code: Some(code), ..
            } => {
                extract_selected_code(
                    code,
                    &mut uid_validity,
                    &mut uid_next,
                    &mut permanent_flags,
                    &mut highest_mod_seq,
                    &mut no_mod_seq,
                    &mut unseen,
                    &mut mailbox_id,
                    &mut uid_not_sticky,
                );
            }
            // RFC 7162 Section 3.2.5.2: capture VANISHED (EARLIER) responses
            // sent during QRESYNC SELECT/EXAMINE. Only `earlier: true` responses
            // belong to the initial sync; `earlier: false` are unsolicited and
            // should not be included in the SelectedMailbox.
            UntaggedResponse::Vanished {
                earlier: true,
                uids,
            } => {
                vanished.extend_from_slice(uids);
            }
            // RFC 7162 Section 3.2.5.2: capture FETCH responses with
            // updated flags sent during QRESYNC SELECT/EXAMINE.
            UntaggedResponse::Fetch(fetch) => {
                changed_messages.push((**fetch).clone());
            }
            _ => {}
        }
    }

    // Also check the tagged response code
    if let Some(code) = &tagged.code {
        extract_selected_code(
            code,
            &mut uid_validity,
            &mut uid_next,
            &mut permanent_flags,
            &mut highest_mod_seq,
            &mut no_mod_seq,
            &mut unseen,
            &mut mailbox_id,
            &mut uid_not_sticky,
        );
    }

    SelectedMailbox {
        exists,
        recent,
        uid_validity,
        uid_next,
        flags,
        permanent_flags,
        highest_mod_seq,
        no_mod_seq,
        unseen,
        mailbox_id,
        read_only,
        uid_not_sticky,
        vanished,
        changed_messages,
    }
}

/// Restrict SELECT/EXAMINE processing to responses belonging to the newly
/// selected mailbox.
///
/// RFC 7162 Section 3.2.11: when switching mailboxes, `* OK [CLOSED]`
/// separates responses for the previously selected mailbox from the new one.
fn selected_mailbox_effective_responses(untagged: &[UntaggedResponse]) -> &[UntaggedResponse] {
    match untagged.iter().rposition(|r| {
        matches!(
            r,
            UntaggedResponse::Status {
                code: Some(ResponseCode::Closed),
                ..
            }
        )
    }) {
        Some(closed_idx) => &untagged[closed_idx + 1..],
        None => untagged,
    }
}

/// Extract mailbox metadata from a response code (RFC 3501 Section 7.1).
#[allow(clippy::too_many_arguments)]
fn extract_selected_code(
    code: &ResponseCode,
    uid_validity: &mut Option<u32>,
    uid_next: &mut Option<u32>,
    permanent_flags: &mut Vec<Flag>,
    highest_mod_seq: &mut Option<u64>,
    no_mod_seq: &mut bool,
    unseen: &mut Option<u32>,
    mailbox_id: &mut Option<String>,
    uid_not_sticky: &mut bool,
) {
    match code {
        ResponseCode::UidValidity(v) => *uid_validity = Some(*v),
        ResponseCode::UidNext(v) => *uid_next = Some(*v),
        ResponseCode::PermanentFlags(f) => permanent_flags.clone_from(f),
        ResponseCode::HighestModSeq(v) => {
            // RFC 7162 Section 3.1.2.1: mod-sequence-value >= 1.
            // HIGHESTMODSEQ 0 is semantically invalid  -  the server should
            // have sent [NOMODSEQ] instead. Treat it equivalently per
            // Postel's law (RFC 1122 Section 1.2.2).
            if *v == 0 {
                *no_mod_seq = true;
            } else {
                *highest_mod_seq = Some(*v);
            }
        }
        // RFC 7162 Section 3.1.2: [NOMODSEQ]  -  mailbox does not support
        // mod-sequences. Distinct from the server simply not sending
        // HIGHESTMODSEQ.
        ResponseCode::NoModSeq => *no_mod_seq = true,
        // RFC 3501 Section 7.1: [UNSEEN n]  -  first unseen message sequence number.
        // Dropped in IMAP4rev2 (RFC 9051), but commonly sent by rev1 servers.
        ResponseCode::Unseen(v) => *unseen = Some(*v),
        // RFC 8474 Section 5.1: [MAILBOXID (<id>)]  -  unique mailbox identifier.
        ResponseCode::MailboxId(id) => *mailbox_id = Some(id.clone()),
        // RFC 4315 Section 2 / RFC 9051 Section 7.1: [UIDNOTSTICKY]  -
        // UIDs assigned to messages in this mailbox are not persistent.
        ResponseCode::UidNotSticky => *uid_not_sticky = true,
        _ => {}
    }
}

/// Check whether a LIST response's attributes conflict with the selection
/// options of a LIST-EXTENDED command (RFC 5258 Section 3).
///
/// When NOTIFY is active, a concurrent create event may match the wildcard
/// but lack the attributes required by the selection options (SUBSCRIBED,
/// REMOTE, SPECIAL-USE). Returns `true` if the response does NOT satisfy
/// the selection criteria and should be treated as a NOTIFY event.
pub(super) fn is_notify_selection_mismatch(info: &MailboxInfo, selection_options: &[&str]) -> bool {
    let has_recursivematch = selection_options
        .iter()
        .any(|o| o.eq_ignore_ascii_case("RECURSIVEMATCH"));

    for opt in selection_options {
        if opt.eq_ignore_ascii_case("SUBSCRIBED") {
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
        if opt.eq_ignore_ascii_case("REMOTE")
            && !info
                .attributes
                .iter()
                .any(|a| matches!(a, MailboxAttribute::Remote))
        {
            return true;
        }
        // RFC 6154 Section 3: SPECIAL-USE requests only mailboxes with
        // a special-use attribute.
        if opt.eq_ignore_ascii_case("SPECIAL-USE")
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
