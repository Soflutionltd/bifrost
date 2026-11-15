#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    /// Current session state (RFC 3501 Section3 / RFC 9051 Section3).
    ///
    /// Returns a snapshot of the session state as last observed by the
    /// driver task. State transitions are driven by command responses
    /// (e.g., `SELECT` moves to `Selected`, `LOGOUT` moves to `Logout`).
    pub fn session_state(&self) -> SessionState {
        self.state_rx.borrow().session_state
    }

    /// Mailbox identity last observed by the driver, including failed SELECTs.
    pub(crate) fn selected_mailbox(&self) -> Option<crate::types::MailboxName> {
        self.state_rx.borrow().selected.clone()
    }

    /// Cached server capabilities (RFC 3501 Section7.2.1 / RFC 9051 Section7.2.1).
    ///
    /// Returns a clone of the capability list as last observed by the
    /// driver task. Updated automatically when the server advertises
    /// new capabilities (e.g., post-STARTTLS, post-LOGIN).
    pub fn capabilities(&self) -> Vec<Capability> {
        self.state_rx.borrow().capabilities.clone()
    }

    /// Caller-friendly server capability profile snapshot.
    ///
    /// This clones the driver's current capability and ENABLE state. The
    /// result is stale after STARTTLS, authentication, or ENABLE; call this
    /// method again after those transitions before making feature decisions.
    pub fn server_profile(&self) -> crate::types::ServerProfile {
        let snap = self.state_rx.borrow();
        crate::types::ServerProfile::new(snap.capabilities.clone(), snap.enabled.clone())
    }

    /// Whether the current transport is encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.tls_active.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Check if `IMAP4rev2` behavior is active (RFC 9051).
    ///
    /// Many extension capabilities (`ESEARCH`, `IDLE`, `MOVE`, etc.) are part of the
    /// `IMAP4rev2` base set and don't need separate capability tokens.
    ///
    /// RFC 9051 Section 6.3.1: when both `IMAP4rev1` and `IMAP4rev2` are
    /// advertised, a client MUST issue `ENABLE IMAP4rev2` before assuming
    /// rev2 behavior. If the server advertises only `IMAP4rev2` (no rev1),
    /// rev2 is implicitly active.
    pub(super) fn is_rev2(&self) -> bool {
        let snap = self.state_rx.borrow();
        super::auth::is_rev2_from_snapshot(&snap)
    }

    /// Check if the server requires UTF8=ACCEPT to be enabled (RFC 6855 Section 3).
    ///
    /// When a server advertises `UTF8=ONLY`, clients MUST issue
    /// `ENABLE UTF8=ACCEPT` before sending commands that use mailbox names
    /// or string arguments.
    ///
    /// On dual-mode servers that advertise both `IMAP4rev1` and `IMAP4rev2`,
    /// RFC 9051 Appendix A requires the client to issue `ENABLE IMAP4rev2`
    /// before rev2 UTF-8 quoted-string behavior becomes active. Once that
    /// happens, the connection is already in the UTF-8-capable mode that
    /// `UTF8=ONLY` requires.
    pub(super) fn check_utf8_only_enforced(&self) -> Result<(), Error> {
        super::admission::SessionView {
            snapshot: &self.state_rx.borrow(),
            tls_active: self.is_encrypted(),
        }
        .check_utf8_only_enforced()
    }

    /// Ensure the session is in a state `kind` may be sent in (RFC 3501
    /// Section 6), per the one table, `CommandKind::legal_states`.
    ///
    /// Refused before submission, so nothing is sent. A session in Logout is
    /// a connection that is gone, reported as `Closed` with `Unsent`
    /// evidence; any other mismatch is caller sequencing, `InvalidState`.
    /// The driver reads the same table again when the command reaches the
    /// head of its queue and reports a mismatch there as
    /// `StateChangedBeforeSend`: the two errors differ because the facts
    /// differ, the table does not. Session history is read the same way at
    /// both sites: a command `CommandKind::refused_after_selection` (ENABLE)
    /// is `InvalidState` once any mailbox has been selected in the session.
    pub(super) fn require_state(&self, kind: crate::types::CommandKind) -> Result<(), Error> {
        super::admission::SessionView {
            snapshot: &self.state_rx.borrow(),
            tls_active: self.is_encrypted(),
        }
        .require_state(kind)
    }

    /// Verify that the server supports CONDSTORE (RFC 7162 Section 3.1).
    ///
    /// QRESYNC implies CONDSTORE (RFC 7162 Section 3.2.3), so either
    /// capability satisfies the requirement.
    pub(super) fn require_condstore(&self) -> Result<(), Error> {
        super::admission::SessionView {
            snapshot: &self.state_rx.borrow(),
            tls_active: self.is_encrypted(),
        }
        .require_condstore()
    }

    /// Verify that the server advertises the SEARCHRES capability (RFC 5182 Section 2).
    pub(super) fn require_searchres(&self) -> Result<(), Error> {
        super::admission::SessionView {
            snapshot: &self.state_rx.borrow(),
            tls_active: self.is_encrypted(),
        }
        .require_searchres()
    }

    /// Returns `true` when a generic SEARCH RETURN command requests the SAVE
    /// result option from the SEARCHRES extension (RFC 5182 Section 2).
    pub(super) fn search_return_requests_save(cmd: &Command) -> bool {
        match cmd {
            Command::SearchReturn { return_opts, .. }
            | Command::UidSearchReturn { return_opts, .. } => return_opts
                .iter()
                .any(|opt| matches!(opt, crate::types::command_options::SearchReturn::Save)),
            _ => false,
        }
    }

    /// `true` when the server advertises support for the named quota resource
    /// via `QUOTA=RES-<name>` (RFC 9208 Section 3.1.1).
    pub(super) fn has_quota_resource(&self, resource: &str) -> bool {
        super::admission::SessionView {
            snapshot: &self.state_rx.borrow(),
            tls_active: self.is_encrypted(),
        }
        .has_quota_resource(resource)
    }

    /// Returns the quota resource name from `QUOTA=RES-<name>`
    /// capabilities (RFC 9208 Section 3.1.1).
    pub(super) fn quota_resource_name(cap: &Capability) -> Option<&str> {
        match cap {
            Capability::QuotaResource(name) => Some(&**name),
            // The capability parser owns recognition of QUOTA=RES-. An Other
            // value is not a second spelling of a recognized resource.
            _ => None,
        }
    }

    /// Validate LIST-EXTENDED request syntax and capability requirements
    /// before sending a LIST command with selection options, multiple
    /// patterns, or return options (RFC 5258 Section 3 /
    /// RFC 9051 Section 6.3.9 / Appendix C).
    pub(super) fn validate_list_extended_request(
        &self,
        patterns: &[&str],
        selection_options: &[&str],
        return_options: &[&str],
    ) -> Result<(), Error> {
        super::admission::SessionView {
            snapshot: &self.state_rx.borrow(),
            tls_active: self.is_encrypted(),
        }
        .validate_list_extended_request(
            patterns,
            &selection_options
                .iter()
                .map(|option| (*option).into())
                .collect::<Vec<_>>(),
            &return_options
                .iter()
                .map(|option| (*option).into())
                .collect::<Vec<_>>(),
        )
    }

    /// RFC 5819 Section 2 adds the reserved `STATUS (<items>)` return option
    /// to RFC 5258's `option-extension` grammar, so tokens that merely start
    /// with `STATUS` remain generic extension names rather than STATUS itself.
    ///
    /// This pre-encode capability check and the encoder must agree on which
    /// return options are LIST-STATUS, so both go through the encoder's
    /// implementation rather than keeping a second copy here.
    pub(super) fn list_status_return_option_items(option: &str) -> Option<Result<&str, Error>> {
        crate::codec::encode::list_status_return_option_items(option.trim())
    }

    /// Validate requested STATUS data items against the negotiated protocol
    /// version and advertised extensions before sending the command.
    ///
    /// RFC 3501 Section 6.3.10 defines the `IMAP4rev1` base items
    /// `MESSAGES`, `RECENT`, `UIDNEXT`, `UIDVALIDITY`, and `UNSEEN`.
    /// RFC 9051 Section 6.3.11 updates the `IMAP4rev2` base set to
    /// `MESSAGES`, `UIDNEXT`, `UIDVALIDITY`, `UNSEEN`, `DELETED`, and `SIZE`.
    /// RFC 9208 Section 4.1.4 additionally allows `DELETED` on
    /// `IMAP4rev1` when `QUOTA=RES-MESSAGE` is advertised and
    /// `DELETED-STORAGE` when `QUOTA=RES-STORAGE` is advertised.
    /// Additional items are gated by their respective extensions:
    /// `HIGHESTMODSEQ` (RFC 7162 Section 3.1.7), `APPENDLIMIT`
    /// (RFC 7889 Section 3), and `MAILBOXID` (RFC 8474 Section 5.1).
    pub(super) fn validate_requested_status_items(&self, items: &str) -> Result<(), Error> {
        super::admission::SessionView {
            snapshot: &self.state_rx.borrow(),
            tls_active: self.is_encrypted(),
        }
        .validate_requested_status_items(&items.into())
    }

    /// Validate requested FETCH data items against negotiated extensions
    /// before sending the command.
    ///
    /// RFC 3501 Section 6.4.5 allows extension data items in FETCH requests,
    /// but clients must not request them unless the corresponding extension
    /// has been advertised:
    /// - `MODSEQ` requires CONDSTORE/QRESYNC (RFC 7162 Section 3.1.5).
    /// - `PREVIEW` requires PREVIEW (RFC 8970 Section 4).
    /// - On `IMAP4rev1`, `BINARY[...]`, `BINARY.PEEK[...]`, and `BINARY.SIZE[...]`
    ///   require BINARY (RFC 3516 Sections 4.5.1-4.5.2).
    /// - On `IMAP4rev2`, those FETCH items are part of the base protocol
    ///   (RFC 9051 Appendix B).
    /// - `SAVEDATE` requires SAVEDATE (RFC 8514 Section 3), on rev2 as well.
    /// - `EMAILID` / `THREADID` require OBJECTID (RFC 8474 Sections 4 and 7),
    ///   on rev2 as well.
    pub(super) fn validate_requested_fetch_items(&self, items: &[FetchAttr]) -> Result<(), Error> {
        super::admission::SessionView {
            snapshot: &self.state_rx.borrow(),
            tls_active: self.is_encrypted(),
        }
        .validate_fetch_items(&crate::types::fetch::FetchItems::Attributes(items.to_vec()))
    }

    /// Parse a STATUS data item list into individual item atoms.
    ///
    /// Accepts either a raw space-separated list (`"MESSAGES UNSEEN"`) or a
    /// parenthesized `status-att-list` (`"(MESSAGES UNSEEN)"`). The list must
    /// contain at least one item, and nested or unbalanced parentheses are
    /// rejected per RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11.
    pub(super) fn status_item_tokens(items: &str) -> Result<Vec<&str>, Error> {
        crate::types::command_options::status_item_tokens(items)
    }

    /// Whether `UTF8=ACCEPT` has been enabled (RFC 6855 Section 3).
    ///
    /// Derived from the connection state snapshot. Does NOT include
    /// `IMAP4rev2`  -  use `utf8_mode()` for the combined check.
    pub(super) fn utf8_enabled(&self) -> bool {
        self.state_rx
            .borrow()
            .enabled
            .iter()
            .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"))
    }

    // -----------------------------------------------------------------------
    // NOOP (RFC 3501 Section 6.1.2 / RFC 9051 Section 6.1.2)
    // -----------------------------------------------------------------------

    /// NOOP  -  no operation (RFC 3501 Section 6.1.2 / RFC 9051 Section 6.1.2).
    ///
    /// Sends a NOOP command to the server. Since the NOOP command can
    /// include unsolicited responses from the server, this is the
    /// recommended method for polling for new messages or status updates.
    ///
    /// Valid in any state (RFC 3501 Section 6.1.2). Returns an error only
    /// if the connection is closed (e.g. after LOGOUT).
    pub async fn noop(&self, timeout: Duration) -> Result<(), Error> {
        self.submit_regular(
            Command::Noop,
            super::dispatch::TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    // -----------------------------------------------------------------------
    // CHECK (RFC 3501 Section 6.4.1)
    // -----------------------------------------------------------------------

    /// CHECK  -  request a checkpoint of the currently selected mailbox
    /// (RFC 3501 Section 6.4.1).
    ///
    /// Implementation-defined server action; typically flushes internal
    /// state to persistent storage.
    ///
    /// **`IMAP4rev1` only.** RFC 9051 removed the CHECK command from
    /// `IMAP4rev2`. On a pure rev2 connection, use [`noop`](Self::noop)
    /// instead  -  this method returns [`Error::MissingCapability`].
    pub async fn check(&self, timeout: Duration) -> Result<(), Error> {
        self.require_state(crate::types::CommandKind::Check)?;
        // RFC 9051 removed CHECK; reject when rev2 behavior is active.
        // `is_rev2_from_snapshot` already handles dual-mode servers: it
        // returns true only after `ENABLE IMAP4rev2` (or on pure-rev2
        // servers). Once rev2 is active, CHECK is undefined.

        self.submit_regular(
            Command::Check,
            super::dispatch::TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    // -----------------------------------------------------------------------
    // CAPABILITY (RFC 3501 Section 6.1.1 / RFC 9051 Section 6.1.1)
    // -----------------------------------------------------------------------

    /// CAPABILITY  -  force a capability round-trip (RFC 3501 Section 6.1.1 /
    /// RFC 9051 Section 6.1.1).
    ///
    /// Sends an explicit CAPABILITY command and returns the server's
    /// response. Unlike [`capabilities`](Self::capabilities), which
    /// returns the cached capability list, this method always performs a
    /// network round-trip and updates the cached state.
    ///
    /// Valid in any state (RFC 3501 Section 6.1.1).
    pub async fn capability(&self, timeout: Duration) -> Result<Vec<Capability>, Error> {
        self.submit_regular(
            Command::Capability,
            super::dispatch::CapabilityConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }
}

/// The refusal for a command checked against `state` on the handle, or `None`
/// when `state` is one of `allowed`.
///
/// The one classification every handle-side session check shares. Logout is
/// a connection that is gone - a BYE or LOGOUT completed - so it is `Closed`
/// with `Unsent` evidence, a transport condition a reconnect answers. Any
/// other mismatch is the caller issuing a command the session it holds does
/// not permit, `InvalidState`: a state refresh does not make it valid.
pub(super) fn state_refusal(state: SessionState, allowed: &[SessionState]) -> Option<Error> {
    if allowed.contains(&state) {
        return None;
    }
    if state == SessionState::Logout {
        return Some(Error::closed().with_attempt(bifrost_types::TransmissionState::Unsent));
    }
    Some(Error::InvalidState(crate::error::LocalFailure::state(
        crate::error::StateRequirement::LegalStates {
            actual: state,
            allowed: allowed.to_vec(),
        },
        format!("command not valid in {state:?} state (expected one of {allowed:?})"),
    )))
}

#[cfg(test)]
#[path = "helpers_tests.rs"]
mod tests;
