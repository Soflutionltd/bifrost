#![allow(clippy::wildcard_imports)]
use super::*;
#[cfg(test)]
pub(crate) use crate::codec::classification::mailbox_names_eq as inbox_eq;

impl ImapConnection {
    /// Current session state (RFC 3501 Section3 / RFC 9051 Section3).
    ///
    /// Returns a snapshot of the session state as last observed by the
    /// driver task. State transitions are driven by command responses
    /// (e.g., `SELECT` moves to `Selected`, `LOGOUT` moves to `Logout`).
    pub fn session_state(&self) -> SessionState {
        self.state_rx.borrow().session_state
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
        let snap = self.state_rx.borrow();
        let utf8_enabled = snap
            .enabled
            .iter()
            .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"));
        let needs_utf8 = snap.capabilities.contains(&Capability::Utf8Only)
            && !utf8_enabled
            && !super::auth::is_rev2_from_snapshot(&snap);
        drop(snap);
        if needs_utf8 {
            // A mode the caller has not established, not a missing server
            // capability: the same request succeeds after the ENABLE.
            return Err(Error::InvalidState(
                "server requires ENABLE UTF8=ACCEPT before use \
                 (UTF8=ONLY advertised, RFC 6855 Section 3)"
                    .into(),
            ));
        }
        Ok(())
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
        let snap = self.state_rx.borrow();
        let session_state = snap.session_state;
        let mailbox_selected = snap.mailbox_selected;
        drop(snap);
        if let Some(refusal) = state_refusal(session_state, kind.legal_states()) {
            return Err(refusal);
        }
        if kind.refused_after_selection() && mailbox_selected {
            return Err(Error::InvalidState(format!(
                "{kind:?} is not valid once a mailbox has been selected in the session \
                 (RFC 5161 Section 3.1)"
            )));
        }
        Ok(())
    }

    /// Verify that the server supports CONDSTORE (RFC 7162 Section 3.1).
    ///
    /// QRESYNC implies CONDSTORE (RFC 7162 Section 3.2.3), so either
    /// capability satisfies the requirement.
    pub(super) fn require_condstore(&self) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        let has_condstore = super::auth::snapshot_supports(&snap, &Capability::Condstore);
        drop(snap);
        if !has_condstore {
            return Err(Error::MissingCapability("CONDSTORE".into()));
        }
        Ok(())
    }

    /// Verify that the server advertises the SEARCHRES capability (RFC 5182 Section 2).
    pub(super) fn require_searchres(&self) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        // RFC 9051 Appendix E: IMAP4rev2 folds SEARCHRES into the base protocol.
        let has_searchres = super::auth::snapshot_supports(&snap, &Capability::SearchRes);
        drop(snap);
        if !has_searchres {
            return Err(Error::MissingCapability("SEARCHRES".into()));
        }
        Ok(())
    }

    /// Returns `true` when a generic SEARCH RETURN command requests the SAVE
    /// result option from the SEARCHRES extension (RFC 5182 Section 2).
    pub(super) fn search_return_requests_save(cmd: &Command) -> bool {
        match cmd {
            Command::SearchReturn { return_opts, .. }
            | Command::UidSearchReturn { return_opts, .. } => return_opts
                .iter()
                .any(|opt| opt.trim().eq_ignore_ascii_case("SAVE")),
            _ => false,
        }
    }

    /// `true` when the server advertises support for the named quota resource
    /// via `QUOTA=RES-<name>` (RFC 9208 Section 3.1.1).
    pub(super) fn has_quota_resource(&self, resource: &str) -> bool {
        let snap = self.state_rx.borrow();
        snap.capabilities.iter().any(|cap| {
            Self::quota_resource_name(cap).is_some_and(|name| name.eq_ignore_ascii_case(resource))
        })
    }

    /// Returns the quota resource name from `QUOTA=RES-<name>`
    /// capabilities (RFC 9208 Section 3.1.1).
    pub(super) fn quota_resource_name(cap: &Capability) -> Option<&str> {
        match cap {
            Capability::QuotaResource(name) => Some(name.as_str()),
            Capability::Other(s)
                if s.len() > "QUOTA=RES-".len()
                    && s[.."QUOTA=RES-".len()].eq_ignore_ascii_case("QUOTA=RES-") =>
            {
                Some(&s["QUOTA=RES-".len()..])
            }
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
        if patterns.is_empty() {
            return Err(Error::InvalidInput(
                "LIST-EXTENDED requires at least one mailbox pattern \
                 (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                    .into(),
            ));
        }

        {
            let snap = self.state_rx.borrow();

            if patterns.len() > 1
                && !super::auth::snapshot_supports(&snap, &Capability::ListExtended)
            {
                return Err(Error::MissingCapability("LIST-EXTENDED".into()));
            }

            // RFC 5258 Section 3 defines selection options and return options as
            // LIST-EXTENDED syntax on IMAP4rev1, even when the individual option
            // token comes from another extension such as RFC 6154 SPECIAL-USE or
            // RFC 5819 LIST-STATUS.
            let needs_list_extended = !selection_options.is_empty() || !return_options.is_empty();

            if needs_list_extended
                && !super::auth::snapshot_supports(&snap, &Capability::ListExtended)
            {
                return Err(Error::MissingCapability("LIST-EXTENDED".into()));
            }

            for option in selection_options {
                let trimmed = option.trim();
                if trimmed.is_empty() {
                    return Err(Error::InvalidInput(
                        "LIST-EXTENDED selection options must not be empty \
                         (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                            .into(),
                    ));
                }
                if trimmed.eq_ignore_ascii_case("SPECIAL-USE")
                    && !super::auth::snapshot_supports(&snap, &Capability::SpecialUse)
                {
                    return Err(Error::MissingCapability("SPECIAL-USE".into()));
                }
            }
        }

        let has_recursivematch = selection_options
            .iter()
            .any(|option| option.trim().eq_ignore_ascii_case("RECURSIVEMATCH"));
        for option in return_options {
            let trimmed = option.trim();
            if trimmed.is_empty() {
                return Err(Error::InvalidInput(
                    "LIST-EXTENDED return options must not be empty \
                     (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                        .into(),
                ));
            }

            {
                let snap = self.state_rx.borrow();
                if trimmed.eq_ignore_ascii_case("SPECIAL-USE")
                    && !super::auth::snapshot_supports(&snap, &Capability::SpecialUse)
                {
                    return Err(Error::MissingCapability("SPECIAL-USE".into()));
                }
            }

            if let Some(status_items) =
                Self::list_status_return_option_items(trimmed).transpose()?
            {
                {
                    let snap = self.state_rx.borrow();
                    if !super::auth::snapshot_supports(&snap, &Capability::ListStatus) {
                        return Err(Error::MissingCapability("LIST-STATUS".into()));
                    }
                }
                self.validate_requested_status_items(status_items)?;
            }
        }

        if has_recursivematch
            && !selection_options.iter().any(|option| {
                let trimmed = option.trim();
                !trimmed.is_empty()
                    && !trimmed.eq_ignore_ascii_case("RECURSIVEMATCH")
                    && !trimmed.eq_ignore_ascii_case("REMOTE")
            })
        {
            return Err(Error::InvalidInput(
                "LIST-EXTENDED selection option RECURSIVEMATCH requires another \
                 non-REMOTE selection option (RFC 5258 Section 3 / \
                 RFC 9051 Section 6.3.9)"
                    .into(),
            ));
        }

        Ok(())
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
        for item in Self::status_item_tokens(items)? {
            match item.to_ascii_uppercase().as_str() {
                "RECENT" => {
                    if self.is_rev2() {
                        return Err(Error::MissingCapability(
                            "STATUS item RECENT was removed in IMAP4rev2 \
                             (RFC 9051 Section 6.3.11)"
                                .into(),
                        ));
                    }
                }
                "DELETED" => {
                    // Deliberately NOT `snapshot_supports(StatusDeleted)`. The
                    // two agree under active rev2; they differ only on rev1,
                    // where the authority would also accept an advertised
                    // `STATUS=DELETED` token. No RFC defines that token: RFC
                    // 9051 Section 6.3.11 makes DELETED a base rev2 item, and
                    // the only rev1 route is RFC 9208 Section 4.1.4's
                    // QUOTA=RES-MESSAGE. The gate therefore names exactly
                    // those two sources.
                    if !self.is_rev2() && !self.has_quota_resource("MESSAGE") {
                        return Err(Error::MissingCapability(
                            "STATUS item DELETED requires IMAP4rev2 or \
                             QUOTA=RES-MESSAGE (RFC 9051 Section 6.3.11 / \
                             RFC 9208 Section 4.1.4)"
                                .into(),
                        ));
                    }
                }
                "DELETED-STORAGE" => {
                    if !self.has_quota_resource("STORAGE") {
                        return Err(Error::MissingCapability("QUOTA=RES-STORAGE".into()));
                    }
                }
                "SIZE" => {
                    let snap = self.state_rx.borrow();
                    if !super::auth::snapshot_supports(&snap, &Capability::StatusSize) {
                        return Err(Error::MissingCapability("STATUS=SIZE".into()));
                    }
                }
                "HIGHESTMODSEQ" => self.require_condstore()?,
                "APPENDLIMIT" => {
                    let has_appendlimit = self
                        .state_rx
                        .borrow()
                        .capabilities
                        .iter()
                        .any(|cap| matches!(cap, Capability::AppendLimit(_)));
                    if !has_appendlimit {
                        return Err(Error::MissingCapability("APPENDLIMIT".into()));
                    }
                }
                "MAILBOXID" => {
                    let snap = self.state_rx.borrow();
                    if !super::auth::snapshot_supports(&snap, &Capability::ObjectId) {
                        return Err(Error::MissingCapability("OBJECTID".into()));
                    }
                }
                _ => {}
            }
        }
        Ok(())
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
        let snap = self.state_rx.borrow();
        let usable = |capability: Capability| super::auth::snapshot_supports(&snap, &capability);
        for item in items {
            match item {
                // RFC 7162 Section 3.1.5: MODSEQ requires CONDSTORE/QRESYNC
                // (the authority treats QRESYNC as implying CONDSTORE).
                FetchAttr::ModSeq => {
                    if !usable(Capability::Condstore) {
                        return Err(Error::MissingCapability("CONDSTORE".into()));
                    }
                }
                // RFC 8970 Section 4: PREVIEW requires PREVIEW capability.
                FetchAttr::Preview | FetchAttr::PreviewLazy => {
                    if !usable(Capability::Preview) {
                        return Err(Error::MissingCapability("PREVIEW".into()));
                    }
                }
                // RFC 8514 Section 3: SAVEDATE requires SAVEDATE capability.
                FetchAttr::SaveDate => {
                    if !usable(Capability::SaveDate) {
                        return Err(Error::MissingCapability("SAVEDATE".into()));
                    }
                }
                // RFC 8474 Sections 4 and 7: EMAILID/THREADID require OBJECTID.
                FetchAttr::EmailId | FetchAttr::ThreadId => {
                    if !usable(Capability::ObjectId) {
                        return Err(Error::MissingCapability("OBJECTID".into()));
                    }
                }
                FetchAttr::GmailMsgId | FetchAttr::GmailThreadId | FetchAttr::GmailLabels => {
                    if !usable(Capability::XGmExt1) {
                        return Err(Error::MissingCapability("X-GM-EXT-1".into()));
                    }
                }
                // RFC 9051 Appendix B: IMAP4rev2 folds the FETCH side of
                // RFC 3516 into the base protocol, but not its APPEND side,
                // so the rev2 clause is asked here and NOT through
                // `supports(Binary)`, which stays advertised-only.
                FetchAttr::Binary { .. } | FetchAttr::BinarySize { .. }
                    if !crate::types::profile::binary_fetch_usable(
                        &snap.capabilities,
                        &snap.enabled,
                    ) =>
                {
                    return Err(Error::MissingCapability("BINARY".into()));
                }
                FetchAttr::Binary { .. } | FetchAttr::BinarySize { .. } => {}
                _ => {}
            }
        }
        Ok(())
    }

    /// Parse a STATUS data item list into individual item atoms.
    ///
    /// Accepts either a raw space-separated list (`"MESSAGES UNSEEN"`) or a
    /// parenthesized `status-att-list` (`"(MESSAGES UNSEEN)"`). The list must
    /// contain at least one item, and nested or unbalanced parentheses are
    /// rejected per RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11.
    pub(super) fn status_item_tokens(items: &str) -> Result<Vec<&str>, Error> {
        let trimmed = items.trim();
        if trimmed.is_empty() {
            return Err(Error::InvalidInput(
                "STATUS item list must contain at least one data item \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ));
        }

        let body = match (trimmed.strip_prefix('('), trimmed.strip_suffix(')')) {
            (Some(without_open), Some(_)) => {
                let inner = without_open
                    .strip_suffix(')')
                    .ok_or_else(|| {
                        Error::InvalidInput(
                            "STATUS item list must use balanced parentheses \
                             (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                                .into(),
                        )
                    })?
                    .trim();
                if inner.is_empty() {
                    return Err(Error::InvalidInput(
                        "STATUS item list must contain at least one data item \
                         (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                            .into(),
                    ));
                }
                inner
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(Error::InvalidInput(
                    "STATUS item list must use balanced parentheses \
                     (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                        .into(),
                ));
            }
            (None, None) => trimmed,
        };

        if body.contains('(') || body.contains(')') {
            return Err(Error::InvalidInput(
                "STATUS item list must be flat and must not contain nested parentheses \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ));
        }

        let tokens: Vec<&str> = body.split_ascii_whitespace().collect();
        if tokens.is_empty() {
            return Err(Error::InvalidInput(
                "STATUS item list must contain at least one data item \
                 (RFC 3501 Section 6.3.10 / RFC 9051 Section 6.3.11)"
                    .into(),
            ));
        }
        Ok(tokens)
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
        tokio::time::timeout(
            timeout,
            self.submit_regular(Command::Noop, super::dispatch::TaggedOkConsumer::default()),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
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
        if self.is_rev2() {
            return Err(Error::MissingCapability(
                "CHECK is not defined in IMAP4rev2 (RFC 9051); use NOOP instead".into(),
            ));
        }
        tokio::time::timeout(
            timeout,
            self.submit_regular(Command::Check, super::dispatch::TaggedOkConsumer::default()),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
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
        tokio::time::timeout(
            timeout,
            self.submit_regular(
                Command::Capability,
                super::dispatch::CapabilityConsumer::default(),
            ),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
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
    Some(Error::InvalidState(format!(
        "command not valid in {state:?} state (expected one of {allowed:?})"
    )))
}

#[cfg(test)]
#[path = "helpers_tests.rs"]
mod tests;
