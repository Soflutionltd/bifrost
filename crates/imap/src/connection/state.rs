//! Protocol state with controlled mutators (RFC 3501 Section3, Section7.1).
//!
//! All fields are private to this module. State is mutated through four
//! entry points, each handling a distinct lifecycle phase:
//!
//! - [`ProtocolState::apply_greeting`]  -  greeting processing at connect time.
//! - [`ProtocolState::apply_infrastructure_failure`]  -  TLS/stream failures.
//! - [`ProtocolState::apply_capability_fetch`]  -  explicit CAPABILITY or
//!   post-upgrade re-fetch.
//! - [`ProtocolState::apply_side_effects`]  -  ongoing response processing
//!   (the primary mutator during the driver loop).
//!
//! Scoped setters (`set_in_auth`, `set_in_select`, etc.) signal in-flight
//! command context so that `apply_side_effects` can interpret state
//! transitions correctly. They do not mutate session state or capabilities
//! directly.
//!
//! Direct field assignment from outside this module is a compile error (I2).

use tracing::warn;

use super::{NotifyFlags, SessionState};
use crate::error::Error;
use crate::types::Response;
use crate::types::response::{
    Capability, GreetingResponse, GreetingStatus, ResponseCode, TaggedResponse, UntaggedResponse,
    UntaggedStatus,
};
use crate::types::validated::MailboxName;

/// Centralized protocol state (RFC 3501 Section3, Section7.1).
///
/// Owns every piece of mutable protocol state. Fields are private to
/// this module  -  callers read through getters, and mutation is
/// restricted to four methods: [`apply_greeting`](Self::apply_greeting),
/// [`apply_infrastructure_failure`](Self::apply_infrastructure_failure),
/// [`apply_capability_fetch`](Self::apply_capability_fetch), and
/// [`apply_side_effects`](Self::apply_side_effects).
#[derive(Debug)]
#[allow(clippy::struct_excessive_bools)] // Each bool is an independent in-flight command flag.
pub(crate) struct ProtocolState {
    /// Current session state (RFC 3501 Section3).
    state: SessionState,
    /// Cached server capabilities (RFC 3501 Section7.2.1).
    capabilities: Vec<Capability>,
    /// Per-type NOTIFY registration flags (RFC 5465 Section5.1-5.8).
    notify: NotifyFlags,
    /// Currently selected mailbox, if any (RFC 3501 Section3.3).
    selected: Option<MailboxName>,
    /// Whether any SELECT or EXAMINE has succeeded in this session, even
    /// if CLOSE or UNSELECT has since returned it to Authenticated. Session
    /// history, not session state: RFC 5161 Section 3.1 forbids ENABLE once
    /// a mailbox has been selected. Reset only by UNAUTHENTICATE, which
    /// starts a new session (RFC 8437 Section 2).
    mailbox_selected: bool,
    /// Successfully `ENABLE`d extensions (RFC 5161 Section3.2).
    enabled: Vec<String>,
    /// The tag of an in-flight LOGOUT command. Consulted by
    /// `apply_tagged` to transition to `Logout` on tagged OK.
    in_logout: Option<String>,
    /// `true` while a LOGIN or AUTHENTICATE command is in flight.
    /// Consulted by `apply_tagged` to transition to `Authenticated`
    /// on tagged OK (RFC 3501 Section6.2.2, Section6.2.3).
    in_auth: bool,
    /// Set to `Some(mailbox)` while a SELECT or EXAMINE command is
    /// in flight. Consulted by `apply_tagged` to transition to
    /// `Selected` on tagged OK, or back to `Authenticated` on tagged
    /// NO (RFC 3501 Section6.3.1-Section6.3.2).
    in_select: Option<MailboxName>,
    /// `true` while a CLOSE or UNSELECT command is in flight.
    /// Consulted by `apply_tagged` to transition to `Authenticated`
    /// on tagged OK (RFC 3501 Section6.4.2, RFC 9051 Section6.4.2).
    in_close: bool,
    /// Set to `Some(flags)` while a NOTIFY SET or NOTIFY NONE command
    /// is in flight. Consulted by `apply_tagged` to update
    /// `self.notify` on tagged OK (RFC 5465 Section3). Cleared by both
    /// `apply_tagged` (on completion) and `apply_untagged` (on
    /// NOTIFICATIONOVERFLOW, which takes precedence over the pending
    /// registration).
    in_notify_set: Option<NotifyFlags>,
    /// `true` while an UNAUTHENTICATE command is in flight.
    /// Consulted by `apply_tagged` to transition to `NotAuthenticated`
    /// on tagged OK (RFC 8437 Section2).
    in_unauthenticate: bool,
    /// Set when a tagged OK completed a LOGIN / AUTHENTICATE or
    /// UNAUTHENTICATE transition; taken by the driver, which owes that
    /// transition its capability refresh before it answers.
    transition: Option<SessionTransition>,
}

/// A session transition that a tagged OK just completed.
///
/// `capabilities_supplied` is `true` only when that same tagged OK carried a
/// `[CAPABILITY ...]` code (RFC 3501 Section 6.2.2). A capability list seen
/// earlier in the exchange does not count: an untagged CAPABILITY can arrive
/// before the credentials do, and describes the session before the
/// transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::connection) struct SessionTransition {
    pub(in crate::connection) capabilities_supplied: bool,
}

impl ProtocolState {
    pub(crate) fn new() -> Self {
        Self {
            state: SessionState::NotAuthenticated,
            capabilities: Vec::new(),
            notify: NotifyFlags::default(),
            selected: None,
            mailbox_selected: false,
            enabled: Vec::new(),
            in_logout: None,
            in_auth: false,
            in_select: None,
            in_close: false,
            in_notify_set: None,
            in_unauthenticate: false,
            transition: None,
        }
    }

    /// Take the transition the last tagged OK completed, if any.
    pub(in crate::connection) fn take_transition(&mut self) -> Option<SessionTransition> {
        self.transition.take()
    }

    // -- Read-only getters --

    /// Current session state (RFC 3501 Section3).
    pub(crate) fn session_state(&self) -> SessionState {
        self.state
    }

    /// Cached server capabilities (RFC 3501 Section7.2.1).
    pub(crate) fn capabilities(&self) -> &[Capability] {
        &self.capabilities
    }

    /// Current NOTIFY registration flags (RFC 5465 Section5.1-5.8).
    pub(crate) fn notify(&self) -> NotifyFlags {
        self.notify
    }

    /// Successfully `ENABLE`d extensions (RFC 5161 Section3.2).
    pub(crate) fn enabled(&self) -> &[String] {
        &self.enabled
    }

    /// Whether any mailbox has been selected in this session (RFC 5161
    /// Section 3.1). See the field.
    pub(crate) fn mailbox_selected(&self) -> bool {
        self.mailbox_selected
    }

    /// Build a read-only snapshot for the `watch::Sender` channel.
    ///
    /// Called by the driver task after every command completes to publish
    /// the latest state to observers.
    pub(in crate::connection) fn snapshot(&self) -> super::driver::ConnectionStateSnapshot {
        super::driver::ConnectionStateSnapshot {
            session_state: self.state,
            capabilities: self.capabilities.clone(),
            enabled: self.enabled.clone(),
            mailbox_selected: self.mailbox_selected,
            selected: self.selected.clone(),
        }
    }

    // -- Lifecycle mutators --
    //
    // These handle greeting processing, infrastructure failures, and
    // explicit capability fetches. Together with `apply_side_effects`
    // they form the complete set of state mutators.

    /// Process the server greeting (RFC 3501 Section7.1).
    ///
    /// Sets the session state based on the greeting status:
    /// - `OK` -> `NotAuthenticated`
    /// - `PREAUTH` -> `Authenticated`
    /// - `BYE` -> `Logout` + returns `Err(Error::Bye { .. })`
    ///
    /// Extracts `[CAPABILITY]` from the response code if present.
    /// Returns `Ok(Some(text))` if the greeting carried an `[ALERT]`,
    /// `Ok(None)` otherwise.
    pub(in crate::connection) fn apply_greeting(
        &mut self,
        g: &GreetingResponse,
    ) -> Result<Option<String>, Error> {
        match g.status {
            GreetingStatus::Ok => {
                // RFC 3501 Section3.1: Not Authenticated state.
                self.state = SessionState::NotAuthenticated;
            }
            GreetingStatus::PreAuth => {
                // RFC 3501 Section3.2: PREAUTH -> Authenticated state.
                self.state = SessionState::Authenticated;
            }
            GreetingStatus::Bye => {
                // RFC 3501 Section3.4: BYE greeting  -  server refusing connection.
                // RFC 3501 Section7.1.5 + RFC 5530: preserve response code.
                self.state = SessionState::Logout;
                return Err(Error::bye_with_code(g.text.clone(), g.code.clone()));
            }
        }

        // RFC 3501 Section7.2.1: greeting may carry [CAPABILITY] in the
        // response code  -  cache it to avoid an extra round-trip.
        if let Some(ResponseCode::Capability(caps)) = &g.code {
            self.capabilities.clone_from(caps);
        }

        // RFC 3501 Section7.1: [ALERT] in the greeting must be presented
        // to the user.
        if g.code == Some(ResponseCode::Alert) {
            return Ok(Some(g.text.clone()));
        }

        Ok(None)
    }

    /// Transition to `Logout` after an infrastructure failure.
    ///
    /// Used for TLS handshake failures, poisoned streams, and buffer
    /// violations where the connection is irrecoverably dead.
    pub(in crate::connection) fn apply_infrastructure_failure(&mut self) {
        self.state = SessionState::Logout;
    }

    /// Replace the cached capability set (RFC 3501 Section7.2.1).
    ///
    /// Used for explicit CAPABILITY command output and post-upgrade
    /// re-fetches (RFC 3501 Section6.2.1).
    pub(in crate::connection) fn apply_capability_fetch(&mut self, caps: Vec<Capability>) {
        self.capabilities = caps;
    }

    // -- Scoped setters --
    //
    // Used by the driver task to signal in-flight command context so
    // that `apply_side_effects` can interpret state transitions
    // correctly. These do NOT mutate session state or capabilities
    // directly  -  they set flags that `apply_side_effects` reads.

    /// Mark a LOGOUT command as in-flight.
    pub(crate) fn set_in_logout(&mut self, tag: String) {
        self.in_logout = Some(tag);
    }

    /// Mark a LOGIN or AUTHENTICATE command as in-flight.
    ///
    /// Consulted by `apply_tagged` to transition to `Authenticated` on
    /// tagged OK (RFC 3501 Section6.2.2, Section6.2.3).
    pub(crate) fn set_in_auth(&mut self, val: bool) {
        self.in_auth = val;
    }

    /// Mark a SELECT or EXAMINE command as in-flight with the target
    /// mailbox.
    ///
    /// Consulted by `apply_tagged` to transition to `Selected` on
    /// tagged OK, or back to `Authenticated` on tagged NO
    /// (RFC 3501 Section6.3.1-Section6.3.2).
    pub(crate) fn set_in_select(&mut self, mailbox: Option<MailboxName>) {
        self.in_select = mailbox;
    }

    /// Mark a CLOSE or UNSELECT command as in-flight.
    ///
    /// Consulted by `apply_tagged` to transition to `Authenticated`
    /// on tagged OK (RFC 3501 Section6.4.2, RFC 9051 Section6.4.2).
    pub(crate) fn set_in_close(&mut self, val: bool) {
        self.in_close = val;
    }

    /// Mark a NOTIFY SET or NOTIFY NONE command as in-flight with the
    /// target notify flags.
    ///
    /// Consulted by `apply_tagged` to update `self.notify` on tagged OK
    /// (RFC 5465 Section3). Cleared by `apply_untagged` on
    /// NOTIFICATIONOVERFLOW (RFC 5465 Section5.8)  -  overflow takes precedence
    /// over the pending registration.
    pub(crate) fn set_in_notify_set(&mut self, flags: Option<NotifyFlags>) {
        self.in_notify_set = flags;
    }

    /// Mark an UNAUTHENTICATE command as in-flight.
    ///
    /// Consulted by `apply_tagged` to transition to `NotAuthenticated`
    /// on tagged OK (RFC 8437 Section2).
    pub(crate) fn set_in_unauthenticate(&mut self, val: bool) {
        self.in_unauthenticate = val;
    }

    /// Disarm every pending command effect armed by the setters above.
    ///
    /// For a command the server completed WITHOUT executing it: its own
    /// tagged `OK` arrived before the `+` its synchronizing literal was owed
    /// (RFC 3501 Section 4.3), so the literal - a LOGIN credential, a SELECT
    /// or NOTIFY mailbox name - never reached the server. Applying that `OK`
    /// with the effects still armed would authenticate, select or register a
    /// command that never ran. Called BEFORE `apply_side_effects` on that
    /// response, so the response's own codes (CAPABILITY,
    /// NOTIFICATIONOVERFLOW) still apply; only the command's transitions are
    /// withheld. A `NO`/`BAD` must not take this path: a refused SELECT
    /// legitimately deselects (RFC 3501 Section 6.3.1).
    pub(in crate::connection) fn disarm_pending_effects(&mut self) {
        self.in_logout = None;
        self.in_auth = false;
        self.in_select = None;
        self.in_close = false;
        self.in_notify_set = None;
        self.in_unauthenticate = false;
    }

    /// The SINGLE mutator. Called exclusively from the wire-reading
    /// wrapper in `ImapConnection`. Every protocol state transition
    /// happens here.
    ///
    /// RFC 3501 Section7.1: response codes in both tagged and untagged
    /// responses carry side effects (ALERT, CAPABILITY,
    /// NOTIFICATIONOVERFLOW).
    pub(crate) fn apply_side_effects(&mut self, resp: &Response) -> SideEffectDigest {
        let mut digest = SideEffectDigest::default();
        match resp {
            Response::Untagged(u) => {
                self.apply_untagged(u, &mut digest);
            }
            Response::Tagged(t) => {
                self.apply_tagged(t);
            }
            Response::Continuation(_) | Response::Greeting(_) => {}
        }
        digest
    }

    // -- All private helpers below. None are pub(crate). --

    /// Handle side effects from untagged responses.
    ///
    /// The BYE state transition (RFC 3501 Section7.1.5) is handled first and
    /// independently of the response code. Each arm of the code-dispatch
    /// match then handles one of:
    /// - CAPABILITY refresh (RFC 3501 Section7.2.1)
    /// - NOTIFICATIONOVERFLOW clear (RFC 5465 Section5.8)
    /// - ENABLED accumulation (RFC 5161 Section3.2)
    fn apply_untagged(&mut self, u: &UntaggedResponse, digest: &mut SideEffectDigest) {
        // RFC 3501 Section7.1.5: BYE is fatal regardless of which response
        // code it carries. Detect it from the status tag alone, before the
        // code-dispatch match below, so a code with its own arm (e.g.
        // `* BYE [CAPABILITY ...]`) cannot shadow the shutdown.
        if let UntaggedResponse::Status {
            status: UntaggedStatus::Bye,
            ..
        } = u
        {
            self.state = SessionState::Logout;
            digest.had_bye = true;
        }

        match u {
            // RFC 3501 Section7.2.1: server MAY send unsolicited
            // `* CAPABILITY ...` at any time  -  update cached set.
            // RFC 3501 Section7.1: the equivalent `* OK [CAPABILITY ...]`
            // form also updates capabilities. These are server advertisements,
            // adopted on arrival even if the enclosing command later fails.
            UntaggedResponse::Capability(caps)
            | UntaggedResponse::Status {
                code: Some(ResponseCode::Capability(caps)),
                ..
            } => {
                self.capabilities.clone_from(caps);
            }
            // RFC 5465 Section5.8: NOTIFICATIONOVERFLOW means the server
            // dropped the NOTIFY registration. Clear all per-type flags.
            UntaggedResponse::Status {
                code: Some(ResponseCode::NotificationOverflow(_)),
                ..
            } => {
                warn!(
                    "server sent NOTIFICATIONOVERFLOW  -  NOTIFY registration \
                     cleared (RFC 5465 Section5.8)"
                );
                self.notify = NotifyFlags::default();
                // Cancel any pending NOTIFY SET  -  overflow takes precedence
                // over the new registration (RFC 5465 Section5.8).
                self.in_notify_set = None;
            }
            // RFC 5161 Section3.2: `* ENABLED <ext1> <ext2> ...` updates the
            // set of active extensions. Accumulates across multiple ENABLE
            // commands with case-insensitive dedup. Adoption is immediate even
            // if ENABLE later fails: the server has announced a wire-mode
            // change, and rolling it back could misdecode later responses.
            UntaggedResponse::Enabled(exts) => {
                for ext in exts {
                    if !self.enabled.iter().any(|e| e.eq_ignore_ascii_case(ext)) {
                        self.enabled.push(ext.clone());
                    }
                }
                // Note: ENABLED is orthogonal to capabilities. The enabled
                // set is published via ConnectionStateSnapshot automatically.
            }
            _ => {}
        }
    }

    /// Handle side effects from tagged response codes.
    ///
    /// RFC 3501 Section7.1: tagged OK/NO/BAD may carry response codes.
    /// Also handles the LOGOUT state transition when `in_logout` is set.
    fn apply_tagged(&mut self, t: &TaggedResponse) {
        match &t.code {
            // RFC 3501 Section7.1 / Section7.2.1: "[CAPABILITY ...]" updates the
            // cached capability set.
            Some(ResponseCode::Capability(caps)) => {
                self.capabilities.clone_from(caps);
            }
            // RFC 5465 Section5.8: "[NOTIFICATIONOVERFLOW]" means the server
            // dropped the NOTIFY registration. Clear all per-type flags.
            Some(ResponseCode::NotificationOverflow(_)) => {
                warn!(
                    "tagged NOTIFICATIONOVERFLOW  -  NOTIFY registration \
                     cleared (RFC 5465 Section5.8)"
                );
                self.notify = NotifyFlags::default();
                // Cancel any pending NOTIFY SET  -  overflow takes precedence
                // over the new registration (RFC 5465 Section5.8).
                self.in_notify_set = None;
            }
            _ => {}
        }

        // RFC 3501 Section6.1.3: tagged OK for LOGOUT confirms the session
        // has ended. Transition to Logout state. (BYE already did this
        // via apply_untagged; this is a belt-and-suspenders confirmation.)
        // LOGOUT is not pipelinable and the driver drains earlier batches
        // before sending it. Still correlate here: a stray foreign completion
        // must not consume the effect of a different command.
        // Only this command's completion may consume its pending effect. A
        // foreign tagged refusal must not prevent LOGOUT's own OK from closing
        // the session, nor may a foreign OK close it prematurely.
        if self.in_logout.as_deref() == Some(t.tag.as_str()) {
            if t.status == crate::types::response::StatusKind::Ok {
                self.state = SessionState::Logout;
            }
            // A refusal completes this attempt too; a later command's OK
            // must not complete a LOGOUT that the server already rejected.
            self.in_logout = None;
        }

        // RFC 3501 Section6.2.2, Section6.2.3: tagged OK for LOGIN or AUTHENTICATE
        // confirms authentication succeeded. Transition to Authenticated.
        let capabilities_supplied = matches!(t.code, Some(ResponseCode::Capability(_)));
        if self.in_auth && t.status == crate::types::response::StatusKind::Ok {
            self.state = SessionState::Authenticated;
            self.in_auth = false;
            self.transition = Some(SessionTransition {
                capabilities_supplied,
            });
        } else if self.in_auth {
            // Auth failed (NO/BAD)  -  clear the flag without transitioning.
            self.in_auth = false;
        }

        // RFC 3501 Section6.3.1-Section6.3.2: SELECT/EXAMINE state transitions.
        if let Some(mailbox) = self.in_select.take() {
            if t.status == crate::types::response::StatusKind::Ok {
                // Success -> Selected state with the target mailbox.
                self.state = SessionState::Selected;
                self.selected = Some(mailbox);
                self.mailbox_selected = true;
            } else if t.status == crate::types::response::StatusKind::No {
                // RFC 3501 Section6.3.1: NO response deselects any currently
                // selected mailbox.
                self.state = SessionState::Authenticated;
                self.selected = None;
            }
            // BAD -> no state change (RFC 3501 Section6).
        }

        // RFC 3501 Section6.4.2, RFC 9051 Section6.4.2: CLOSE/UNSELECT transitions
        // back to Authenticated on tagged OK.
        if self.in_close && t.status == crate::types::response::StatusKind::Ok {
            self.state = SessionState::Authenticated;
            self.selected = None;
            self.in_close = false;
        } else if self.in_close {
            // NO/BAD  -  clear flag, no state change.
            self.in_close = false;
        }

        // RFC 8437 Section2: UNAUTHENTICATE resets the session to Not Authenticated.
        // All per-user state is cleared. Does NOT expunge deleted messages
        // (UNSELECT semantics, not CLOSE semantics).
        if self.in_unauthenticate && t.status == crate::types::response::StatusKind::Ok {
            self.state = SessionState::NotAuthenticated;
            self.selected = None;
            self.mailbox_selected = false;
            self.notify = NotifyFlags::default();
            self.enabled.clear();
            // Prevent the pending NOTIFY SET block below from applying
            // stale flags after the session has been reset.
            self.in_notify_set = None;
            self.in_unauthenticate = false;
            self.transition = Some(SessionTransition {
                capabilities_supplied,
            });
        } else if self.in_unauthenticate {
            // NO/BAD  -  clear flag, no state change.
            self.in_unauthenticate = false;
        }

        // RFC 5465 Section3: NOTIFY SET/NONE updates the per-type registration
        // flags on tagged OK. NOTIFICATIONOVERFLOW in the tagged response
        // code was already handled above (clears self.notify AND
        // self.in_notify_set), so the take() below returns None in that
        // case  -  no double-write.
        if let Some(pending_flags) = self.in_notify_set.take()
            && t.status == crate::types::response::StatusKind::Ok
        {
            self.notify = pending_flags;
        }
    }
}

/// Summary of side effects applied during a single
/// [`ProtocolState::apply_side_effects`] call.
///
/// Returned to the driver so it can stop response processing on a fatal
/// server shutdown without re-inspecting protocol state.
#[must_use = "route the digest through the driver's BYE short-circuit"]
#[derive(Debug, Default)]
pub(crate) struct SideEffectDigest {
    /// A `* BYE` was received (RFC 3501 Section7.1.5).
    had_bye: bool,
}

impl SideEffectDigest {
    /// Whether the response was an untagged server shutdown.
    pub(super) const fn had_bye(&self) -> bool {
        self.had_bye
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
