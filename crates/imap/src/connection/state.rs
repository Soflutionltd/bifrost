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
//! Tag-keyed armed effects signal in-flight
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
pub(crate) struct ProtocolState {
    /// Current session state (RFC 3501 Section3).
    state: SessionState,
    /// Driver exchange evidence, independent of protocol error classification.
    /// Reset at request admission; sends leave it unfinished until ReadLoop
    /// observes the terminal completion. Logout remains an independent veto.
    pub(in crate::connection) framing: super::driver::read_loop::Framing,
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
    /// One effect per command tag, shared by regular and pipeline dispatch.
    pending: std::collections::HashMap<String, PendingEffect>,
    /// Set when a tagged OK completed a LOGIN / AUTHENTICATE or
    /// UNAUTHENTICATE transition; taken by the driver, which owes that
    /// transition its capability refresh before it answers.
    transition: Option<SessionTransition>,
}

/// Foreign completions cannot consume these; every own completion consumes one.
#[derive(Debug)]
enum PendingEffect {
    Logout,
    Authenticate,
    Select(MailboxName),
    Close,
    Unauthenticate,
    Notify(NotifyFlags),
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
            framing: super::driver::read_loop::Framing::Untouched,
            capabilities: Vec::new(),
            notify: NotifyFlags::default(),
            selected: None,
            mailbox_selected: false,
            enabled: Vec::new(),
            pending: std::collections::HashMap::new(),
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
        self.pending.clear();
    }

    /// Replace the cached capability set (RFC 3501 Section7.2.1).
    ///
    /// Used for explicit CAPABILITY command output and post-upgrade
    /// re-fetches (RFC 3501 Section6.2.1).
    pub(in crate::connection) fn apply_capability_fetch(&mut self, caps: Vec<Capability>) {
        self.capabilities = caps;
    }

    /// Arm this command's effect after preparation and caller commitment.
    pub(crate) fn arm_effect(&mut self, command: &crate::types::Command, tag: &str) {
        use crate::types::Command;
        let effect = match command {
            Command::Logout => PendingEffect::Logout,
            Command::Login { .. } | Command::Authenticate { .. } => PendingEffect::Authenticate,
            Command::Select { mailbox, .. } | Command::Examine { mailbox, .. } => {
                PendingEffect::Select(mailbox.clone())
            }
            Command::Close | Command::Unselect => PendingEffect::Close,
            Command::Unauthenticate => PendingEffect::Unauthenticate,
            Command::NotifySet(params) => PendingEffect::Notify(NotifyFlags::from(params)),
            Command::NotifyNone => PendingEffect::Notify(NotifyFlags::default()),
            _ => return,
        };
        self.pending.insert(tag.to_owned(), effect);
    }

    /// Own OK before a required literal grant means this command never ran.
    /// Remove only its effect: earlier pipelined commands still owe completions.
    /// NO/BAD must retain SELECT's legitimate deselection behavior.
    pub(in crate::connection) fn disarm_effect(&mut self, tag: &str) {
        self.pending.remove(tag);
    }

    /// Overflow cancels registrations, never unrelated session transitions.
    fn cancel_pending_notify(&mut self) {
        self.pending
            .retain(|_, effect| !matches!(effect, PendingEffect::Notify(_)));
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
                self.cancel_pending_notify();
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
    /// Also consumes the pending effect belonging to this exact tag.
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
                self.cancel_pending_notify();
            }
            _ => {}
        }

        // Consume on OK, NO and BAD alike. Foreign tags change no command
        // effect, although their advertised response codes still apply.
        let Some(effect) = self.pending.remove(&t.tag) else {
            return;
        };
        let ok = t.status == crate::types::response::StatusKind::Ok;
        let capabilities_supplied = matches!(t.code, Some(ResponseCode::Capability(_)));
        match effect {
            PendingEffect::Logout if ok => self.state = SessionState::Logout,
            PendingEffect::Authenticate if ok => {
                self.state = SessionState::Authenticated;
                self.transition = Some(SessionTransition {
                    capabilities_supplied,
                });
            }
            PendingEffect::Select(mailbox) => {
                if ok {
                    self.state = SessionState::Selected;
                    self.selected = Some(mailbox);
                    self.mailbox_selected = true;
                } else if t.status == crate::types::response::StatusKind::No {
                    // RFC 3501 Section 6.3.1: NO deselects, BAD does not.
                    self.state = SessionState::Authenticated;
                    self.selected = None;
                }
            }
            PendingEffect::Close if ok => {
                self.state = SessionState::Authenticated;
                self.selected = None;
            }
            PendingEffect::Unauthenticate if ok => {
                self.state = SessionState::NotAuthenticated;
                self.selected = None;
                self.mailbox_selected = false;
                self.notify = NotifyFlags::default();
                self.enabled.clear();
                self.pending.clear();
                self.transition = Some(SessionTransition {
                    capabilities_supplied,
                });
            }
            PendingEffect::Notify(flags) if ok => self.notify = flags,
            _ => {}
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
