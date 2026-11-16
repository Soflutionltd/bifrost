//! Capability profile helpers.

use super::{AuthMechanism, Capability};

/// Server-wide APPENDLIMIT policy advertised in CAPABILITY.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum AppendLimitPolicy {
    /// The server did not advertise APPENDLIMIT.
    #[default]
    NotAdvertised,
    /// Bare `APPENDLIMIT` was advertised; query destination mailbox STATUS for
    /// a mailbox-specific limit.
    PerMailbox {
        /// Numeric ceiling also advertised, applied along with the mailbox limit.
        global_ceiling: Option<u64>,
    },
    /// `APPENDLIMIT=<n>` advertised a server-wide limit in octets.
    Limit(u64),
}

/// Confirmed ENABLE observations, parsed through the capability vocabulary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct EnabledSet(Vec<Capability>);

impl EnabledSet {
    pub(crate) fn contains(&self, capability: &Capability) -> bool {
        self.0.contains(capability)
    }

    pub(crate) fn insert(&mut self, capability: Capability) {
        if !self.contains(&capability) {
            self.0.push(capability);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.0.clear();
    }
    pub(crate) fn as_slice(&self) -> &[Capability] {
        &self.0
    }
}

impl std::ops::Deref for EnabledSet {
    type Target = [Capability];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl FromIterator<Capability> for EnabledSet {
    fn from_iter<T: IntoIterator<Item = Capability>>(iter: T) -> Self {
        let mut enabled = Self::default();
        for capability in iter {
            enabled.insert(capability);
        }
        enabled
    }
}

/// One coherent advertised/confirmed session observation. Snapshots and command
/// views share this value; driver mutations use copy-on-write so queued views
/// retain the exact negotiation state they observed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionCapabilities {
    pub(crate) capabilities: Vec<Capability>,
    pub(crate) enabled: EnabledSet,
}

impl SessionCapabilities {
    #[cfg(test)]
    pub(crate) fn empty() -> &'static Self {
        static EMPTY: std::sync::LazyLock<SessionCapabilities> =
            std::sync::LazyLock::new(SessionCapabilities::default);
        &EMPTY
    }

    pub(crate) fn new(capabilities: Vec<Capability>, enabled: Vec<String>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            capabilities,
            enabled: enabled
                .iter()
                .map(|s| Capability::from_imap_str(s))
                .collect(),
        })
    }

    pub(crate) fn supports(&self, capability: &Capability) -> bool {
        supports(&self.capabilities, self.enabled.as_slice(), capability)
    }

    pub(crate) fn imap4rev2_active(&self) -> bool {
        imap4rev2_active(&self.capabilities, self.enabled.as_slice())
    }

    pub(crate) fn utf8_mode(&self) -> crate::types::wire::WireMode {
        if self.imap4rev2_active() || self.enabled.contains(&Capability::Utf8Accept) {
            crate::types::wire::WireMode::Utf8
        } else {
            crate::types::wire::WireMode::ModifiedUtf7
        }
    }

    pub(crate) fn qresync_enabled(&self) -> bool {
        self.enabled.contains(&Capability::QResync)
    }
    pub(crate) fn binary_fetch_usable(&self) -> bool {
        self.supports(&Capability::Binary) || self.imap4rev2_active()
    }

    pub(crate) fn literal_mode(&self) -> crate::types::wire::LiteralMode {
        if self.capabilities.contains(&Capability::LiteralPlus) {
            crate::types::wire::LiteralMode::LiteralPlus
        } else if self.supports(&Capability::LiteralMinus) {
            crate::types::wire::LiteralMode::LiteralMinus
        } else {
            crate::types::wire::LiteralMode::Synchronizing
        }
    }
}

/// Caller-friendly snapshot of server capabilities and enabled extensions.
///
/// This is not a live view. Capabilities and enabled extensions can change
/// after STARTTLS, authentication, and ENABLE, so callers that cross those
/// boundaries should call `server_profile()` again instead of caching an old
/// profile.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerProfile {
    /// Raw advertised capabilities.
    pub(crate) session: std::sync::Arc<crate::types::profile::SessionCapabilities>,
    /// True when IMAP4rev2 behavior is active.
    pub imap4rev2: bool,
    /// Server APPENDLIMIT policy advertised in CAPABILITY.
    pub append_limit: AppendLimitPolicy,
}

impl std::ops::Deref for ServerProfile {
    type Target = crate::types::profile::SessionCapabilities;
    fn deref(&self) -> &Self::Target {
        &self.session
    }
}

#[allow(
    dead_code,
    reason = "Raw connection capability queries extend beyond the account strategy"
)]
impl ServerProfile {
    /// Build a profile from capability and enabled-extension snapshots.
    pub fn new(capabilities: Vec<Capability>, enabled: Vec<String>) -> Self {
        Self::from_session(SessionCapabilities::new(capabilities, enabled))
    }

    pub(crate) fn from_session(session: std::sync::Arc<SessionCapabilities>) -> Self {
        let capabilities = &session.capabilities;
        let imap4rev2 = session.imap4rev2_active();
        // Preserve both constraints on contradictory advertisements. A user
        // expects the size check and the displayed policy to agree, and the
        // most restrictive advertised limit avoids a preventable rejection.
        let has_mailbox_specific_limit = capabilities
            .iter()
            .any(|cap| matches!(cap, Capability::AppendLimit(None)));
        let global_append_limit = capabilities
            .iter()
            .filter_map(|cap| match cap {
                Capability::AppendLimit(Some(limit)) => Some(*limit),
                _ => None,
            })
            .min();
        let append_limit = if has_mailbox_specific_limit {
            AppendLimitPolicy::PerMailbox {
                global_ceiling: global_append_limit,
            }
        } else {
            global_append_limit
                .map(AppendLimitPolicy::Limit)
                .unwrap_or_default()
        };
        Self {
            session,
            imap4rev2,
            append_limit,
        }
    }

    /// AUTH mechanism observations are projected from their capability owner.
    pub fn auth_mechanisms(&self) -> impl Iterator<Item = &bifrost_sasl::MechanismName> {
        self.capabilities.iter().filter_map(|cap| match cap {
            Capability::Auth(mechanism) => Some(mechanism),
            _ => None,
        })
    }

    /// THREAD algorithms retain the capability parser's canonical vocabulary.
    pub fn thread_algorithms(&self) -> impl Iterator<Item = &str> {
        self.capabilities.iter().filter_map(|cap| match cap {
            Capability::Thread(algorithm) => Some(&**algorithm),
            _ => None,
        })
    }

    /// Return `true` if the capability is advertised, implied by an advertised
    /// QRESYNC (CONDSTORE only), or folded into active IMAP4rev2.
    pub fn supports(&self, capability: Capability) -> bool {
        self.session.supports(&capability)
    }

    /// Return `true` if the server advertises the SASL mechanism.
    ///
    /// [`AuthMechanism::ImapLogin`] represents the legacy IMAP LOGIN command in
    /// this crate, not the non-standard `AUTH=LOGIN` SASL mechanism. For
    /// clearer call sites, prefer [`supports_sasl_auth`](Self::supports_sasl_auth)
    /// for SASL and [`supports_login_command`](Self::supports_login_command)
    /// for the legacy command.
    pub fn supports_auth(&self, mechanism: AuthMechanism) -> bool {
        if mechanism == AuthMechanism::ImapLogin {
            return self.supports_login_command();
        }
        self.supports_sasl_auth(mechanism)
    }

    /// Return `true` if the server advertises a SASL AUTH mechanism.
    pub fn supports_sasl_auth(&self, mechanism: AuthMechanism) -> bool {
        if mechanism == AuthMechanism::ImapLogin {
            return false;
        }
        self.auth_mechanisms().any(|m| m.known() == Some(mechanism))
    }

    /// Return `true` if the legacy IMAP LOGIN command is available.
    pub fn supports_login_command(&self) -> bool {
        !self.supports(Capability::LoginDisabled)
    }

    /// Return `true` if the extension has been enabled.
    pub fn enabled(&self, extension: &str) -> bool {
        match Capability::from_imap_str(extension) {
            Capability::QResync => self.session.qresync_enabled(),
            capability => self.session.enabled.contains(&capability),
        }
    }

    /// Return `true` when UIDPLUS commands are available.
    pub fn supports_uidplus(&self) -> bool {
        self.supports(Capability::UidPlus)
    }

    /// Return `true` when MOVE is available.
    pub fn supports_move(&self) -> bool {
        self.supports(Capability::Move)
    }

    /// Return `true` when CONDSTORE behavior is available.
    pub fn supports_condstore(&self) -> bool {
        self.supports(Capability::Condstore)
    }

    /// Return `true` when QRESYNC is advertised.
    pub fn supports_qresync(&self) -> bool {
        self.supports(Capability::QResync)
    }

    /// Return `true` when UTF8 mode is active or available.
    pub fn utf8_available(&self) -> bool {
        self.imap4rev2
            || self.supports(Capability::Utf8Accept)
            || self.supports(Capability::Utf8Only)
    }

    /// Return `true` when NOTIFY is available.
    pub fn supports_notify(&self) -> bool {
        self.supports(Capability::Notify)
    }

    /// Return `true` when COMPRESS=DEFLATE is available.
    pub fn supports_compress(&self) -> bool {
        self.supports(Capability::CompressDeflate)
    }
}

/// Whether the RFC 9051 IMAP4rev2 baseline folds `capability` into the base
/// protocol, so an active rev2 connection may use it without the server
/// advertising the token.
///
/// This is THE list, and it is private to this module on purpose: every
/// consumer asks [`supports`] instead, which combines it with the rev2-ACTIVE
/// rule and the QRESYNC special case. Extensions the server may advertise but
/// which rev2 did not fold in (CONDSTORE, QRESYNC, SORT, THREAD, WITHIN,
/// PREVIEW, NOTIFY, MULTIAPPEND, ...) are deliberately absent.
///
/// LITERAL+ is among the absent ones. RFC 9051 Appendix E folds in LITERAL-
/// only: a rev2 server accepts non-synchronizing literals up to 4096 octets
/// (RFC 9051 Section 4.3), and unbounded ones only when it ALSO advertises
/// LITERAL+ (RFC 7888 Section 4). Listing LITERAL+ here would tell every
/// `supports` caller that a pure rev2 server takes a `{N+}` literal of any
/// size, which such a server is entitled to reject.
///
/// OBJECTID (RFC 8474) and SAVEDATE (RFC 8514) are absent too: RFC 9051
/// defines none of their FETCH items, SEARCH keys, STATUS items or response
/// codes, so a rev2 server that does not advertise them owes none of it.
///
/// So is BINARY, because rev2 folds in only HALF of it. RFC 9051 Appendix B:
/// IMAP4rev2 includes the BINARY FETCH items (`BINARY`, `BINARY.PEEK`,
/// `BINARY.SIZE`) but not RFC 3516's extension to APPEND, and a server with
/// full RFC 3516 support must still advertise BINARY. The capability token
/// stands for the whole extension, APPEND side included, so answering it from
/// rev2 would let an APPEND carry a NUL-bearing `literal8` body a rev2 server
/// is entitled to reject. The FETCH half has its own question,
/// [`binary_fetch_usable`].
fn rev2_baseline_includes(capability: &Capability) -> bool {
    match capability {
        Capability::Enable
        | Capability::Esearch
        | Capability::Idle
        | Capability::ListExtended
        | Capability::ListStatus
        | Capability::LiteralMinus
        | Capability::Move
        | Capability::Namespace
        | Capability::SaslIr
        | Capability::SearchRes
        | Capability::SpecialUse
        | Capability::StatusDeleted
        | Capability::StatusSize
        | Capability::UidPlus
        | Capability::Unselect => true,
        // RFC 9051 Appendix E folds in LITERAL-, not LITERAL+.
        Capability::LiteralPlus
        // RFC 9051 Appendix B: only the FETCH side of BINARY is base
        // rev2, not its APPEND extension, so the token is not implied.
        | Capability::Binary
        // Nothing of RFC 8474 or RFC 8514 is in RFC 9051.
        | Capability::ObjectId
        | Capability::SaveDate
        | Capability::Imap4Rev1
        | Capability::Imap4Rev2
        | Capability::Acl
        | Capability::AppendLimit(_)
        | Capability::Children
        | Capability::CompressDeflate
        | Capability::Condstore
        | Capability::CreateSpecialUse
        | Capability::Id
        | Capability::LoginDisabled
        | Capability::Metadata
        | Capability::MetadataServer
        | Capability::MultiAppend
        | Capability::Notify
        | Capability::QResync
        | Capability::Quota
        | Capability::QuotaResource(_)
        | Capability::QuotaSet
        | Capability::Rights(_)
        | Capability::Preview
        | Capability::Sort
        | Capability::SortDisplay
        | Capability::StartTls
        | Capability::Thread(_)
        | Capability::Unauthenticate
        | Capability::Utf8Accept
        | Capability::Utf8Only
        | Capability::Within
        | Capability::XGmExt1
        | Capability::Auth(_)
        | Capability::Other(_) => false,
    }
}

/// Whether `capability` is usable on a connection with these advertised
/// capabilities and ENABLEd extensions: the single authority for the question
/// every encoder command gate, connection-handle gate and
/// [`ServerProfile::supports`] asks.
///
/// Usable means any of:
/// - the server advertises it;
/// - it is the CONDSTORE capability and the server advertises QRESYNC (RFC 7162
///   Section 3.2.3: QRESYNC implies CONDSTORE);
/// - IMAP4rev2 is active ([`imap4rev2_active`]) and the rev2 baseline folds it
///   in (RFC 9051 Appendix E). Extensions outside that baseline get no rev2
///   clause.
///
/// Session views call this through `SessionCapabilities::supports`.
fn supports(capabilities: &[Capability], enabled: &[Capability], capability: &Capability) -> bool {
    if capabilities.contains(capability) {
        return true;
    }
    if matches!(capability, Capability::Condstore) && capabilities.contains(&Capability::QResync) {
        return true;
    }
    rev2_baseline_includes(capability) && imap4rev2_active(capabilities, enabled)
}

/// Whether the BINARY FETCH items (`BINARY[...]`, `BINARY.PEEK[...]`,
/// `BINARY.SIZE[...]`, RFC 3516 Section 4.2) may be requested: the server
/// advertises BINARY, or IMAP4rev2 is active, whose base protocol carries
/// those items (RFC 9051 Section 6.4.5, Appendix B).
///
/// Deliberately a separate question from `supports(Binary)`, which stays
/// advertised-only on rev2 because rev2 did not fold in RFC 3516's APPEND
/// side (see [`rev2_baseline_includes`]).
#[cfg(test)]
fn binary_fetch_usable(capabilities: &[Capability], enabled: &[Capability]) -> bool {
    supports(capabilities, enabled, &Capability::Binary) || imap4rev2_active(capabilities, enabled)
}

/// Whether IMAP4rev2 behaviour is active: the single authority for the RFC 9051
/// Section6.3.1 dual-mode rule.
///
/// A server advertising BOTH revisions has not committed to either until the
/// client says which it wants, so rev2 behaviour there requires an explicit
/// `ENABLE IMAP4rev2`. A server advertising only rev2 is in rev2 from the start.
///
fn imap4rev2_active(capabilities: &[Capability], enabled: &[Capability]) -> bool {
    let has_rev2 = capabilities.contains(&Capability::Imap4Rev2);
    let has_rev1 = capabilities.contains(&Capability::Imap4Rev1);
    if has_rev2 && has_rev1 {
        enabled.contains(&Capability::Imap4Rev2)
    } else {
        has_rev2
    }
}

/// Shared oracle for tests of every view over [`supports`] (the profile, the
/// encoder options and the connection snapshot).
///
/// `expected_usable` combines advertised support and the QRESYNC implication
/// with the production rev2 baseline decision. The exhaustive Capability match
/// belongs to `rev2_baseline_includes`, so a new variant requires a production
/// decision even when no capability-matrix test runs.
#[cfg(test)]
pub(crate) mod capability_matrix {
    use super::{Capability, imap4rev2_active, rev2_baseline_includes};

    /// One sample of every [`Capability`] variant (parameterised variants get a
    /// representative payload).
    pub(crate) fn every_capability() -> Vec<Capability> {
        vec![
            Capability::Imap4Rev1,
            Capability::Imap4Rev2,
            Capability::Acl,
            Capability::AppendLimit(None),
            Capability::AppendLimit(Some(1024)),
            Capability::Binary,
            Capability::Children,
            Capability::CompressDeflate,
            Capability::Condstore,
            Capability::CreateSpecialUse,
            Capability::Enable,
            Capability::Esearch,
            Capability::Id,
            Capability::Idle,
            Capability::ListExtended,
            Capability::ListStatus,
            Capability::LiteralPlus,
            Capability::LoginDisabled,
            Capability::LiteralMinus,
            Capability::Metadata,
            Capability::MetadataServer,
            Capability::Move,
            Capability::MultiAppend,
            Capability::Namespace,
            Capability::Notify,
            Capability::ObjectId,
            Capability::QResync,
            Capability::Quota,
            Capability::from_imap_str("QUOTA=RES-STORAGE"),
            Capability::QuotaSet,
            Capability::from_imap_str("RIGHTS=texk"),
            Capability::Preview,
            Capability::SaslIr,
            Capability::SaveDate,
            Capability::SearchRes,
            Capability::Sort,
            Capability::SortDisplay,
            Capability::StartTls,
            Capability::SpecialUse,
            Capability::from_imap_str("THREAD=REFERENCES"),
            Capability::StatusSize,
            Capability::StatusDeleted,
            Capability::UidPlus,
            Capability::Unauthenticate,
            Capability::Unselect,
            Capability::Utf8Accept,
            Capability::Utf8Only,
            Capability::Within,
            Capability::XGmExt1,
            Capability::from_imap_str("AUTH=PLAIN"),
            Capability::from_imap_str("X-VENDOR"),
        ]
    }

    /// The expected answer for `capability` given the pair, from first
    /// principles: advertised, or QRESYNC for CONDSTORE, or baseline under
    /// active rev2.
    pub(crate) fn expected_usable(
        capabilities: &[Capability],
        enabled: &[String],
        capability: &Capability,
    ) -> bool {
        capabilities.contains(capability)
            || (matches!(capability, Capability::Condstore)
                && capabilities.contains(&Capability::QResync))
            || (rev2_baseline_includes(capability)
                && imap4rev2_active(
                    capabilities,
                    &enabled
                        .iter()
                        .map(|s| Capability::from_imap_str(s))
                        .collect::<Vec<_>>(),
                ))
    }

    /// Connection states that exercise every branch of the rev2-ACTIVE rule.
    pub(crate) fn connection_states() -> Vec<(Vec<Capability>, Vec<String>)> {
        let en = || vec!["IMAP4rev2".to_owned()];
        vec![
            (vec![], vec![]),
            (vec![Capability::Imap4Rev1], vec![]),
            (vec![Capability::Imap4Rev2], vec![]),
            (vec![Capability::Imap4Rev1, Capability::Imap4Rev2], vec![]),
            (vec![Capability::Imap4Rev1, Capability::Imap4Rev2], en()),
            (vec![Capability::Imap4Rev1], en()),
        ]
    }
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
