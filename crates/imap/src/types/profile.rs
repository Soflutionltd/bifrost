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
    pub capabilities: Vec<Capability>,
    /// Extensions successfully enabled with ENABLE.
    pub enabled: Vec<String>,
    /// True when IMAP4rev2 behavior is active.
    pub imap4rev2: bool,
    /// AUTH mechanisms advertised by the server.
    pub auth_mechanisms: Vec<String>,
    /// Server APPENDLIMIT policy advertised in CAPABILITY.
    pub append_limit: AppendLimitPolicy,
    /// Advertised THREAD algorithms.
    pub thread_algorithms: Vec<String>,
}

impl ServerProfile {
    /// Build a profile from capability and enabled-extension snapshots.
    pub fn new(capabilities: Vec<Capability>, enabled: Vec<String>) -> Self {
        let imap4rev2 = imap4rev2_active(&capabilities, &enabled);
        let auth_mechanisms = capabilities
            .iter()
            .filter_map(|cap| match cap {
                Capability::Auth(mechanism) => Some(mechanism.to_ascii_uppercase()),
                _ => None,
            })
            .collect();
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
        let thread_algorithms = capabilities
            .iter()
            .filter_map(|cap| match cap {
                Capability::Thread(algorithm) => Some(algorithm.to_ascii_uppercase()),
                _ => None,
            })
            .collect();

        Self {
            capabilities,
            enabled,
            imap4rev2,
            auth_mechanisms,
            append_limit,
            thread_algorithms,
        }
    }

    /// Return `true` if the capability is advertised, implied by an advertised
    /// QRESYNC (CONDSTORE only), or folded into active IMAP4rev2.
    pub fn supports(&self, capability: Capability) -> bool {
        supports(&self.capabilities, &self.enabled, &capability)
    }

    /// Return `true` if the server advertises the SASL mechanism.
    ///
    /// [`AuthMechanism::Login`] represents the legacy IMAP LOGIN command in
    /// this crate, not the non-standard `AUTH=LOGIN` SASL mechanism. For
    /// clearer call sites, prefer [`supports_sasl_auth`](Self::supports_sasl_auth)
    /// for SASL and [`supports_login_command`](Self::supports_login_command)
    /// for the legacy command.
    pub fn supports_auth(&self, mechanism: AuthMechanism) -> bool {
        if mechanism == AuthMechanism::Login {
            return self.supports_login_command();
        }
        self.supports_sasl_auth(mechanism)
    }

    /// Return `true` if the server advertises a SASL AUTH mechanism.
    pub fn supports_sasl_auth(&self, mechanism: AuthMechanism) -> bool {
        if mechanism == AuthMechanism::Login {
            return false;
        }
        self.auth_mechanisms
            .iter()
            .any(|m| m.eq_ignore_ascii_case(mechanism.name()))
    }

    /// Return `true` if the legacy IMAP LOGIN command is available.
    pub fn supports_login_command(&self) -> bool {
        !self.supports(Capability::LoginDisabled)
    }

    /// Return `true` if the extension has been enabled.
    pub fn enabled(&self, extension: &str) -> bool {
        self.enabled
            .iter()
            .any(|enabled| enabled.eq_ignore_ascii_case(extension))
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
    matches!(
        capability,
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
            | Capability::Unselect
    )
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
/// The same warning as [`imap4rev2_active`] applies: pass a coherent
/// `(capabilities, enabled)` pair from one owner, preferably through a typed
/// wrapper.
pub(crate) fn supports(
    capabilities: &[Capability],
    enabled: &[String],
    capability: &Capability,
) -> bool {
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
pub(crate) fn binary_fetch_usable(capabilities: &[Capability], enabled: &[String]) -> bool {
    supports(capabilities, enabled, &Capability::Binary) || imap4rev2_active(capabilities, enabled)
}

/// Whether IMAP4rev2 behaviour is active: the single authority for the RFC 9051
/// Section6.3.1 dual-mode rule.
///
/// A server advertising BOTH revisions has not committed to either until the
/// client says which it wants, so rev2 behaviour there requires an explicit
/// `ENABLE IMAP4rev2`. A server advertising only rev2 is in rev2 from the start.
///
/// This lives here, over a slice pair, rather than on any one of the state
/// types that hold those slices, because the rule's real input is exactly
/// `(capabilities, enabled)` and four different views own that pair -
/// `ProtocolState`, `ConnectionStateSnapshot`, `EncodeOptions` and
/// `ServerProfile`. It used to be written out five times over those four views,
/// and the two copies that had tests were not the three that decide wire bytes
/// (modified UTF-7 versus raw UTF-8, synchronizing versus non-synchronizing
/// literals) and SELECT validation.
///
/// CALL IT THROUGH A TYPED WRAPPER unless you are adapting one coherent owner.
/// The slice pair is deliberately low-level and nothing stops a caller pairing
/// one view's capabilities with another view's enabled list; the wrappers exist
/// so that ordinary call sites cannot.
pub(crate) fn imap4rev2_active(capabilities: &[Capability], enabled: &[String]) -> bool {
    let has_rev2 = capabilities.contains(&Capability::Imap4Rev2);
    let has_rev1 = capabilities.contains(&Capability::Imap4Rev1);
    if has_rev2 && has_rev1 {
        enabled
            .iter()
            .any(|extension| extension.eq_ignore_ascii_case("IMAP4rev2"))
    } else {
        has_rev2
    }
}

/// Shared oracle for tests of every view over [`supports`] (the profile, the
/// encoder options and the connection snapshot).
///
/// `expected_usable` is written from RFC 9051 Appendix E and RFC 7162 Section
/// 3.2.3 with an EXHAUSTIVE match over [`Capability`], so adding a variant does
/// not compile until someone has decided whether the rev2 baseline folds it in.
/// That is the guard against the failure this authority exists to prevent: a
/// capability added to one list and silently missing from another.
#[cfg(test)]
pub(crate) mod capability_matrix {
    use super::{Capability, imap4rev2_active};

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
            Capability::QuotaResource("STORAGE".to_owned()),
            Capability::QuotaSet,
            Capability::Rights("texk".to_owned()),
            Capability::Preview,
            Capability::SaslIr,
            Capability::SaveDate,
            Capability::SearchRes,
            Capability::Sort,
            Capability::SortDisplay("DISPLAY".to_owned()),
            Capability::StartTls,
            Capability::SpecialUse,
            Capability::Thread("REFERENCES".to_owned()),
            Capability::StatusSize,
            Capability::StatusDeleted,
            Capability::UidPlus,
            Capability::Unauthenticate,
            Capability::Unselect,
            Capability::Utf8Accept,
            Capability::Utf8Only,
            Capability::Within,
            Capability::XGmExt1,
            Capability::Auth("PLAIN".to_owned()),
            Capability::Other("X-VENDOR".to_owned()),
        ]
    }

    /// Whether the RFC 9051 Appendix E baseline folds the capability in. Kept
    /// as an exhaustive match, deliberately independent of the production list.
    fn in_rev2_baseline(capability: &Capability) -> bool {
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
            | Capability::SortDisplay(_)
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
            || (in_rev2_baseline(capability) && imap4rev2_active(capabilities, enabled))
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
