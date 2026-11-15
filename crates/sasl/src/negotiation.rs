//! Protocol-neutral authentication vocabulary and local selection policy.

use crate::{ChannelBinding, SaslError, ScramChannelBinding, ScramHash};

/// Authentication vocabulary. IMAP LOGIN is deliberately distinct from SASL LOGIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Mechanism {
    Plain,
    Login,
    ImapLogin,
    Xoauth2,
    OAuthBearer,
    CramMd5,
    ScramSha1,
    ScramSha256,
    ScramSha1Plus,
    ScramSha256Plus,
}

impl Mechanism {
    /// Canonical wire spelling; IMAP LOGIN names a command, not an AUTH token.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::Login | Self::ImapLogin => "LOGIN",
            Self::Xoauth2 => "XOAUTH2",
            Self::OAuthBearer => "OAUTHBEARER",
            Self::CramMd5 => "CRAM-MD5",
            Self::ScramSha1 => "SCRAM-SHA-1",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha1Plus => "SCRAM-SHA-1-PLUS",
            Self::ScramSha256Plus => "SCRAM-SHA-256-PLUS",
        }
    }

    /// Parse a SASL advertisement, independently of ASCII case.
    pub fn from_name(name: &str) -> Option<Self> {
        PASSWORD_PREFERENCE
            .iter()
            .chain(OAUTH_PREFERENCE)
            .copied()
            .find(|m| *m != Self::ImapLogin && m.name().eq_ignore_ascii_case(name))
    }

    /// Mechanism identity for a SCRAM hash and binding dimension.
    pub const fn from_scram(hash: ScramHash, binding: ChannelBinding) -> Self {
        match (hash, binding) {
            (ScramHash::Sha1, ChannelBinding::None) => Self::ScramSha1,
            (ScramHash::Sha256, ChannelBinding::None) => Self::ScramSha256,
            (ScramHash::Sha1, ChannelBinding::TlsServerEndPoint) => Self::ScramSha1Plus,
            (ScramHash::Sha256, ChannelBinding::TlsServerEndPoint) => Self::ScramSha256Plus,
        }
    }

    /// SCRAM's orthogonal hash and binding dimensions.
    pub const fn scram(self) -> Option<(ScramHash, ChannelBinding)> {
        match self {
            Self::ScramSha1 => Some((ScramHash::Sha1, ChannelBinding::None)),
            Self::ScramSha256 => Some((ScramHash::Sha256, ChannelBinding::None)),
            Self::ScramSha1Plus => Some((ScramHash::Sha1, ChannelBinding::TlsServerEndPoint)),
            Self::ScramSha256Plus => Some((ScramHash::Sha256, ChannelBinding::TlsServerEndPoint)),
            _ => None,
        }
    }
}

/// Bound SCRAM outranks unbound SCRAM; legacy mechanisms require an allow-list.
pub const PASSWORD_PREFERENCE: &[Mechanism] = &[
    Mechanism::ScramSha256Plus,
    Mechanism::ScramSha1Plus,
    Mechanism::ScramSha256,
    Mechanism::ScramSha1,
    Mechanism::Plain,
    Mechanism::CramMd5,
    Mechanism::Login,
    Mechanism::ImapLogin,
];
/// Prefer the standardized bearer mechanism regardless of allow-list order.
pub const OAUTH_PREFERENCE: &[Mechanism] = &[Mechanism::OAuthBearer, Mechanism::Xoauth2];

/// Verified RFC 5929 certificate-derived bytes. Only the resolver can mint these.
#[derive(Debug, Clone)]
pub struct ChannelBindingData(Vec<u8>);

/// Absence alone permits skipping PLUS. A bad certificate is a hard error.
pub fn binding_from_peer_certificate(
    der: Option<&[u8]>,
) -> Result<Option<ChannelBindingData>, SaslError> {
    der.map(crate::tls_server_end_point)
        .transpose()
        .map(|data| data.map(ChannelBindingData))
}

impl ChannelBindingData {
    /// Input for an explicitly selected PLUS exchange.
    pub fn scram_binding(&self) -> ScramChannelBinding {
        ScramChannelBinding::TlsServerEndPoint(self.0.clone())
    }
}

/// A local reason to skip a SCRAM rung, never a server authentication result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScramSkip {
    NotAdvertised,
    PlusWithoutBinding,
    DowngradeForbidden,
}

/// A selected SCRAM mechanism carries its actual GS2 input, without re-selection.
#[derive(Debug, Clone)]
pub struct ScramRung {
    pub mechanism: Mechanism,
    pub binding: ScramChannelBinding,
}

/// Resolve one SCRAM rung with real binding data (RFC 5802 section 6).
pub fn scram_rung(
    mechanism: Mechanism,
    advertised: &impl Fn(Mechanism) -> bool,
    binding: Option<&ChannelBindingData>,
) -> Result<ScramRung, ScramSkip> {
    let Some((hash, channel)) = mechanism.scram() else {
        return Err(ScramSkip::NotAdvertised);
    };
    if !advertised(mechanism) {
        return Err(ScramSkip::NotAdvertised);
    }
    let plus = match hash {
        ScramHash::Sha1 => Mechanism::ScramSha1Plus,
        ScramHash::Sha256 => Mechanism::ScramSha256Plus,
    };
    let binding = match channel {
        ChannelBinding::TlsServerEndPoint => binding
            .ok_or(ScramSkip::PlusWithoutBinding)?
            .scram_binding(),
        ChannelBinding::None if advertised(plus) => return Err(ScramSkip::DowngradeForbidden),
        ChannelBinding::None if binding.is_some() => ScramChannelBinding::SupportedButNotAdvertised,
        ChannelBinding::None => ScramChannelBinding::None,
    };
    Ok(ScramRung { mechanism, binding })
}

/// Local selection policy. The allow-list expresses protocol support and opt-ins.
pub struct NegotiationPolicy<'a> {
    pub allowed: &'a [Mechanism],
    pub allow_insecure: bool,
    pub oauth: bool,
}

/// Local policy refusal; no credential has crossed the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NegotiationSkip {
    Disabled,
    TlsRequired,
    Scram(ScramSkip),
}

#[derive(Debug, Clone)]
pub struct NegotiatedMechanism {
    pub mechanism: Mechanism,
    pub scram: Option<ScramRung>,
}

/// Produce an ordered local ladder. Consumers stop after any wire rejection.
pub fn negotiate(
    advertised: impl Fn(Mechanism) -> bool,
    policy: &NegotiationPolicy<'_>,
    encrypted: bool,
    binding: Option<&ChannelBindingData>,
) -> Vec<(Mechanism, Result<NegotiatedMechanism, NegotiationSkip>)> {
    let order = if policy.oauth {
        OAUTH_PREFERENCE
    } else {
        PASSWORD_PREFERENCE
    };
    order
        .iter()
        .copied()
        .filter(|m| advertised(*m))
        .map(|mechanism| {
            // Enterprise users expect authenticated TLS even when SCRAM hides the
            // password. A proof does not protect subsequent mail or sieve traffic.
            let result = if !authentication_allowed(encrypted, policy.allow_insecure) {
                Err(NegotiationSkip::TlsRequired)
            } else if !policy.allowed.contains(&mechanism) {
                Err(NegotiationSkip::Disabled)
            } else {
                let scram = mechanism
                    .scram()
                    .map(|_| scram_rung(mechanism, &advertised, binding))
                    .transpose()
                    .map_err(NegotiationSkip::Scram);
                scram.map(|scram| NegotiatedMechanism { mechanism, scram })
            };
            (mechanism, result)
        })
        .collect()
}

/// The one cleartext gate for automatic SASL authentication on every protocol,
/// including protocols that check before token lookup.
///
/// TLS is required by default for every mechanism - SCRAM included, which IMAP
/// once allowed over plaintext - and only an explicit caller override
/// (`allow_insecure`) lifts it. Repository owner ruling: an enterprise client
/// should not send credentials or a SCRAM exchange over an unencrypted channel
/// by default. SCRAM hides the password, but the exchange still exposes the
/// account to offline dictionary attack and active downgrade, and a successful
/// proof does nothing to protect the mail or sieve traffic that follows.
pub const fn authentication_allowed(encrypted: bool, allow_insecure: bool) -> bool {
    encrypted || allow_insecure
}

/// A wire mechanism identity, retaining unknown extensions for diagnostics.
/// Framing consumers still validate atom syntax before writing unknown names.
#[derive(Debug, Clone)]
pub struct MechanismName(MechanismNameKind);

#[derive(Debug, Clone)]
enum MechanismNameKind {
    Known(Mechanism),
    Extension(String),
}

impl MechanismName {
    /// Recognized SASL mechanism, excluding the legacy IMAP LOGIN command.
    pub fn known(&self) -> Option<Mechanism> {
        match self.0 {
            MechanismNameKind::Known(m) => Some(m),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &str {
        match &self.0 {
            MechanismNameKind::Known(m) => m.name(),
            MechanismNameKind::Extension(s) => s,
        }
    }
}
impl From<&str> for MechanismName {
    fn from(value: &str) -> Self {
        Self(Mechanism::from_name(value).map_or_else(
            || MechanismNameKind::Extension(value.to_owned()),
            MechanismNameKind::Known,
        ))
    }
}
impl From<String> for MechanismName {
    fn from(value: String) -> Self {
        Self(match Mechanism::from_name(&value) {
            Some(m) => MechanismNameKind::Known(m),
            None => MechanismNameKind::Extension(value),
        })
    }
}
impl std::ops::Deref for MechanismName {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl std::fmt::Display for MechanismName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
impl PartialEq for MechanismName {
    fn eq(&self, other: &Self) -> bool {
        self.eq_ignore_ascii_case(other)
    }
}
impl Eq for MechanismName {}
impl PartialEq<str> for MechanismName {
    fn eq(&self, other: &str) -> bool {
        self.eq_ignore_ascii_case(other)
    }
}
impl PartialEq<&str> for MechanismName {
    fn eq(&self, other: &&str) -> bool {
        self.eq_ignore_ascii_case(other)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn binding() -> ChannelBindingData {
        let der = [
            0x30, 0x0f, 0x30, 0x00, 0x30, 0x0b, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d,
            0x01, 0x01, 0x0b,
        ];
        binding_from_peer_certificate(Some(&der)).unwrap().unwrap()
    }

    #[test]
    fn names_distinguish_sasl_login_from_the_imap_command_and_retain_extensions() {
        for mechanism in PASSWORD_PREFERENCE.iter().chain(OAUTH_PREFERENCE) {
            if *mechanism != Mechanism::ImapLogin {
                assert_eq!(
                    MechanismName::from(mechanism.name().to_ascii_lowercase()).known(),
                    Some(*mechanism)
                );
            }
        }
        assert_ne!(Mechanism::Login, Mechanism::ImapLogin);
        let extension = MechanismName::from("X-CUSTOM");
        assert_eq!(extension.known(), None);
        assert_eq!(extension.as_str(), "X-CUSTOM");
        assert_eq!(extension, MechanismName::from("x-custom"));
    }

    #[test]
    fn plaintext_gate_covers_scram_and_oauth_with_an_explicit_escape_hatch() {
        for oauth in [false, true] {
            let allowed = if oauth {
                OAUTH_PREFERENCE
            } else {
                PASSWORD_PREFERENCE
            };
            let policy = NegotiationPolicy {
                allowed,
                allow_insecure: false,
                oauth,
            };
            let results = negotiate(|_| true, &policy, false, Some(&binding()));
            assert_eq!(results.len(), allowed.len());
            assert!(
                results
                    .iter()
                    .all(|(_, r)| matches!(r, Err(NegotiationSkip::TlsRequired)))
            );
            let policy = NegotiationPolicy {
                allow_insecure: true,
                ..policy
            };
            assert!(
                negotiate(|_| true, &policy, false, Some(&binding()))
                    .iter()
                    .any(|(_, r)| r.is_ok())
            );
        }
    }

    #[test]
    fn bound_sha1_precedes_unbound_sha256_and_carries_real_binding() {
        let data = binding();
        let allowed = [Mechanism::ScramSha256, Mechanism::ScramSha1Plus];
        let policy = NegotiationPolicy {
            allowed: &allowed,
            allow_insecure: false,
            oauth: false,
        };
        let results = negotiate(|m| allowed.contains(&m), &policy, true, Some(&data));
        let selected = results.into_iter().find_map(|(_, r)| r.ok()).unwrap();
        assert_eq!(selected.mechanism, Mechanism::ScramSha1Plus);
        assert_eq!(
            selected.scram.unwrap().binding.gs2_header(),
            "p=tls-server-end-point,,"
        );
        let rung = scram_rung(
            Mechanism::ScramSha256,
            &|m| m == Mechanism::ScramSha256,
            Some(&data),
        )
        .unwrap();
        assert_eq!(rung.binding.gs2_header(), "y,,");
    }

    #[test]
    fn excluding_advertised_plus_does_not_enable_its_unbound_counterpart() {
        let allowed = [Mechanism::ScramSha256];
        let policy = NegotiationPolicy {
            allowed: &allowed,
            allow_insecure: false,
            oauth: false,
        };
        let results = negotiate(
            |m| matches!(m, Mechanism::ScramSha256 | Mechanism::ScramSha256Plus),
            &policy,
            true,
            Some(&binding()),
        );
        assert!(results.iter().all(|(_, r)| r.is_err()));
        assert!(results.iter().any(|(m, r)| *m == Mechanism::ScramSha256
            && matches!(
                r,
                Err(NegotiationSkip::Scram(ScramSkip::DowngradeForbidden))
            )));
    }

    #[test]
    fn oauth_order_is_canonical_but_xoauth2_remains_selectable() {
        let allowed = [Mechanism::Xoauth2, Mechanism::OAuthBearer];
        let policy = NegotiationPolicy {
            allowed: &allowed,
            allow_insecure: false,
            oauth: true,
        };
        let selected = negotiate(|_| true, &policy, true, None)
            .into_iter()
            .find_map(|(_, r)| r.ok())
            .unwrap();
        assert_eq!(selected.mechanism, Mechanism::OAuthBearer);
        let policy = NegotiationPolicy {
            allowed: &[Mechanism::Xoauth2],
            ..policy
        };
        let selected = negotiate(|_| true, &policy, true, None)
            .into_iter()
            .find_map(|(_, r)| r.ok())
            .unwrap();
        assert_eq!(selected.mechanism, Mechanism::Xoauth2);
    }
}
