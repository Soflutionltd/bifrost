//! Provides limited SASL authentication mechanisms

use std::fmt::{self, Debug, Display, Formatter};

use bifrost_sasl::{ScramChannelBinding, ScramHash, Secret};

use crate::transport::smtp::error::{self, Error, SmtpCommandPhase};
use crate::transport::smtp::extension::ServerInfo;
// pub: Credentials exposes zeroizing fields and constructors accept this type.
pub use zeroize::Zeroizing;

/// Accepted password authentication mechanisms, strongest first.
///
/// SCRAM (channel-bound first, then unbound), then PLAIN. LOGIN is **not** in
/// the default set: it is an opt-in legacy mechanism (it sends reusable
/// cleartext credentials), so a caller that wants it must list it explicitly
/// via `authentication(...)`. This mirrors IMAP's `allow_login = false`
/// default. A server advertising only `AUTH LOGIN` therefore yields an empty
/// attempt order under the default and fails with "no compatible mechanism"
/// rather than silently downgrading to LOGIN.
pub(crate) const PASSWORD_MECHANISMS: &[Mechanism] = &[
    Mechanism::ScramSha256Plus,
    Mechanism::ScramSha1Plus,
    Mechanism::ScramSha256,
    Mechanism::ScramSha1,
    Mechanism::Plain,
];

/// Accepted OAuth 2.0 bearer-token authentication mechanisms.
///
/// `OAUTHBEARER` is the standard mechanism. `XOAUTH2` is kept for providers
/// that only expose the older non-standard mechanism.
pub(crate) const OAUTH2_MECHANISMS: &[Mechanism] = &[Mechanism::OAuthBearer, Mechanism::Xoauth2];

/// Default authentication mechanisms.
pub(crate) const DEFAULT_MECHANISMS: &[Mechanism] = PASSWORD_MECHANISMS;

mod credentials;
mod framing;
pub(crate) use framing::{decode_auth_challenge, decode_scram_payload};
// Preserve the published credential paths while separating token-source I/O
// from SMTP challenge framing and mechanism selection.
#[cfg(all(test, feature = "account-error"))]
use credentials::oauth_token_error;
pub use credentials::{Credentials, IntoSecretString};

/// Represents authentication mechanisms
#[derive(PartialEq, Eq, Copy, Clone, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
// pub: users can constrain or inspect the SASL mechanism selection.
pub enum Mechanism {
    /// PLAIN authentication mechanism, defined in
    /// [RFC 4616](https://tools.ietf.org/html/rfc4616)
    Plain,
    /// LOGIN authentication mechanism
    /// Obsolete but needed for some providers (like Office 365)
    ///
    /// Defined in [draft-murchison-sasl-login-00](https://www.ietf.org/archive/id/draft-murchison-sasl-login-00.txt).
    Login,
    /// Non-standard XOAUTH2 mechanism, defined in
    /// [xoauth2-protocol](https://developers.google.com/gmail/imap/xoauth2-protocol)
    Xoauth2,
    /// OAUTHBEARER authentication mechanism, defined in
    /// [RFC 7628](https://www.rfc-editor.org/rfc/rfc7628.html)
    OAuthBearer,
    /// SCRAM-SHA-1, defined in [RFC 5802](https://www.rfc-editor.org/rfc/rfc5802).
    ScramSha1,
    /// SCRAM-SHA-256, defined in [RFC 7677](https://www.rfc-editor.org/rfc/rfc7677).
    ScramSha256,
    /// SCRAM-SHA-1-PLUS (channel-bound), defined in
    /// [RFC 5802](https://www.rfc-editor.org/rfc/rfc5802).
    ScramSha1Plus,
    /// SCRAM-SHA-256-PLUS (channel-bound), defined in
    /// [RFC 7677](https://www.rfc-editor.org/rfc/rfc7677).
    ScramSha256Plus,
}

impl Display for Mechanism {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl Mechanism {
    /// The wire token for this mechanism, exactly as it is written on the
    /// `AUTH` line and advertised in EHLO.
    ///
    /// The `-PLUS` suffix and the SCRAM token spelling have a single
    /// authority in `bifrost-sasl`, so the SMTP wire token can never drift
    /// from the value used in the proof math. This is also the string the
    /// auth-phase error stamp carries into support diagnostics, so the name
    /// an operator reads is the name that went out on the wire.
    pub(crate) fn name(self) -> &'static str {
        self.shared().name()
    }

    /// Adapt the published SMTP allow-list to the shared vocabulary.
    pub(crate) const fn shared(self) -> bifrost_sasl::Mechanism {
        match self {
            Self::Plain => bifrost_sasl::Mechanism::Plain,
            Self::Login => bifrost_sasl::Mechanism::Login,
            Self::Xoauth2 => bifrost_sasl::Mechanism::Xoauth2,
            Self::OAuthBearer => bifrost_sasl::Mechanism::OAuthBearer,
            Self::ScramSha1 => bifrost_sasl::Mechanism::ScramSha1,
            Self::ScramSha256 => bifrost_sasl::Mechanism::ScramSha256,
            Self::ScramSha1Plus => bifrost_sasl::Mechanism::ScramSha1Plus,
            Self::ScramSha256Plus => bifrost_sasl::Mechanism::ScramSha256Plus,
        }
    }

    /// Does the mechanism support initial response?
    pub fn supports_initial_response(self) -> bool {
        match self {
            Mechanism::Plain | Mechanism::Xoauth2 | Mechanism::OAuthBearer => true,
            // SCRAM is driven as a no-IR challenge exchange so the multi-round
            // state lives entirely in `ScramExchange`, never in the stateless
            // `Auth` command `Display`.
            Mechanism::Login
            | Mechanism::ScramSha1
            | Mechanism::ScramSha256
            | Mechanism::ScramSha1Plus
            | Mechanism::ScramSha256Plus => false,
        }
    }

    /// Prepare a password exchange before writing AUTH. SCRAM's selected
    /// channel binding is carried separately by its framing adapter.
    pub(crate) fn password_client(
        self,
        credentials: &Credentials,
    ) -> Result<bifrost_sasl::SaslClient, Error> {
        let (username, password) = credentials.password_parts()?;
        match self {
            Self::Plain => {
                bifrost_sasl::SaslClient::plain("", username, password).map_err(Error::from)
            }
            Self::Login => Ok(bifrost_sasl::SaslClient::login(username, password.into())),
            _ => Err(error::invalid_input(
                "mechanism requires OAuth credentials or a SCRAM binding",
            )),
        }
    }

    /// OAuth exchange construction requires the token already resolved by the
    /// adapter. There is no optional token in the client construction path.
    pub(crate) fn oauth_client(
        self,
        identity: &str,
        token: &str,
    ) -> Result<bifrost_sasl::SaslClient, Error> {
        let mechanism = match self {
            Self::Xoauth2 => bifrost_sasl::OAuthMechanism::Xoauth2,
            Self::OAuthBearer => bifrost_sasl::OAuthMechanism::OAuthBearer,
            _ => {
                return Err(error::invalid_input(
                    "mechanism does not accept OAuth credentials",
                ));
            }
        };
        bifrost_sasl::SaslClient::oauth(mechanism, identity, token).map_err(Error::from)
    }
}

/// Map a `bifrost-sasl` computation failure into the SMTP error model.
///
/// The shared refusal class owns recovery intent. Malformed messages and
/// verifier mismatches stay parse-class; local facility failures stay local.
impl From<bifrost_sasl::SaslError> for Error {
    fn from(e: bifrost_sasl::SaslError) -> Self {
        match e {
            bifrost_sasl::SaslError::Protocol(m) => error::parse(m),
            // A caller credential that fails SASLprep, refused while the
            // exchange is built, before the AUTH line is sent. This remains
            // caller input, `Request(Malformed)`, independently of any outer
            // AUTH phase stamp, per the
            // local-refusal rule in `reference/error-model.md`.
            bifrost_sasl::SaslError::InvalidCredential(m) => error::invalid_input(m),
            bifrost_sasl::SaslError::LocalRuntime(m) => error::local_runtime(m),
            bifrost_sasl::SaslError::ServerRejected(refusal) => {
                use bifrost_sasl::RefusalClass;
                let kind = match refusal.refusal_class() {
                    RefusalClass::Credential => error::ErrorKind::Authentication,
                    RefusalClass::Protocol => error::ErrorKind::Parse,
                    RefusalClass::Policy => error::ErrorKind::Policy,
                    RefusalClass::Unavailable => error::ErrorKind::ServerUnavailable,
                    _ => error::ErrorKind::Internal,
                };
                Error::new(kind, Some(format!("SCRAM server error: {refusal}")))
                    .with_phase(SmtpCommandPhase::Auth { mechanism: None })
            }
            // `SaslError` is `#[non_exhaustive]`; any future variant is an
            // client mapping gap until it is handled explicitly; it is not
            // evidence that the user must replace credentials.
            other => error::internal(other.to_string())
                .with_phase(SmtpCommandPhase::Auth { mechanism: None }),
        }
    }
}

/// Selection carries the actual GS2 input through to the exchange.
#[derive(Debug)]
pub(crate) struct PasswordSelection {
    pub(crate) mechanism: Mechanism,
    pub(crate) scram: Option<bifrost_sasl::ScramRung>,
}

/// Select once from the SASL-owned ladder. A wire rejection is final.
pub(crate) fn password_mechanism(
    allowed: &[Mechanism],
    advertised: &ServerInfo,
    binding: Option<&bifrost_sasl::ChannelBindingData>,
) -> Result<PasswordSelection, Error> {
    let shared = allowed.iter().map(|m| m.shared()).collect::<Vec<_>>();
    let mut skipped_plus_for_binding = false;
    for (mechanism, result) in bifrost_sasl::negotiate(
        |m| {
            SMTP_MECHANISMS.iter().any(|candidate| {
                candidate.shared() == m && advertised.supports_auth_mechanism(*candidate)
            })
        },
        &bifrost_sasl::NegotiationPolicy {
            allowed: &shared,
            allow_insecure: false,
            oauth: false,
        },
        // The transport checks the shared TLS gate before resolving credentials.
        true,
        binding,
    ) {
        match result {
            Ok(selected) => {
                if let Some(mechanism) = allowed.iter().copied().find(|m| m.shared() == mechanism) {
                    return Ok(PasswordSelection {
                        mechanism,
                        scram: selected.scram,
                    });
                }
            }
            Err(bifrost_sasl::NegotiationSkip::Scram(
                bifrost_sasl::ScramSkip::PlusWithoutBinding,
            )) => {
                skipped_plus_for_binding |= shared.contains(&mechanism);
            }
            Err(_) => {}
        }
    }
    let message = if skipped_plus_for_binding {
        "channel binding required but unavailable"
    } else {
        "no compatible authentication mechanism"
    };
    Err(error::policy(message).with_phase(SmtpCommandPhase::Auth { mechanism: None }))
}

// Support set, not preference order. Advertised PLUS suppresses unbound SCRAM
// even when the caller excludes PLUS from its allow-list.
const SMTP_MECHANISMS: &[Mechanism] = &[
    Mechanism::Plain,
    Mechanism::Login,
    Mechanism::Xoauth2,
    Mechanism::OAuthBearer,
    Mechanism::ScramSha1,
    Mechanism::ScramSha256,
    Mechanism::ScramSha1Plus,
    Mechanism::ScramSha256Plus,
];

/// First allowed, advertised OAuth mechanism in the shared preference order.
///
/// OAuth credentials never flow through the SCRAM/password ladder; they pick
/// the first allowed `OAUTHBEARER` / `XOAUTH2` rung in shared order and run
/// the shared SASL client. Returns `None` for password credentials so
/// the caller falls through to [`password_mechanism`].
pub(crate) fn oauth_mechanism(
    mechanisms: &[Mechanism],
    advertised: &ServerInfo,
    credentials: &Credentials,
) -> Option<Mechanism> {
    if !matches!(credentials, Credentials::OAuth2 { .. }) {
        return None;
    }
    bifrost_sasl::OAUTH_PREFERENCE.iter().find_map(|preferred| {
        mechanisms
            .iter()
            .copied()
            .find(|m| m.shared() == *preferred && advertised.supports_auth_mechanism(*m))
    })
}

/// Resolve cached peer-certificate DER into SCRAM channel binding. Absence is
/// the only skip signal; a present but unusable certificate remains an error.
pub(crate) fn resolve_scram_binding(
    peer_certificate_der: Option<&[u8]>,
) -> Result<Option<bifrost_sasl::ChannelBindingData>, Error> {
    bifrost_sasl::binding_from_peer_certificate(peer_certificate_der).map_err(Error::from)
}

/// Next client line a [`ScramExchange::step`] produces.
#[derive(Debug)]
pub(crate) enum ScramStep {
    /// Base64 client line to write back to the server, kept zeroizing until the
    /// connection writes it.
    Reply(Secret),
    /// Server-final verified; the caller sends an empty continuation line and
    /// expects the tagged success reply.
    Complete,
}

/// Classify a shared SASL failure before adding SMTP mechanism attribution.
fn named_sasl_error(mechanism: Mechanism, error: bifrost_sasl::SaslError) -> Error {
    let mapped: Error = error.into();
    mapped.with_phase(SmtpCommandPhase::Auth {
        mechanism: Some(mechanism),
    })
}

/// SMTP base64 framing adapter for the shared SASL client.
pub(crate) struct ScramExchange {
    client: bifrost_sasl::SaslClient,
    first: Secret,
    mechanism: Mechanism,
}

impl ScramExchange {
    pub(crate) fn new(
        hash: ScramHash,
        binding: ScramChannelBinding,
        username: &str,
        password: Secret,
    ) -> Result<Self, Error> {
        let mut client = bifrost_sasl::SaslClient::scram(
            hash,
            binding,
            username,
            &password,
            generate_scram_nonce()?,
        )?;
        let mechanism = SMTP_MECHANISMS
            .iter()
            .copied()
            .find(|m| m.shared() == client.mechanism())
            .ok_or_else(|| error::internal("unmapped SCRAM mechanism"))?;
        let first = client
            .initial_response()
            .ok_or_else(|| error::internal("SCRAM initial response missing"))?;
        Ok(Self {
            client,
            first: crate::base64::encode_zeroizing(first.as_bytes()).into(),
            mechanism,
        })
    }

    pub(crate) fn client_first(&self) -> Secret {
        self.first.clone()
    }

    pub(crate) fn step(&mut self, server_line: &str) -> Result<ScramStep, Error> {
        match self
            .client
            .step(server_line.as_bytes())
            .map_err(|e| named_sasl_error(self.mechanism, e))?
        {
            bifrost_sasl::Step::Reply(reply) => Ok(ScramStep::Reply(
                crate::base64::encode_zeroizing(reply.as_bytes()).into(),
            )),
            bifrost_sasl::Step::Done => Ok(ScramStep::Complete),
            _ => Err(error::internal("unmapped SASL step")),
        }
    }
}

/// `ScramHash` for a SCRAM `Mechanism` variant (bound or unbound).
pub(crate) fn scram_hash(mechanism: Mechanism) -> Option<ScramHash> {
    mechanism.shared().scram().map(|(hash, _)| hash)
}

/// 18-byte URL-safe-no-pad base64 client nonce.
fn generate_scram_nonce() -> Result<String, Error> {
    generate_scram_nonce_with(getrandom::fill)
}

fn generate_scram_nonce_with<E: std::fmt::Display>(
    fill: impl FnOnce(&mut [u8]) -> Result<(), E>,
) -> Result<String, Error> {
    use base64::Engine;

    let mut bytes = [0u8; 18];
    fill(&mut bytes).map_err(|e| {
        // Entropy failure is a local runtime fault; changing credentials or
        // authentication policy cannot repair it.
        error::local_runtime(format!("failed to generate SCRAM nonce: {e}"))
            .with_phase(SmtpCommandPhase::Auth { mechanism: None })
    })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(all(test, feature = "account-error"))]
#[allow(clippy::expect_used)]
fn test_binding() -> Option<bifrost_sasl::ChannelBindingData> {
    let der = [
        0x30, 0x14, 0x30, 0x00, 0x30, 0x0b, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01,
        0x01, 0x0b, 0x03, 0x03, 0x00, 0x01, 0x02,
    ];
    bifrost_sasl::binding_from_peer_certificate(Some(&der)).expect("fixture certificate")
}

#[cfg(all(test, feature = "account-error"))]
mod test {
    use super::{Credentials, Mechanism, test_binding};
    use bifrost_net::auth::TokenSnapshot;
    use bifrost_net::{AccessToken, StaticTokenSource, TokenSource};
    use std::sync::Arc;

    #[test]
    fn nonce_entropy_failure_is_a_local_runtime_error() {
        use crate::transport::smtp::{
            account_error::{SmtpErrorContext, into_account_error},
            error::ErrorKind,
        };
        use bifrost_types::error::{AccountErrorKind, InternalErrorKind};

        let error = super::generate_scram_nonce_with(|_| Err("entropy unavailable")).unwrap_err();
        assert_eq!(error.kind(), &ErrorKind::LocalRuntime);
        let account = into_account_error(
            error,
            SmtpErrorContext::send(crate::transport::smtp::Dialect::Smtp),
        );
        assert_eq!(
            account.kind(),
            &AccountErrorKind::Internal(InternalErrorKind::RuntimeFailure)
        );
    }

    #[test]
    fn scram_mechanism_wire_tokens() {
        use bifrost_sasl::{ChannelBinding, ScramHash};

        assert_eq!(Mechanism::ScramSha1.to_string(), "SCRAM-SHA-1");
        assert_eq!(Mechanism::ScramSha256.to_string(), "SCRAM-SHA-256");
        assert_eq!(Mechanism::ScramSha1Plus.to_string(), "SCRAM-SHA-1-PLUS");
        assert_eq!(Mechanism::ScramSha256Plus.to_string(), "SCRAM-SHA-256-PLUS");

        // Single-authority pin: each token equals the matching bifrost-sasl
        // mechanism name.
        assert_eq!(
            Mechanism::ScramSha1.to_string(),
            ScramHash::Sha1.mechanism_name(ChannelBinding::None)
        );
        assert_eq!(
            Mechanism::ScramSha256.to_string(),
            ScramHash::Sha256.mechanism_name(ChannelBinding::None)
        );
        assert_eq!(
            Mechanism::ScramSha1Plus.to_string(),
            ScramHash::Sha1.mechanism_name(ChannelBinding::TlsServerEndPoint)
        );
        assert_eq!(
            Mechanism::ScramSha256Plus.to_string(),
            ScramHash::Sha256.mechanism_name(ChannelBinding::TlsServerEndPoint)
        );
    }

    #[test]
    fn oauth_token_source_preserves_terminal_auth_loss_classification() {
        use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
        use bifrost_net::Error as NetError;
        use bifrost_types::error::{AccountErrorKind, AuthErrorKind, RecoveryClass};

        let error = super::oauth_token_error(NetError::AuthLost {
            transmission_state: None,
            final_response: None,
        });
        let account = into_account_error(
            error,
            SmtpErrorContext::send(crate::transport::smtp::Dialect::Smtp),
        );

        assert_eq!(
            account.kind(),
            &AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        );
        assert_eq!(account.recovery(), &RecoveryClass::AuthLost);
    }

    #[test]
    fn oauth_token_source_preserves_transient_refresh_classification() {
        use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
        use bifrost_net::Error as NetError;
        use bifrost_types::error::{
            AccountErrorKind, AuthErrorKind, RecoveryClass, RetryDisposition, RetryReason,
        };
        use std::sync::Arc;

        let error = super::oauth_token_error(NetError::RefreshFailed {
            retry_after: None,
            source: Arc::new(NetError::Cancelled),
        });
        let account = into_account_error(
            error.clone(),
            SmtpErrorContext::send(crate::transport::smtp::Dialect::Lmtp),
        );

        assert_eq!(
            account.kind(),
            &AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient)
        );
        assert_eq!(
            account.protocol(),
            Some(bifrost_types::error::Protocol::Lmtp)
        );
        assert!(matches!(
            account.recovery(),
            RecoveryClass::Retry(advice)
                if advice.disposition == RetryDisposition::AfterAuthRefresh
                    && advice.reason == RetryReason::RefreshTransient
        ));
    }

    #[test]
    fn blocking_oauth_empty_refresher_needs_async_resolution_without_a_runtime() {
        let source = bifrost_net::OAuthRefresher::new(Arc::new(StaticTokenSource::new(
            "issuer-token",
            None,
        )));
        let credentials = Credentials::oauth2_source("alice", Arc::new(source));
        let error = credentials.oauth2_token_blocking().unwrap_err();
        assert!(error.is_invalid_input());
        assert!(error.to_string().contains("use the async SMTP transport"));
    }

    #[test]
    fn blocking_oauth_contended_snapshot_is_transient() {
        use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
        use bifrost_types::error::{AccountErrorKind, AuthErrorKind};

        struct BusySource;
        impl TokenSource for BusySource {
            fn snapshot(&self) -> Result<TokenSnapshot, bifrost_net::Error> {
                Ok(TokenSnapshot::Contended)
            }
            fn current(
                &self,
            ) -> bifrost_net::AccountFuture<Result<AccessToken, bifrost_net::Error>> {
                panic!("blocking resolution must never call current")
            }
            fn refresh(
                &self,
            ) -> bifrost_net::AccountFuture<Result<AccessToken, bifrost_net::Error>> {
                panic!("blocking resolution must never refresh")
            }
        }
        let credentials = Credentials::oauth2_source("alice", Arc::new(BusySource));
        let error = credentials.oauth2_token_blocking().unwrap_err();
        let account = into_account_error(
            error,
            SmtpErrorContext::send(crate::transport::smtp::Dialect::Smtp),
        );
        assert_eq!(
            account.kind(),
            &AccountErrorKind::Authentication(AuthErrorKind::RefreshTransient)
        );
    }

    #[test]
    fn blocking_oauth_static_snapshot_resolves_without_a_runtime() {
        let credentials = Credentials::oauth2("alice", "cached");
        let (identity, token) = credentials.oauth2_token_blocking().unwrap();
        assert_eq!(identity, "alice");
        assert_eq!(token.as_str(), "cached");
    }

    #[test]
    fn blocking_oauth_snapshot_preserves_typed_source_errors() {
        use crate::transport::smtp::account_error::{SmtpErrorContext, into_account_error};
        use bifrost_types::error::{AccountErrorKind, AuthErrorKind};

        struct LostSource;
        impl TokenSource for LostSource {
            fn snapshot(&self) -> Result<TokenSnapshot, bifrost_net::Error> {
                Err(bifrost_net::Error::AuthLost {
                    transmission_state: None,
                    final_response: None,
                })
            }
            fn current(
                &self,
            ) -> bifrost_net::AccountFuture<Result<AccessToken, bifrost_net::Error>> {
                panic!("blocking resolution must never call current")
            }
            fn refresh(
                &self,
            ) -> bifrost_net::AccountFuture<Result<AccessToken, bifrost_net::Error>> {
                panic!("blocking resolution must never refresh")
            }
        }
        let credentials = Credentials::oauth2_source("alice", Arc::new(LostSource));
        let error = credentials.oauth2_token_blocking().unwrap_err();
        let account = into_account_error(
            error,
            SmtpErrorContext::send(crate::transport::smtp::Dialect::Lmtp),
        );
        assert_eq!(
            account.kind(),
            &AccountErrorKind::Authentication(AuthErrorKind::ReauthorizationRequired)
        );
    }

    #[test]
    fn blocking_oauth_default_snapshot_never_polls_a_source() {
        use crate::transport::smtp::error::SmtpCommandPhase;
        use bifrost_net::{AccessToken, AccountFuture, Error as NetError, TokenSource};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct PendingSource {
            current_polls: Arc<AtomicUsize>,
        }

        impl TokenSource for PendingSource {
            fn is_coordinating(&self) -> bool {
                true
            }

            fn current(&self) -> AccountFuture<Result<AccessToken, NetError>> {
                let current_polls = Arc::clone(&self.current_polls);
                Box::pin(std::future::poll_fn(
                    move |_| -> std::task::Poll<Result<AccessToken, NetError>> {
                        current_polls.fetch_add(1, Ordering::SeqCst);
                        std::task::Poll::Pending
                    },
                ))
            }

            fn refresh(&self) -> AccountFuture<Result<AccessToken, NetError>> {
                Box::pin(std::future::pending())
            }
        }

        let current_polls = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(PendingSource {
            current_polls: Arc::clone(&current_polls),
        });
        assert!(source.is_coordinating());
        let credentials = Credentials::oauth2_source("alice", source);
        let error = credentials.oauth2_token_blocking().unwrap_err();

        assert!(error.is_invalid_input());
        assert!(
            error
                .to_string()
                .contains("requires refresh or async resolution")
        );
        assert_eq!(
            error.phase(),
            Some(SmtpCommandPhase::Auth { mechanism: None })
        );
        assert_eq!(current_polls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn scram_variants_do_not_support_initial_response() {
        assert!(!Mechanism::ScramSha1.supports_initial_response());
        assert!(!Mechanism::ScramSha256.supports_initial_response());
        assert!(!Mechanism::ScramSha1Plus.supports_initial_response());
        assert!(!Mechanism::ScramSha256Plus.supports_initial_response());
    }

    #[test]
    fn scram_requires_its_binding_aware_constructor() {
        let credentials = Credentials::password("u", "p");
        assert!(
            Mechanism::ScramSha256
                .password_client(&credentials)
                .is_err()
        );
        assert!(
            Mechanism::ScramSha256Plus
                .password_client(&credentials)
                .is_err()
        );
    }

    /// RFC 5802 Section 6. The single-selection API can only express this drop
    /// when channel binding is *unavailable*: with binding available the PLUS
    /// rung outranks its unbound twin regardless, so every assertion here uses
    /// absent binding data. That is exactly the MITM shape the rule
    /// exists for - a peer that strips the certificate must not be handed the
    /// unbound SCRAM rung as a consolation prize.
    #[test]
    fn unbound_scram_is_never_selected_when_its_plus_variant_is_advertised() {
        use super::{PASSWORD_MECHANISMS, password_mechanism};
        use crate::transport::smtp::extension::ServerInfo;

        // Both PLUS rungs advertised, binding unavailable: neither unbound
        // SCRAM rung is reachable, so selection lands on PLAIN.
        let advertised = ServerInfo::with_auth_mechanisms(&[
            Mechanism::ScramSha256Plus,
            Mechanism::ScramSha1Plus,
            Mechanism::ScramSha256,
            Mechanism::ScramSha1,
            Mechanism::Plain,
        ]);
        assert_eq!(
            password_mechanism(PASSWORD_MECHANISMS, &advertised, None)
                .unwrap()
                .mechanism,
            Mechanism::Plain
        );

        // Only SHA-256-PLUS advertised: SHA-256 is dropped, but SHA-1 has no
        // advertised PLUS variant and stays selectable.
        let advertised = ServerInfo::with_auth_mechanisms(&[
            Mechanism::ScramSha256Plus,
            Mechanism::ScramSha256,
            Mechanism::ScramSha1,
            Mechanism::Plain,
        ]);
        assert_eq!(
            password_mechanism(PASSWORD_MECHANISMS, &advertised, None)
                .unwrap()
                .mechanism,
            Mechanism::ScramSha1
        );

        // SHA-256-PLUS + SHA-256 only, binding unavailable: the unbound rung is
        // forbidden and there is nothing weaker, so this is a hard failure
        // diagnosed as a channel-binding problem - never an unbound success.
        let advertised =
            ServerInfo::with_auth_mechanisms(&[Mechanism::ScramSha256Plus, Mechanism::ScramSha256]);
        let err = password_mechanism(PASSWORD_MECHANISMS, &advertised, None).unwrap_err();
        assert!(err.is_policy());
        assert_eq!(
            err.to_string(),
            "SMTP policy error: channel binding required but unavailable"
        );
    }

    #[test]
    fn password_mechanism_selection() {
        use super::{PASSWORD_MECHANISMS, password_mechanism};
        use crate::transport::smtp::extension::ServerInfo;

        // All SCRAM + PLAIN advertised and allowed.
        let advertised = ServerInfo::with_auth_mechanisms(&[
            Mechanism::ScramSha256Plus,
            Mechanism::ScramSha1Plus,
            Mechanism::ScramSha256,
            Mechanism::ScramSha1,
            Mechanism::Plain,
        ]);
        // Strongest advertised rung wins. (This case cannot pin the RFC 5802
        // drop on its own - the PLUS rung outranks the unbound one anyway; see
        // `unbound_scram_is_never_selected_when_its_plus_variant_is_advertised`.)
        assert_eq!(
            password_mechanism(PASSWORD_MECHANISMS, &advertised, test_binding().as_ref())
                .unwrap()
                .mechanism,
            Mechanism::ScramSha256Plus
        );

        // SHA-256-PLUS + SHA-256 advertised: SHA-256-PLUS first.
        let advertised =
            ServerInfo::with_auth_mechanisms(&[Mechanism::ScramSha256Plus, Mechanism::ScramSha256]);
        assert_eq!(
            password_mechanism(PASSWORD_MECHANISMS, &advertised, test_binding().as_ref())
                .unwrap()
                .mechanism,
            Mechanism::ScramSha256Plus
        );

        // No PLUS advertised: non-PLUS SCRAM kept.
        let advertised = ServerInfo::with_auth_mechanisms(&[
            Mechanism::ScramSha256,
            Mechanism::ScramSha1,
            Mechanism::Plain,
        ]);
        assert_eq!(
            password_mechanism(PASSWORD_MECHANISMS, &advertised, None)
                .unwrap()
                .mechanism,
            Mechanism::ScramSha256
        );

        // LOGIN only appears when in `allowed`, and always last.
        let allowed = [Mechanism::Plain, Mechanism::Login];
        let advertised = ServerInfo::with_auth_mechanisms(&[Mechanism::Login, Mechanism::Plain]);
        assert_eq!(
            password_mechanism(&allowed, &advertised, None)
                .unwrap()
                .mechanism,
            Mechanism::Plain
        );

        // LOGIN-only server under the default `PASSWORD_MECHANISMS` (LOGIN not
        // allowed): empty order. Pins the migration-note regression.
        let advertised = ServerInfo::with_auth_mechanisms(&[Mechanism::Login]);
        assert!(password_mechanism(PASSWORD_MECHANISMS, &advertised, None).is_err());
    }

    #[test]
    fn default_password_mechanisms() {
        use super::{DEFAULT_MECHANISMS, PASSWORD_MECHANISMS};

        assert_eq!(
            PASSWORD_MECHANISMS,
            &[
                Mechanism::ScramSha256Plus,
                Mechanism::ScramSha1Plus,
                Mechanism::ScramSha256,
                Mechanism::ScramSha1,
                Mechanism::Plain,
            ]
        );
        assert_eq!(DEFAULT_MECHANISMS, PASSWORD_MECHANISMS);
        // LOGIN is opt-in: not in the default set.
        assert!(!PASSWORD_MECHANISMS.contains(&Mechanism::Login));
    }

    #[test]
    fn scram_binding_skip_falls_through() {
        use super::password_mechanism;
        use crate::transport::smtp::error::SmtpCommandPhase;
        use crate::transport::smtp::extension::ServerInfo;

        // PLUS rungs first, but binding unavailable -> fall through to PLAIN.
        let allowed = [
            Mechanism::ScramSha256Plus,
            Mechanism::ScramSha1Plus,
            Mechanism::Plain,
        ];
        assert_eq!(
            password_mechanism(&allowed, &ServerInfo::with_auth_mechanisms(&allowed), None,)
                .unwrap()
                .mechanism,
            Mechanism::Plain
        );

        // A PLUS rung whose binding *is* available is chosen.
        assert_eq!(
            password_mechanism(
                &allowed,
                &ServerInfo::with_auth_mechanisms(&allowed),
                test_binding().as_ref(),
            )
            .unwrap()
            .mechanism,
            Mechanism::ScramSha256Plus
        );

        // All-PLUS order with no binding -> Auth-phase policy error,
        // diagnosed as a channel-binding failure (every candidate was a
        // binding-unavailable PLUS rung).
        let all_plus = [Mechanism::ScramSha256Plus, Mechanism::ScramSha1Plus];
        let err = password_mechanism(
            &all_plus,
            &ServerInfo::with_auth_mechanisms(&all_plus),
            None,
        )
        .unwrap_err();
        assert!(err.is_policy());
        assert_eq!(
            err.phase(),
            Some(SmtpCommandPhase::Auth { mechanism: None })
        );
        assert_eq!(
            err.diagnostic_text().as_deref(),
            Some("channel binding required but unavailable")
        );

        // Empty order -> same routing (Auth-phase policy) but the
        // no-compatible-mechanism diagnostic, not the channel-binding one (no
        // PLUS rung was skipped).
        let err =
            password_mechanism(&[], &ServerInfo::default(), test_binding().as_ref()).unwrap_err();
        assert!(err.is_policy());
        assert_eq!(
            err.phase(),
            Some(SmtpCommandPhase::Auth { mechanism: None })
        );
        assert_eq!(
            err.diagnostic_text().as_deref(),
            Some("no compatible authentication mechanism")
        );
    }

    #[test]
    fn scram_binding_only_treats_absent_certificate_as_unavailable() {
        use super::resolve_scram_binding;

        assert!(resolve_scram_binding(None).unwrap().is_none());
        let err = resolve_scram_binding(Some(&[0x30, 0x01, 0x00])).unwrap_err();
        assert!(err.is_parse());
    }

    #[test]
    fn scram_server_refusals_keep_shared_recovery_intent() {
        use crate::transport::smtp::error::ErrorKind;
        for (token, expected) in [
            ("unknown-user", ErrorKind::Authentication),
            ("invalid-proof", ErrorKind::Authentication),
            ("channel-binding-not-supported", ErrorKind::Policy),
            ("invalid-encoding", ErrorKind::Parse),
            ("no-resources", ErrorKind::ServerUnavailable),
            ("other-error", ErrorKind::ServerUnavailable),
            ("future-extension", ErrorKind::ServerUnavailable),
        ] {
            let sasl = bifrost_sasl::verify_server_final(&format!("e={token}"), &[]).unwrap_err();
            let error = super::Error::from(sasl);
            assert_eq!(error.kind(), &expected, "{token}");
        }
    }

    #[test]
    fn scram_sasl_error_mapping() {
        use crate::transport::smtp::error::{ErrorKind, SmtpCommandPhase};

        let protocol: super::Error =
            bifrost_sasl::SaslError::Protocol("bad server-first".into()).into();
        assert_eq!(protocol.kind(), &ErrorKind::Parse);

        let auth_failed: super::Error = bifrost_sasl::SaslError::ServerRejected(
            bifrost_sasl::ScramServerError::ChannelBindingNotSupported,
        )
        .into();
        assert_eq!(auth_failed.kind(), &ErrorKind::Policy);
        assert_eq!(
            auth_failed.phase(),
            Some(SmtpCommandPhase::Auth { mechanism: None })
        );
    }

    /// A password that fails `SASLprep` is refused when the exchange is BUILT,
    /// before the AUTH line is sent, as the caller's invalid input - not a
    /// parse fault; AUTH phase decoration cannot change that classification.
    /// It used to surface from `step`, after the server-first
    /// message, as a protocol-class parse error.
    #[test]
    fn a_password_that_fails_saslprep_is_refused_before_the_exchange() {
        use super::{ScramChannelBinding, ScramExchange, ScramHash};
        use crate::transport::smtp::error::ErrorKind;

        let Err(err) = ScramExchange::new(
            ScramHash::Sha256,
            ScramChannelBinding::None,
            "user",
            "bad\u{7}".into(),
        ) else {
            panic!("a prohibited password must not build an exchange");
        };
        assert_eq!(err.kind(), &ErrorKind::InvalidInput);
        assert_eq!(err.phase(), None);
    }

    #[test]
    fn scram_exchange_gs2_and_c_attribute_agree() {
        use super::{ScramExchange, ScramStep};
        use bifrost_sasl::{ScramChannelBinding, ScramHash};
        for (binding, header, c_value) in [
            (ScramChannelBinding::None, "n,,", "biws"),
            (
                ScramChannelBinding::SupportedButNotAdvertised,
                "y,,",
                "eSws",
            ),
            (
                ScramChannelBinding::TlsServerEndPoint(vec![1, 2, 3, 4]),
                "p=tls-server-end-point,,",
                "cD10bHMtc2VydmVyLWVuZC1wb2ludCwsAQIDBA==",
            ),
        ] {
            let mut exchange =
                ScramExchange::new(ScramHash::Sha256, binding, "user", "pencil".into()).unwrap();
            let first = String::from_utf8(
                crate::base64::decode(exchange.client_first().as_bytes()).unwrap(),
            )
            .unwrap();
            assert!(first.starts_with(&format!("{header}n=user,r=")));
            let nonce = first.rsplit("r=").next().unwrap();
            let reply = exchange
                .step(&format!("r={nonce}server,s=QSXCR+Q6sek8bf92,i=4096"))
                .unwrap();
            let ScramStep::Reply(reply) = reply else {
                panic!("expected client-final")
            };
            let final_message =
                String::from_utf8(crate::base64::decode(reply.as_bytes()).unwrap()).unwrap();
            assert_eq!(
                final_message.split(',').next().unwrap(),
                format!("c={c_value}")
            );
        }
    }

    #[test]
    fn scram_exchange_none_binding_drives_to_complete() {
        use super::{ScramExchange, ScramStep, scram_hash};
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256).unwrap();
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pencil".into()).unwrap();

        // client-first decodes to `n,,n=user,r=<nonce>`.
        let client_first = exchange.client_first();
        let decoded =
            String::from_utf8(crate::base64::decode(client_first.as_bytes()).unwrap()).unwrap();
        assert!(decoded.starts_with("n,,n=user,r="), "got: {decoded}");
        let nonce = decoded.rsplit("r=").next().unwrap().to_owned();

        // Hand-build an RFC-5802-shaped server-first reusing the client nonce.
        let server_first = format!("r={nonce}srvextra,s=QSXCR+Q6sek8bf92,i=4096");

        // Drive the exchange with the server-first; it must reply.
        let reply = exchange.step(&server_first).unwrap();
        let client_final_b64 = match reply {
            ScramStep::Reply(s) => s,
            ScramStep::Complete => panic!("expected Reply on server-first"),
        };
        assert!(!client_final_b64.is_empty());

        // Recompute the expected server-final from the exchange's own state by
        // re-deriving the signature through scram_client_final with the exact
        // client-first-bare the exchange used.
        let client_first_bare = format!("n=user,r={nonce}");
        let password = bifrost_sasl::prepare_scram_password("pencil").unwrap();
        let (_cf, sig) = bifrost_sasl::scram_client_final(
            hash,
            &password,
            &nonce,
            &client_first_bare,
            &server_first,
            &ScramChannelBinding::None,
        )
        .unwrap();
        use base64::Engine;
        let server_final = format!(
            "v={}",
            base64::engine::general_purpose::STANDARD.encode(&sig)
        );
        assert!(matches!(
            exchange.step(&server_final).unwrap(),
            ScramStep::Complete
        ));

        // A continuation after Done is a parse-class error.
        assert!(exchange.step(&server_final).unwrap_err().is_parse());
    }

    #[test]
    fn scram_exchange_plus_binding_gs2_prefix() {
        use super::{ScramExchange, scram_hash};
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256Plus).unwrap();
        let exchange = ScramExchange::new(
            hash,
            ScramChannelBinding::TlsServerEndPoint(vec![1, 2, 3, 4]),
            "us=,er",
            "pw".into(),
        )
        .unwrap();
        let client_first = exchange.client_first();
        let decoded =
            String::from_utf8(crate::base64::decode(client_first.as_bytes()).unwrap()).unwrap();
        assert!(
            decoded.starts_with("p=tls-server-end-point,,n="),
            "got: {decoded}"
        );
        // saslname escaping: `=` -> `=3D`, `,` -> `=2C`.
        assert!(decoded.contains("n=us=3D=2Cer,r="), "got: {decoded}");
    }

    #[test]
    fn scram_exchange_malformed_server_first_is_parse() {
        use super::{ScramExchange, scram_hash};
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256).unwrap();
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pw".into()).unwrap();
        // Not a SCRAM server-first message at all.
        let err = exchange.step("garbage-without-r=").unwrap_err();
        assert!(err.is_parse(), "expected parse-class, got {err:?}");
    }

    #[test]
    fn scram_exchange_tampered_server_final_is_parse() {
        use super::{ScramExchange, ScramStep, scram_hash};
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256).unwrap();
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pencil".into()).unwrap();
        let client_first = exchange.client_first();
        let decoded =
            String::from_utf8(crate::base64::decode(client_first.as_bytes()).unwrap()).unwrap();
        let nonce = decoded.rsplit("r=").next().unwrap().to_owned();
        let server_first = format!("r={nonce}srvextra,s=QSXCR+Q6sek8bf92,i=4096");
        assert!(matches!(
            exchange.step(&server_first).unwrap(),
            ScramStep::Reply(_)
        ));
        // A wrong v= signature is a protocol-class verification failure (the
        // bifrost-sasl contract: signature mismatch is `Protocol`, not
        // `ServerRejected`), so it maps to a parse-class SMTP error.
        let err = exchange.step("v=AAAA").unwrap_err();
        assert!(err.is_parse(), "got {err:?}");
    }

    #[test]
    fn scram_exchange_server_error_is_auth_failure() {
        use super::{ScramExchange, ScramStep, scram_hash};
        use crate::transport::smtp::error::SmtpCommandPhase;
        use bifrost_sasl::ScramChannelBinding;

        let hash = scram_hash(Mechanism::ScramSha256).unwrap();
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pencil".into()).unwrap();
        let client_first = exchange.client_first();
        let decoded =
            String::from_utf8(crate::base64::decode(client_first.as_bytes()).unwrap()).unwrap();
        let nonce = decoded.rsplit("r=").next().unwrap().to_owned();
        let server_first = format!("r={nonce}srvextra,s=QSXCR+Q6sek8bf92,i=4096");
        assert!(matches!(
            exchange.step(&server_first).unwrap(),
            ScramStep::Reply(_)
        ));
        // A rejected proof requires new credentials, not a policy change.
        let err = exchange.step("e=invalid-proof").unwrap_err();
        assert_eq!(
            err.kind(),
            &crate::transport::smtp::error::ErrorKind::Authentication
        );
        assert_eq!(
            err.phase(),
            Some(SmtpCommandPhase::Auth {
                mechanism: Some(Mechanism::ScramSha256)
            })
        );
        // The server refused THIS rung, and the exchange is the last place
        // that still knows which one. The enum rides the phase stamp,
        // which `account_error.rs` turns into support-only `DiagnosticText`;
        // the classification above is unchanged by the attribution.
        let text = err.diagnostic_text().unwrap_or_default();
        assert!(text.contains("invalid-proof"), "got {text}");

        let account = crate::transport::smtp::account_error::into_account_error(
            err,
            crate::transport::smtp::account_error::SmtpErrorContext::send(
                crate::transport::smtp::Dialect::Smtp,
            ),
        );
        assert!(matches!(
            account.kind(),
            bifrost_types::error::AccountErrorKind::Authentication(
                bifrost_types::error::AuthErrorKind::ReauthorizationRequired
            )
        ));
        assert!(matches!(
            account.recovery(),
            bifrost_types::error::RecoveryClass::AuthLost
        ));
    }

    #[test]
    fn a_named_scram_failure_keeps_the_plus_suffix() {
        use super::{ScramExchange, ScramStep, scram_hash};
        use crate::transport::smtp::error::SmtpCommandPhase;
        use bifrost_sasl::ScramChannelBinding;

        // An operator debugging a channel-binding problem has to be able to
        // tell the bound rung from the unbound one, so the `-PLUS` suffix must
        // survive into the diagnostic rather than collapsing to the hash name.
        let hash = scram_hash(Mechanism::ScramSha256Plus).unwrap();
        let mut exchange = ScramExchange::new(
            hash,
            ScramChannelBinding::TlsServerEndPoint(vec![1, 2, 3, 4]),
            "user",
            "pencil".into(),
        )
        .unwrap();
        let client_first = exchange.client_first();
        let decoded =
            String::from_utf8(crate::base64::decode(client_first.as_bytes()).unwrap()).unwrap();
        let nonce = decoded.rsplit("r=").next().unwrap().to_owned();
        let server_first = format!("r={nonce}srvextra,s=QSXCR+Q6sek8bf92,i=4096");
        assert!(matches!(
            exchange.step(&server_first).unwrap(),
            ScramStep::Reply(_)
        ));

        let err = exchange.step("e=invalid-proof").unwrap_err();
        assert_eq!(
            err.phase(),
            Some(SmtpCommandPhase::Auth {
                mechanism: Some(Mechanism::ScramSha256Plus),
            })
        );

        // A protocol-class failure is named too, and stays parse-class.
        let mut exchange =
            ScramExchange::new(hash, ScramChannelBinding::None, "user", "pw".into()).unwrap();
        let err = exchange.step("garbage-without-r=").unwrap_err();
        assert!(err.is_parse(), "got {err:?}");
        assert_eq!(
            err.phase(),
            Some(SmtpCommandPhase::Auth {
                mechanism: Some(Mechanism::ScramSha256),
            })
        );
    }

    #[test]
    fn server_final_decodes_from_either_334_or_235() {
        use super::{decode_auth_challenge, decode_scram_payload};
        use crate::transport::smtp::response::{Category, Code, Detail, Response, Severity};

        let payload = crate::base64::encode("v=server-signature");

        // RFC 4954 shape: server-final on a 334 continuation.
        let continuation = Response::new(
            Code {
                severity: Severity::PositiveIntermediate,
                category: Category::Unspecified3,
                detail: Detail::Four,
            },
            vec![payload.clone()],
        );
        assert_eq!(
            decode_auth_challenge(&continuation).unwrap(),
            "v=server-signature"
        );
        assert_eq!(
            decode_scram_payload(&continuation).unwrap(),
            "v=server-signature"
        );

        // Field-observed shape: server-final folded onto the 235 success reply.
        let success = Response::new(
            Code {
                severity: Severity::PositiveCompletion,
                category: Category::Unspecified3,
                detail: Detail::Five,
            },
            vec![format!("2.7.0 {payload}")],
        );
        assert!(decode_auth_challenge(&success).is_err());
        assert_eq!(
            decode_scram_payload(&success).unwrap(),
            "v=server-signature"
        );
    }

    fn reply(step: bifrost_sasl::Step) -> bifrost_sasl::Secret {
        match step {
            bifrost_sasl::Step::Reply(reply) => reply,
            _ => panic!("expected SASL reply"),
        }
    }

    #[test]
    fn test_plain() {
        let mut client = Mechanism::Plain
            .password_client(&Credentials::password("username", "password"))
            .unwrap();
        assert_eq!(
            client.initial_response().unwrap().as_str(),
            "\0username\0password"
        );
        assert!(client.step(b"test").is_err());
    }

    #[test]
    fn plain_rejects_nul_in_credentials() {
        for credentials in [
            Credentials::password("admin\0username", "password"),
            Credentials::password("username", "pass\0word"),
        ] {
            assert!(Mechanism::Plain.password_client(&credentials).is_err());
        }
    }

    #[test]
    fn test_login() {
        let mut client = Mechanism::Login
            .password_client(&Credentials::password("alice", "wonderland"))
            .unwrap();
        assert!(client.initial_response().is_none());
        assert_eq!(reply(client.step(b"Username").unwrap()).as_str(), "alice");
        assert_eq!(
            reply(client.step(b"Password").unwrap()).as_str(),
            "wonderland"
        );
        assert!(client.step(b"again").is_err());
    }

    #[test]
    fn login_challenges_are_positional_not_prompt_matched() {
        let mut client = Mechanism::Login
            .password_client(&Credentials::password("alice", "wonderland"))
            .unwrap();
        assert_eq!(
            reply(client.step(b"Nom d'utilisateur: ").unwrap()).as_str(),
            "alice"
        );
        assert_eq!(
            reply(client.step(b"Enter password: ").unwrap()).as_str(),
            "wonderland"
        );
    }

    #[test]
    fn oauth_error_answers_do_not_reuse_the_initial_payload() {
        for (mechanism, expected) in [(Mechanism::Xoauth2, ""), (Mechanism::OAuthBearer, "\x01")] {
            let mut client = mechanism.oauth_client("user", "token").unwrap();
            client.initial_response().unwrap();
            assert_eq!(
                reply(client.step(br#"{"status":"401"}"#).unwrap()).as_str(),
                expected
            );
            assert!(client.step(b"repeat").is_err());
        }
    }

    #[test]
    fn oauth_frame_delimiters_are_local_refusals() {
        for mechanism in [Mechanism::Xoauth2, Mechanism::OAuthBearer] {
            for (identity, token) in [("u\x01ser", "token"), ("user", "tok\x01en")] {
                let error = mechanism.oauth_client(identity, token).unwrap_err();
                assert_eq!(
                    error.kind(),
                    &crate::transport::smtp::error::ErrorKind::InvalidInput
                );
            }
        }
    }

    #[test]
    fn test_xoauth2() {
        let mut client = Mechanism::Xoauth2
            .oauth_client("username", "vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==")
            .unwrap();
        assert_eq!(
            client.initial_response().unwrap().as_str(),
            "user=username\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==\x01\x01"
        );
        assert_eq!(
            reply(client.step(br#"{"status":"401"}"#).unwrap()).as_str(),
            ""
        );
    }

    #[test]
    fn test_oauthbearer() {
        for challenge in [
            b"{}".as_slice(),
            br#"{"status":"invalid_token"}"#.as_slice(),
        ] {
            let mut client = Mechanism::OAuthBearer
                .oauth_client(
                    "user@example.com",
                    "vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==",
                )
                .unwrap();
            let response = client.initial_response().unwrap();
            assert_eq!(
                response.as_str(),
                "n,a=user@example.com,\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhdHRhdmlzdGEuY29tCg==\x01\x01"
            );
            assert!(!response.contains("\x01host="));
            assert!(!response.contains("\x01port="));
            assert_eq!(reply(client.step(challenge).unwrap()).as_str(), "\x01");
        }
    }

    #[test]
    fn test_oauthbearer_escapes_gs2_identity() {
        assert_eq!(
            Mechanism::OAuthBearer
                .oauth_client("a,b=c", "token")
                .unwrap()
                .initial_response()
                .unwrap()
                .as_str(),
            "n,a=a=2Cb=3Dc,\x01auth=Bearer token\x01\x01"
        );
    }

    #[test]
    fn test_rejects_wrong_credential_kind() {
        assert!(
            Mechanism::Plain
                .password_client(&Credentials::oauth2("alice", "token"))
                .is_err()
        );
        assert!(
            Mechanism::Xoauth2
                .password_client(&Credentials::password("alice", "wonderland"))
                .is_err()
        );
        assert!(Mechanism::Plain.oauth_client("alice", "token").is_err());
    }

    #[tokio::test]
    #[cfg(feature = "tokio")]
    async fn test_accepts_zeroizing_secrets() {
        use super::Zeroizing;
        let credentials = Credentials::oauth2(
            "alice".to_owned(),
            Zeroizing::new("access-token".to_owned()),
        );

        // The token threads through the source convenience constructor and
        // is read back by both the async and the blocking resolver.
        let (_identity, blocking_token) = credentials.oauth2_token_blocking().unwrap();
        let (identity, token) = credentials.oauth2_token().await.unwrap();
        assert_eq!(blocking_token.as_str(), token.as_str());
        assert_eq!(
            Mechanism::Xoauth2
                .oauth_client(&identity, token.as_str())
                .unwrap()
                .initial_response()
                .unwrap()
                .as_str(),
            "user=alice\x01auth=Bearer access-token\x01\x01"
        );
    }

    #[tokio::test]
    #[cfg(feature = "tokio")]
    async fn oauth2_payload_reads_current_token() {
        use bifrost_net::{AccessToken, StaticTokenSource};
        use std::sync::Arc;

        let source = StaticTokenSource::new("old-token", None);
        let credentials = Credentials::oauth2_source("user@example.com", Arc::new(source.clone()));

        // The SASL payload is built from the token the source currently
        // holds, both through the async read and the blocking read.
        let (identity, token) = credentials.oauth2_token().await.unwrap();
        let payload = Mechanism::OAuthBearer
            .oauth_client(&identity, token.as_str())
            .unwrap()
            .initial_response()
            .unwrap();
        assert!(
            payload.contains("auth=Bearer old-token"),
            "got: {}",
            payload.as_str()
        );

        // Rotate the token on the shared source; the next read presents the
        // new token with no reconstruction of the credential.
        source.set(AccessToken::new("new-token", None));
        let (identity, token) = credentials.oauth2_token().await.unwrap();
        let payload = Mechanism::Xoauth2
            .oauth_client(&identity, token.as_str())
            .unwrap()
            .initial_response()
            .unwrap();
        assert!(
            payload.contains("auth=Bearer new-token"),
            "got: {}",
            payload.as_str()
        );

        let (_identity, token) = credentials.oauth2_token_blocking().unwrap();
        assert_eq!(token.as_str(), "new-token");
    }
}

#[cfg(all(test, not(feature = "account-error")))]
mod standalone_tests {
    use super::{Credentials, Mechanism, PASSWORD_MECHANISMS, password_mechanism};
    use crate::transport::smtp::extension::ServerInfo;

    #[test]
    fn direct_authentication_preserves_policy_and_protocol_errors_without_account_types() {
        let refused =
            password_mechanism(PASSWORD_MECHANISMS, &ServerInfo::default(), None).unwrap_err();
        assert!(refused.is_policy());
        let credentials = Credentials::password("user", "password");
        let mut client = Mechanism::Plain.password_client(&credentials).unwrap();
        client.initial_response().unwrap();
        let malformed =
            crate::transport::smtp::error::Error::from(client.step(b"unexpected").unwrap_err());
        assert!(malformed.is_parse());
        let (_, token) = Credentials::oauth2("user", "token")
            .oauth2_token_blocking()
            .unwrap();
        assert_eq!(token.as_str(), "token");
    }
}
