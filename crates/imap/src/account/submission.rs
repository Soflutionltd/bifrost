//! SMTP submission transport owned by an IMAP account.
//!
//! When an `ImapAccountConfig` carries a `SmtpSubmissionConfig`, the
//! factory builds one of these and stores it on the account. The send
//! path (`pim::send_message` / `pim::draft_send`) drives it to submit
//! RFC 5322 bytes over `bifrost-smtp`, authenticating with the same
//! credentials the IMAP login uses (or an explicit override), so a
//! consumer reaches `send` through the uniform `Account` surface with no
//! IMAP-plus-SMTP composition leaking up.
//!
//! Address conversion is confined here: the send path stays in
//! `bifrost_types::Address` space, and the narrow, total
//! `Address -> bifrost_smtp::Address` reverse-path / recipient
//! conversion (bare addr-specs, no display name) happens only at
//! `send_rfc5322`.

use std::sync::Arc;
use std::time::Duration;

use bifrost_net::TokenSource;
use bifrost_smtp::transport::smtp::authentication::Credentials as SmtpCredentials;
use bifrost_smtp::transport::smtp::{PoolConfig, SendOptions};
use bifrost_smtp::{Address as SmtpAddress, AsyncSmtpTransport, TokioExecutor};
use bifrost_types::error::{AccountError, BatchItem, BatchItemId};
use bifrost_types::mime::SubmissionEnvelope;

use crate::types::{Credentials, CredentialsKind};

/// TLS mode for the submission transport. Mirrors `ImapConfig`'s TLS
/// modes; maps to SMTP's `relay` / `starttls_relay` / `builder_dangerous`
/// at build time.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmissionTls {
    /// Implicit TLS (SMTPS). Default port 465.
    Implicit,
    /// STARTTLS upgrade. Default port 587.
    StartTls,
    /// Plaintext. Default port 587. Requires opting into insecure auth.
    Plaintext,
}

impl SubmissionTls {
    const fn default_port(self) -> u16 {
        match self {
            Self::Implicit => 465,
            Self::StartTls | Self::Plaintext => 587,
        }
    }
}

/// Submission auth when it differs from the IMAP login. The common
/// single-sign-on case (`SmtpSubmissionConfig::credentials == None`)
/// reuses the IMAP `Credentials` and never constructs one of these.
#[non_exhaustive]
#[derive(Clone)]
pub enum SubmissionCredentials {
    /// Username + password submission auth.
    Password {
        /// Authentication identity.
        username: String,
        /// Password or app password.
        password: String,
    },
    /// OAuth 2.0 bearer submission auth, sharing a rotation source.
    OAuth2 {
        /// Authorization identity, usually the email address.
        identity: String,
        /// Live token source, read fresh at each connect.
        token_source: Arc<dyn TokenSource>,
    },
}

/// Submission transport configuration, independent of the IMAP dial
/// config because submission host/port differ from IMAP host/port.
#[non_exhaustive]
#[derive(Clone)]
pub struct SmtpSubmissionConfig {
    /// Submission host (e.g. "smtp.example.com").
    pub host: String,
    /// TLS mode for submission.
    pub tls: SubmissionTls,
    /// Override the mode-default port when set.
    pub port: Option<u16>,
    /// Per-command timeout for the submission transport.
    pub timeout: Option<Duration>,
    /// Override the SMTP credentials. `None` reuses the IMAP account
    /// credentials (same username + password, or same identity + A1
    /// token source).
    pub credentials: Option<SubmissionCredentials>,
    /// Pool sizing for the submission transport.
    pub pool: Option<PoolConfig>,
    /// Default From address, used when a `SendRequest` omits `from` and
    /// as the MAIL FROM reverse path default.
    pub default_from: bifrost_types::Address,
    /// Append sent messages to the Sent folder unless the caller says
    /// otherwise via `SendRequest::save_to_sent`.
    pub save_to_sent_default: bool,
}

impl SmtpSubmissionConfig {
    /// Construct a submission config that reuses the IMAP credentials.
    pub fn new(
        host: impl Into<String>,
        tls: SubmissionTls,
        default_from: bifrost_types::Address,
    ) -> Self {
        Self {
            host: host.into(),
            tls,
            port: None,
            timeout: None,
            credentials: None,
            pool: None,
            default_from,
            save_to_sent_default: true,
        }
    }

    /// Override the port (defaults follow the TLS mode: 465 / 587 / 587).
    #[must_use]
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Set the per-command submission timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Use explicit submission credentials instead of reusing the IMAP
    /// login.
    #[must_use]
    pub fn with_credentials(mut self, credentials: SubmissionCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Tune the submission connection pool.
    #[must_use]
    pub fn with_pool(mut self, pool: PoolConfig) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Set whether sends append to the Sent folder by default.
    #[must_use]
    pub fn with_save_to_sent_default(mut self, save: bool) -> Self {
        self.save_to_sent_default = save;
        self
    }
}

/// Thin crate-internal wrapper around the SMTP transport plus the
/// resolved default From / save-to-sent policy, so the send path can
/// default `from` and the MAIL FROM reverse path.
pub(crate) struct SubmissionTransport {
    transport: AsyncSmtpTransport<TokioExecutor>,
    pub(crate) budget: bifrost_net::BandwidthBudget,
    default_from: bifrost_types::Address,
    save_to_sent_default: bool,
}

impl SubmissionTransport {
    pub(crate) fn default_from(&self) -> &bifrost_types::Address {
        &self.default_from
    }

    pub(crate) fn save_to_sent_default(&self) -> bool {
        self.save_to_sent_default
    }

    /// Build the submission transport from the config, deriving SMTP
    /// credentials from the IMAP login when the config does not override
    /// them.
    pub(crate) fn build(
        cfg: &SmtpSubmissionConfig,
        imap_credentials: &Credentials,
        meter: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_budget: bifrost_net::BandwidthBudget,
    ) -> Result<Self, crate::Error> {
        let credentials = resolve_credentials(cfg, imap_credentials);
        let port = cfg.port.unwrap_or_else(|| cfg.tls.default_port());

        // A builder error here is a local submission-config fault (bad TLS
        // SNI / host), not a wire failure: surface it as InvalidInput so the
        // account boundary classifies it as a client-side request error.
        let smtp_config_err =
            |e: bifrost_smtp::transport::smtp::Error| crate::Error::InvalidInput(e.to_string());
        let mut builder = match cfg.tls {
            SubmissionTls::Implicit => {
                AsyncSmtpTransport::<TokioExecutor>::relay(&cfg.host).map_err(smtp_config_err)?
            }
            SubmissionTls::StartTls => {
                AsyncSmtpTransport::<TokioExecutor>::starttls_relay(&cfg.host)
                    .map_err(smtp_config_err)?
            }
            SubmissionTls::Plaintext => {
                AsyncSmtpTransport::<TokioExecutor>::builder_dangerous(cfg.host.clone())
                    .dangerous_allow_insecure_auth(true)
            }
        };

        // Submission and IMAP share one account budget in each direction.
        builder = builder
            .port(port)
            .credentials(credentials)
            .bandwidth_metering(meter, Some(bandwidth_budget.clone()));
        if let Some(timeout) = cfg.timeout {
            builder = builder.timeout(Some(timeout));
        }
        if let Some(pool) = &cfg.pool {
            builder = builder.pool_config(pool.clone());
        }

        Ok(Self {
            transport: builder.build(),
            budget: bandwidth_budget,
            default_from: cfg.default_from.clone(),
            save_to_sent_default: cfg.save_to_sent_default,
        })
    }

    /// Submit the RFC 5322 bytes over SMTP using `envelope` as the
    /// reverse path / recipient set. This is the only place the
    /// `bifrost_types::Address -> bifrost_smtp::Address` conversion
    /// happens. Returns the already-translated `AccountError` from
    /// `bifrost-smtp` on failure.
    pub(crate) async fn send_rfc5322(
        &self,
        envelope: &SubmissionEnvelope,
        raw: &[u8],
        hold: Option<std::time::SystemTime>,
    ) -> Result<(), AccountError> {
        let from = to_smtp_address(&envelope.from);
        let recipients: Vec<BatchItem<SmtpAddress>> = envelope
            .recipients
            .iter()
            .enumerate()
            .map(|(index, address)| {
                BatchItem::new(BatchItemId(index.to_string()), to_smtp_address(address))
            })
            .collect();

        let options = build_send_options(hold);

        let outcome = self
            .transport
            .send_raw_batch_with_options(Some(from), recipients, raw, &options)
            .await?;

        // One shared DATA, three-lane per-recipient outcome. The
        // `Err(_)`-means-nothing-transmitted boundary (error-model.md)
        // governs what we may return: the IMAP send surface is
        // `Result<(), _>` -> `Result<ObjectId, _>`, and the engine
        // re-drives a non-idempotent `Send`/`DraftSend` on `Err`. So we
        // must collapse to `Err` ONLY when the message reached no
        // recipient at all. If any recipient landed in the succeeded
        // lane, the body crossed the side-effect boundary; re-driving
        // would double-deliver to those recipients. A partial send
        // (recipient A delivered, B rejected on RCPT) is therefore an
        // overall success at the send-commit boundary - the rejected
        // recipients are logged for reconcile, never resent.
        if !outcome.succeeded().is_empty() {
            if let Some(failure) = outcome.failed().first() {
                tracing::warn!(
                    target: "bifrost_imap::send",
                    delivered = outcome.succeeded().len(),
                    rejected = outcome.failed().len(),
                    uncertain = outcome.uncertain().len(),
                    error = %failure.error,
                    "partial submission: message committed to some recipients while \
                     others were rejected; not re-driving (would double-deliver)"
                );
            } else if let Some(uncertain) = outcome.uncertain().first() {
                tracing::warn!(
                    target: "bifrost_imap::send",
                    delivered = outcome.succeeded().len(),
                    uncertain = outcome.uncertain().len(),
                    error = %uncertain.error,
                    "partial submission: message committed to some recipients while \
                     others are uncertain; not re-driving (would double-deliver)"
                );
            }
            return Ok(());
        }

        // Nothing succeeded. The whole message failed to reach any
        // recipient, so `Err` (re-drive permitted) is correct. The first
        // failed recipient's already-classified `AccountError` is
        // authoritative; fall back to the uncertain lane (a transport
        // drop after body carries `Reconcile(PartialCompletionSignal)`
        // and stays a non-blind-retry).
        if let Some(failure) = outcome.failed().first() {
            return Err(failure.error.clone());
        }
        if let Some(uncertain) = outcome.uncertain().first() {
            return Err(uncertain.error.clone());
        }
        Ok(())
    }
}

/// Build the SMTP `SendOptions` for a submission. When `hold` is
/// `Some`, FUTURERELEASE rides as the absolute-time HOLDUNTIL parameter
/// so the boundary does not race `now()`; the relay computes the delay
/// and decides support (an unsupporting relay yields an
/// `Unsupported(Send)` AccountError from the smtp layer).
fn build_send_options(hold: Option<std::time::SystemTime>) -> SendOptions {
    match hold {
        Some(at) => SendOptions::default().hold_until(hold_until_rfc3339(at)),
        None => SendOptions::default(),
    }
}

/// Format an absolute instant as RFC 3339 for the SMTP FUTURERELEASE
/// `HOLDUNTIL` parameter.
fn hold_until_rfc3339(at: std::time::SystemTime) -> String {
    jiff::Timestamp::try_from(at)
        .map(|ts| ts.to_string())
        .unwrap_or_default()
}

/// Convert a `bifrost_types::Address` into an SMTP envelope address.
/// Envelope addresses are bare addr-specs (no display name), so this is
/// total: even a malformed user/domain split is accepted unchecked, with
/// the server rejecting an invalid RCPT - the same posture
/// `bifrost-smtp`'s own envelope builder takes.
fn to_smtp_address(address: &bifrost_types::Address) -> SmtpAddress {
    match address.address.rsplit_once('@') {
        Some((user, domain)) => SmtpAddress::new_dangerous(user, domain),
        None => SmtpAddress::new_dangerous(address.address.as_str(), ""),
    }
}

/// Derive SMTP credentials: an explicit override if present, else a
/// structural copy of the IMAP login (password or A1 token source). The
/// `Arc<dyn TokenSource>` threads straight across - the same source the
/// IMAP side reads, so a rotated token is presented on both legs.
fn resolve_credentials(
    cfg: &SmtpSubmissionConfig,
    imap_credentials: &Credentials,
) -> SmtpCredentials {
    match &cfg.credentials {
        Some(SubmissionCredentials::Password { username, password }) => {
            SmtpCredentials::password(username.clone(), password.clone())
        }
        Some(SubmissionCredentials::OAuth2 {
            identity,
            token_source,
        }) => SmtpCredentials::oauth2_source(identity.clone(), Arc::clone(token_source)),
        None => match imap_credentials.kind() {
            CredentialsKind::Password { username, password } => {
                SmtpCredentials::password(username.clone(), password.as_str().to_owned())
            }
            CredentialsKind::OAuth2 {
                identity,
                token_source,
            } => SmtpCredentials::oauth2_source(identity.clone(), Arc::clone(token_source)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_net::StaticTokenSource;
    use bifrost_smtp::transport::smtp::extension::{FutureReleaseParameter, MailParameter};

    #[tokio::test]
    async fn submission_budget_tracks_account_cap_updates() {
        use super::super::test_support::scripted_account_parts;
        use crate::connection::test_support::{driver_pair, preauth_greeting};
        use bifrost_types::Account;
        let (conn, peer) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        drop(peer);
        let config = SmtpSubmissionConfig::new(
            "smtp.invalid",
            SubmissionTls::Plaintext,
            bifrost_types::Address::bare("user@example.com"),
        );
        let budget = bifrost_net::BandwidthBudget::default();
        let submission = SubmissionTransport::build(
            &config,
            &Credentials::password("user", "pass"),
            None,
            budget.clone(),
        )
        .unwrap();
        let mut parts = scripted_account_parts(conn, 1);
        parts.bandwidth_budget = budget.clone();
        parts.submission = Some(Arc::new(submission));
        let account = super::super::ImapAccount::new(parts);
        for (requested, expected) in [(Some(17), Some(17)), (Some(0), Some(1)), (None, None)] {
            account.set_bandwidth_cap(requested);
            assert_eq!(budget.cap(), expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_submission_and_imap_use_the_live_account_budget_in_both_directions() {
        use crate::account::test_support::scripted_account_parts;
        use crate::connection::test_support::{
            driver_pair_metered, preauth_greeting, read_line, respond, select_inbox, tag_of,
        };
        use bifrost_net::{BandwidthBudget, BandwidthDirection};
        use bifrost_types::Account;
        use futures::FutureExt;
        use tokio::time::{Duration, Instant};
        let budget = BandwidthBudget::default();
        let (conn, mut peer) =
            driver_pair_metered(&preauth_greeting("IMAP4rev1"), budget.clone()).await;
        select_inbox(&conn, &mut peer).await;
        let config = SmtpSubmissionConfig::new(
            "smtp.invalid",
            SubmissionTls::Plaintext,
            bifrost_types::Address::bare("user@example.com"),
        );
        let submission = Arc::new(
            SubmissionTransport::build(
                &config,
                &Credentials::password("user", "pass"),
                None,
                budget.clone(),
            )
            .unwrap(),
        );
        let mut parts = scripted_account_parts(conn, 1);
        parts.bandwidth_budget = budget;
        parts.submission = Some(Arc::clone(&submission));
        let account = super::super::ImapAccount::new(parts);
        account.set_bandwidth_cap(Some(64));
        // Spend the exact handle handed to SMTP's bandwidth_metering builder.
        for direction in [BandwidthDirection::Inbound, BandwidthDirection::Outbound] {
            submission.budget.admit(direction, 64).await.finish(64);
        }
        let pooled = account.pool.checkout_any().await.unwrap();
        let start = Instant::now();
        let script = async {
            let command = read_line(&mut peer).await;
            let tag = tag_of(&command);
            assert!(command.contains("UID FETCH"));
            // Check outbound independently: inbound waiting must not hide an
            // uncapped command write behind the response's one-second slot.
            assert_eq!(
                start.elapsed(),
                Duration::from_nanos(u64::try_from(command.len()).unwrap() * 1_000_000_000 / 64)
            );
            respond(
                &mut peer,
                &format!("* 1 FETCH (UID 1 BODY[] {{4}}\r\ndata)\r\n{tag} OK done\r\n"),
            )
            .await;
        };
        let set = crate::types::SequenceSet::new("1").unwrap();
        let attrs = [
            crate::types::FetchAttr::Uid,
            crate::types::FetchAttr::BodySection {
                peek: true,
                section: None,
                partial: None,
            },
        ];
        let (result, ()) = tokio::join!(
            pooled
                .connection()
                .uid_fetch(&set, &attrs, Duration::from_secs(10)),
            script
        );
        assert_eq!(result.unwrap().len(), 1);
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        let mut pending = Box::pin(submission.budget.admit(BandwidthDirection::Inbound, 64));
        assert!((&mut pending).now_or_never().is_none());
        let wake = Instant::now();
        account.set_bandwidth_cap(None);
        pending.await.finish(64);
        assert_eq!(wake.elapsed(), Duration::ZERO);
    }

    #[test]
    fn scheduled_send_builds_holduntil_mail_parameter() {
        let at = std::time::SystemTime::now() + std::time::Duration::from_secs(600);
        let options = build_send_options(Some(at));
        assert!(
            options.mail_parameters().iter().any(|p| matches!(
                p,
                MailParameter::FutureRelease(FutureReleaseParameter::HoldUntil(_))
            )),
            "scheduled send must emit a FutureRelease(HoldUntil) mail parameter"
        );

        // An immediate send carries no FUTURERELEASE parameter.
        let immediate = build_send_options(None);
        assert!(
            !immediate
                .mail_parameters()
                .iter()
                .any(|p| matches!(p, MailParameter::FutureRelease(_)))
        );
    }

    #[test]
    fn submission_credentials_reuse_imap_oauth() {
        let source: Arc<dyn TokenSource> = Arc::new(StaticTokenSource::new("tok", None));
        let imap = Credentials::oauth2_source("user@example.com", Arc::clone(&source));
        let cfg = SmtpSubmissionConfig::new(
            "smtp.example.com",
            SubmissionTls::Implicit,
            bifrost_types::Address::bare("user@example.com"),
        );

        let smtp = resolve_credentials(&cfg, &imap);
        match smtp {
            SmtpCredentials::OAuth2 {
                identity,
                token_source,
            } => {
                assert_eq!(identity, "user@example.com");
                // Same A1 token source threaded across the IMAP/SMTP boundary.
                assert!(Arc::ptr_eq(&token_source, &source));
            }
            SmtpCredentials::Password { .. } => panic!("expected OAuth2 credentials"),
        }
    }

    #[test]
    fn submission_tls_default_ports() {
        assert_eq!(SubmissionTls::Implicit.default_port(), 465);
        assert_eq!(SubmissionTls::StartTls.default_port(), 587);
        assert_eq!(SubmissionTls::Plaintext.default_port(), 587);
    }
}
