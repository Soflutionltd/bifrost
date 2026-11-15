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
//! `bifrost_types::Address` space, and the checked
//! `Address -> bifrost_smtp::Address` reverse-path / recipient
//! conversion (bare addr-specs, no display name) happens only at
//! `send_rfc5322`.

use std::sync::Arc;
use std::time::Duration;

use bifrost_net::TokenSource;
use bifrost_smtp::transport::smtp::authentication::Credentials as SmtpCredentials;
use bifrost_smtp::transport::smtp::{PoolConfig, SendOptions, SubmissionStatus, submission_status};
use bifrost_smtp::{Address as SmtpAddress, AsyncSmtpTransport, TokioExecutor};
use bifrost_types::error::{AccountError, BatchItem, BatchItemId};
use bifrost_types::mime::SubmissionEnvelope;
use bifrost_types::{RecipientNotAccepted, RecipientOutcome};

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
    /// Message-submission defaults, not relay defaults: plaintext stays on
    /// 587 (see `default_submission_port`).
    const fn default_port(self) -> u16 {
        use bifrost_smtp::transport::smtp::{PortTls, default_submission_port};
        let tls = match self {
            Self::Implicit => PortTls::Implicit,
            Self::StartTls => PortTls::StartTls,
            Self::Plaintext => PortTls::Plaintext,
        };
        default_submission_port(tls)
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
    pub(crate) async fn shutdown(&self) {
        bifrost_smtp::AsyncTransport::shutdown(&self.transport).await;
    }

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
        Self::build_with_connector(
            cfg,
            imap_credentials,
            meter,
            bandwidth_budget,
            #[cfg(test)]
            None,
        )
    }

    pub(super) fn build_with_connector(
        cfg: &SmtpSubmissionConfig,
        imap_credentials: &Credentials,
        meter: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_budget: bifrost_net::BandwidthBudget,
        #[cfg(test)] connector: Option<bifrost_smtp::transport::smtp::ScriptedConnector>,
    ) -> Result<Self, crate::Error> {
        let credentials = resolve_credentials(cfg, imap_credentials);
        let port = cfg.port.unwrap_or_else(|| cfg.tls.default_port());

        // A builder error here is a local submission-config fault (bad TLS
        // SNI / host), not a wire failure: surface it as InvalidInput so the
        // account boundary classifies it as a client-side request error.
        // Keep this conversion: relay/starttls_relay only construct local TLS
        // parameters, and cannot produce a network or handshake failure.
        let smtp_config_err = |e: bifrost_smtp::transport::smtp::Error| {
            crate::Error::InvalidInput(e.to_string().into())
        };
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

        #[cfg(test)]
        if let Some(connector) = connector {
            builder = builder.scripted_connector(connector);
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
    /// `bifrost-smtp` on failure, and on success the recipients that were
    /// not accepted while another one was (empty on a clean send). Their
    /// errors are not yet stamped with the caller's operation.
    pub(crate) async fn send_rfc5322(
        &self,
        envelope: &SubmissionEnvelope,
        raw: &[u8],
        hold: Option<std::time::SystemTime>,
    ) -> Result<Vec<RecipientNotAccepted>, AccountError> {
        let from = SmtpAddress::try_from(&envelope.from)
            .map_err(|_| submission_invalid("invalid SMTP reverse-path address"))?;
        let options = build_send_options(hold)?;
        let mut locally_failed = Vec::new();
        let recipients: Vec<BatchItem<SmtpAddress>> = envelope
            .recipients
            .iter()
            .enumerate()
            .filter_map(|(index, address)| match SmtpAddress::try_from(address) {
                Ok(address) => Some(BatchItem::new(BatchItemId(index.to_string()), address)),
                Err(_) => {
                    locally_failed.push(RecipientNotAccepted::new(
                        address.clone(),
                        RecipientOutcome::Failed,
                        submission_invalid("invalid SMTP recipient address"),
                    ));
                    None
                }
            })
            .collect();
        if recipients.is_empty() {
            return Err(locally_failed.first().map_or_else(
                || submission_invalid("SMTP submission requires a recipient"),
                |failure| failure.error.clone(),
            ));
        }

        let outcome = self
            .transport
            .send_raw_batch_with_options(Some(from), recipients, raw, &options)
            .await?;

        // One shared DATA, three-lane per-recipient outcome. An `Err` from
        // a non-idempotent send reads as "send failed" and invites the
        // caller to resend the whole message, so `Err` is returned ONLY
        // when no recipient acknowledged acceptance. If any recipient
        // landed in the succeeded lane, the body crossed the side-effect
        // boundary and a resend would double-deliver to it. A partial
        // send (A delivered, B rejected on RCPT) is therefore a success,
        // and the recipients that were not accepted ride the receipt,
        // each with its own error, never resent by us.
        //
        // bifrost-smtp's `submission_status` owns that "counts as a send"
        // decision; this match only maps its answer onto the receipt.
        match submission_status(&outcome) {
            SubmissionStatus::Committed => {}
            // Nothing acknowledged acceptance, but a recipient may still
            // have the message. Its error (InFlight evidence, so
            // `Application::from_leaf` reads Uncertain) is returned rather
            // than a failed recipient's, whose recovery could advise
            // resending the whole call and duplicate the uncertain one.
            // Whether an uncertain IMAP send should instead surface as an
            // uncertain receipt is an open owner ruling; this keeps the
            // existing error return until it is made.
            SubmissionStatus::AcceptanceUncertain(error) | SubmissionStatus::Rejected(error) => {
                return Err(error.clone());
            }
            SubmissionStatus::Empty => return Ok(Vec::new()),
            // A status bifrost-smtp adds later is not known to commit the
            // message, so it is reported through whichever lane error exists
            // rather than claimed as sent.
            _ => {
                if let Some(lane) = outcome.uncertain().first() {
                    return Err(lane.error.clone());
                }
                return match outcome.failed().first() {
                    Some(lane) => Err(lane.error.clone()),
                    None => Ok(Vec::new()),
                };
            }
        }
        let mut not_accepted = locally_failed;
        not_accepted.extend(not_accepted_recipients(&envelope.recipients, &outcome));
        if !not_accepted.is_empty() {
            tracing::warn!(
                target: "bifrost_imap::send",
                delivered = outcome.succeeded().len(),
                failed = not_accepted.iter().filter(|r| {
                    r.outcome == RecipientOutcome::Failed
                }).count(),
                uncertain = outcome.uncertain().len(),
                "partial submission: message committed to some recipients while \
                 others were not accepted; reported on the receipt"
            );
        }
        Ok(not_accepted)
    }
}

/// Map the failed and uncertain lanes back to their envelope recipients.
/// Each lane entry carries the `BatchItemId` `send_rfc5322` assigned (the
/// envelope index); the mapping goes by that id, never by position within a
/// lane, since LMTP's final-status drain skips rejected recipients. An id
/// that names no envelope entry cannot come from bifrost-smtp, which checks
/// one answer per submitted id; it is skipped rather than invented.
fn not_accepted_recipients<T>(
    recipients: &[bifrost_types::Address],
    outcome: &bifrost_types::BatchOutcome<T>,
) -> Vec<RecipientNotAccepted> {
    let address = |id: &BatchItemId| {
        id.0.parse::<usize>()
            .ok()
            .and_then(|index| recipients.get(index))
            .cloned()
    };
    let failed = outcome.failed().iter().filter_map(|lane| {
        address(&lane.item).map(|address| {
            RecipientNotAccepted::new(address, RecipientOutcome::Failed, lane.error.clone())
        })
    });
    let uncertain = outcome.uncertain().iter().filter_map(|lane| {
        address(&lane.item).map(|address| {
            RecipientNotAccepted::new(address, RecipientOutcome::Uncertain, lane.error.clone())
        })
    });
    failed.chain(uncertain).collect()
}

/// Build the SMTP `SendOptions` for a submission. When `hold` is
/// `Some`, FUTURERELEASE rides as the absolute-time HOLDUNTIL parameter
/// so the boundary does not race `now()`; the relay computes the delay
/// and decides support (an unsupporting relay yields an
/// `Unsupported(Send)` AccountError from the smtp layer).
fn build_send_options(hold: Option<std::time::SystemTime>) -> Result<SendOptions, AccountError> {
    match hold {
        Some(at) => Ok(SendOptions::default().hold_until(hold_until_rfc3339(at)?)),
        None => Ok(SendOptions::default()),
    }
}

/// Refuse instants outside the timestamp range before opening an SMTP transaction.
fn hold_until_rfc3339(at: std::time::SystemTime) -> Result<String, AccountError> {
    jiff::Timestamp::try_from(at)
        .map(|ts| ts.to_string())
        .map_err(|_| {
            submission_invalid("scheduled send instant is outside the supported timestamp range")
        })
}

fn submission_invalid(detail: &str) -> AccountError {
    super::error::into_account_error(
        crate::Error::InvalidInput(detail.into()),
        super::error::ImapErrorContext::operation(bifrost_types::AccountOperation::Send),
    )
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

    #[tokio::test(start_paused = true)]
    async fn account_close_shuts_down_the_owned_submission_pool() {
        use crate::account::test_support::scripted_account_parts;
        use crate::connection::test_support::{driver_pair, preauth_greeting};
        use bifrost_smtp::transport::smtp::{ScriptedConnector, Transcript};
        use bifrost_types::Account;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dials = Arc::new(AtomicUsize::new(0));
        let dial_count = Arc::clone(&dials);
        let connector = ScriptedConnector::new(move || {
            dial_count.fetch_add(1, Ordering::SeqCst);
            let hello = format!(
                "EHLO {}\r\n",
                bifrost_smtp::transport::smtp::extension::ClientId::default()
            );
            Transcript::new("220 peer\r\n")
                .expect(&hello, "250-peer\r\n250 AUTH PLAIN\r\n")
                .expect("AUTH PLAIN AHVzZXIAcGFzcw==\r\n", "235 authenticated\r\n")
                .expect(&hello, "250 peer\r\n")
                .expect("MAIL FROM:<user@example.com>\r\n", "250 sender\r\n")
                .expect("RCPT TO:<to@example.com>\r\n", "250 recipient\r\n")
                .expect("DATA\r\n", "354 send\r\n")
                .expect("Subject: test\r\n\r\nbody\r\n", "")
                .expect(".\r\n", "250 delivered\r\n")
        });
        let cfg = SmtpSubmissionConfig::new(
            "smtp.invalid",
            SubmissionTls::Plaintext,
            bifrost_types::Address::bare("user@example.com"),
        );
        let submission = Arc::new(
            SubmissionTransport::build_with_connector(
                &cfg,
                &Credentials::password("user", "pass"),
                None,
                bifrost_net::BandwidthBudget::default(),
                Some(connector),
            )
            .unwrap(),
        );
        let envelope = SubmissionEnvelope {
            from: bifrost_types::Address::bare("user@example.com"),
            recipients: vec![bifrost_types::Address::bare("to@example.com")],
        };
        let raw = b"Subject: test\r\n\r\nbody\r\n";
        submission.send_rfc5322(&envelope, raw, None).await.unwrap();
        let (conn, peer) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        drop(peer);
        let mut parts = scripted_account_parts(conn, 1);
        parts.submission = Some(Arc::clone(&submission));
        let account = super::super::ImapAccount::new(parts);
        account.close().await.unwrap();
        // Bypass the account send gate to prove that close shut down SMTP
        // itself, rather than only making the submission unreachable.
        assert!(submission.send_rfc5322(&envelope, raw, None).await.is_err());
        assert_eq!(dials.load(Ordering::SeqCst), 1);
    }

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

    #[derive(Default)]
    struct CountingSink {
        inbound: std::sync::atomic::AtomicU64,
        outbound: std::sync::atomic::AtomicU64,
    }

    impl bifrost_net::MeterSink for CountingSink {
        fn record_bytes_in(&self, _: &bifrost_net::AccountId, n: u64) {
            self.inbound
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        }
        fn record_bytes_out(&self, _: &bifrost_net::AccountId, n: u64) {
            self.outbound
                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        }
    }

    async fn submission_budget_contention(direction: bifrost_net::BandwidthDirection) {
        use bifrost_net::{AccountId, BandwidthBudget, MeterSinkHandle};
        use bifrost_smtp::transport::smtp::{ScriptedConnector, Transcript};
        use futures::FutureExt;
        use std::sync::atomic::Ordering;
        use tokio::time::Instant;

        let hello = format!(
            "EHLO {}\r\n",
            bifrost_smtp::transport::smtp::extension::ClientId::default()
        );
        let steps = [
            (hello.as_str(), "250-peer\r\n250 AUTH PLAIN\r\n"),
            ("AUTH PLAIN AHVzZXIAcGFzcw==\r\n", "235 authenticated\r\n"),
            (hello.as_str(), "250 peer\r\n"),
            ("MAIL FROM:<user@example.com>\r\n", "250 sender\r\n"),
            ("RCPT TO:<to@example.com>\r\n", "250 recipient\r\n"),
            ("DATA\r\n", "354 send\r\n"),
            ("Subject: test\r\n\r\nbody\r\n", ""),
            (".\r\n", "250 delivered\r\n"),
        ];
        let greeting = "220 peer\r\n";
        let expected_in =
            greeting.len() + steps.iter().map(|(_, reply)| reply.len()).sum::<usize>();
        let expected_out = steps
            .iter()
            .map(|(command, _)| command.len())
            .sum::<usize>();
        let steps: Vec<(String, String)> = steps
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect();
        let connector = ScriptedConnector::new(move || {
            steps
                .iter()
                .fold(Transcript::new(greeting), |script, (command, reply)| {
                    script.expect(command, reply)
                })
        });
        let budget = BandwidthBudget::default();
        let sink = Arc::new(CountingSink::default());
        let handle = MeterSinkHandle::new(
            Arc::<CountingSink>::clone(&sink),
            AccountId("submission".into()),
        );
        let cfg = SmtpSubmissionConfig::new(
            "smtp.invalid",
            SubmissionTls::Plaintext,
            bifrost_types::Address::bare("user@example.com"),
        );
        let submission = SubmissionTransport::build_with_connector(
            &cfg,
            &Credentials::password("user", "pass"),
            Some(handle),
            budget.clone(),
            Some(connector),
        )
        .unwrap();
        use crate::account::test_support::scripted_account_parts;
        use crate::connection::test_support::{driver_pair_metered, preauth_greeting};
        use bifrost_types::Account;
        let (conn, peer) =
            driver_pair_metered(&preauth_greeting("IMAP4rev1"), budget.clone()).await;
        let submission = Arc::new(submission);
        let mut parts = scripted_account_parts(conn, 1);
        parts.bandwidth_budget = budget.clone();
        parts.submission = Some(Arc::clone(&submission));
        let account = super::super::ImapAccount::new(parts);
        // A small live account cap splits even commands across several writes.
        account.set_bandwidth_cap(Some(8));
        budget.admit(direction, 8).await.finish(8);
        let envelope = SubmissionEnvelope {
            from: bifrost_types::Address::bare("user@example.com"),
            recipients: vec![bifrost_types::Address::bare("to@example.com")],
        };
        let start = Instant::now();
        let mut send =
            Box::pin(submission.send_rfc5322(&envelope, b"Subject: test\r\n\r\nbody\r\n", None));
        assert!((&mut send).now_or_never().is_none());
        let counter = match direction {
            bifrost_net::BandwidthDirection::Inbound => &sink.inbound,
            bifrost_net::BandwidthDirection::Outbound => &sink.outbound,
            _ => unreachable!(),
        };
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!((&mut send).now_or_never().is_none());
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        send.await.unwrap();
        assert!(start.elapsed() >= Duration::from_secs(1));
        assert_eq!(sink.inbound.load(Ordering::Relaxed), expected_in as u64);
        assert_eq!(sink.outbound.load(Ordering::Relaxed), expected_out as u64);
        drop(peer);
    }

    #[tokio::test(start_paused = true)]
    async fn submission_budget_inbound_io() {
        submission_budget_contention(bifrost_net::BandwidthDirection::Inbound).await;
    }

    #[tokio::test(start_paused = true)]
    async fn submission_budget_outbound_io() {
        submission_budget_contention(bifrost_net::BandwidthDirection::Outbound).await;
    }

    /// A submission whose single dial plays `after_mail_from` once the
    /// greeting, AUTH and MAIL FROM steps are done.
    fn two_recipient_submission(
        after_mail_from: fn(
            bifrost_smtp::transport::smtp::Transcript,
        ) -> bifrost_smtp::transport::smtp::Transcript,
    ) -> (SubmissionTransport, SubmissionEnvelope) {
        use bifrost_smtp::transport::smtp::{ScriptedConnector, Transcript};
        let connector = ScriptedConnector::new(move || {
            let hello = format!(
                "EHLO {}\r\n",
                bifrost_smtp::transport::smtp::extension::ClientId::default()
            );
            after_mail_from(
                Transcript::new("220 peer\r\n")
                    .expect(&hello, "250-peer\r\n250 AUTH PLAIN\r\n")
                    .expect("AUTH PLAIN AHVzZXIAcGFzcw==\r\n", "235 authenticated\r\n")
                    .expect(&hello, "250 peer\r\n")
                    .expect("MAIL FROM:<user@example.com>\r\n", "250 sender\r\n"),
            )
        });
        let cfg = SmtpSubmissionConfig::new(
            "smtp.invalid",
            SubmissionTls::Plaintext,
            bifrost_types::Address::bare("user@example.com"),
        );
        let submission = SubmissionTransport::build_with_connector(
            &cfg,
            &Credentials::password("user", "pass"),
            None,
            bifrost_net::BandwidthBudget::default(),
            Some(connector),
        )
        .unwrap();
        let envelope = SubmissionEnvelope {
            from: bifrost_types::Address::bare("user@example.com"),
            recipients: vec![
                bifrost_types::Address::bare("a@example.com"),
                bifrost_types::Address::bare("b@example.com"),
            ],
        };
        (submission, envelope)
    }

    #[tokio::test]
    async fn a_partial_send_reports_the_refused_recipient_on_success() {
        let (submission, envelope) = two_recipient_submission(|script| {
            script
                .expect("RCPT TO:<a@example.com>\r\n", "250 recipient\r\n")
                .expect("RCPT TO:<b@example.com>\r\n", "550 no such user\r\n")
                .expect("DATA\r\n", "354 send\r\n")
                .expect("Subject: test\r\n\r\nbody\r\n", "")
                .expect(".\r\n", "250 delivered\r\n")
        });
        let not_accepted = submission
            .send_rfc5322(&envelope, b"Subject: test\r\n\r\nbody\r\n", None)
            .await
            .expect("a recipient accepted, so the send is a success");
        assert_eq!(not_accepted.len(), 1);
        assert_eq!(not_accepted[0].address.address, "b@example.com");
        assert_eq!(not_accepted[0].outcome, RecipientOutcome::Failed);
    }

    #[tokio::test]
    async fn with_no_acceptance_an_uncertain_recipient_outranks_a_refused_one() {
        // B refused at RCPT, A accepted, the body sent and the peer gone
        // before the final reply: A may have the message, so the returned
        // error must say so rather than speak with B's refusal.
        let (submission, envelope) = two_recipient_submission(|script| {
            script
                .expect("RCPT TO:<a@example.com>\r\n", "250 recipient\r\n")
                .expect("RCPT TO:<b@example.com>\r\n", "450 try later\r\n")
                .expect("DATA\r\n", "354 send\r\n")
                .expect("Subject: test\r\n\r\nbody\r\n", "")
                .expect(".\r\n", "")
        });
        let err = submission
            .send_rfc5322(&envelope, b"Subject: test\r\n\r\nbody\r\n", None)
            .await
            .expect_err("no recipient acknowledged acceptance");
        assert_eq!(
            bifrost_types::error::Application::from_leaf(&err),
            bifrost_types::error::Application::Uncertain,
            "{err}"
        );
    }

    #[tokio::test]
    async fn malformed_recipient_is_failed_without_emitting_its_rcpt_command() {
        let (submission, mut envelope) = two_recipient_submission(|script| {
            script
                .expect("RCPT TO:<b@example.com>\r\n", "250 recipient\r\n")
                .expect("DATA\r\n", "354 send\r\n")
                .expect("Subject: test\r\n\r\nbody\r\n", "")
                .expect(".\r\n", "250 delivered\r\n")
        });
        envelope.recipients[0] =
            bifrost_types::Address::bare("a@b.example> NOTIFY=SUCCESS ORCPT=rfc822;x");
        let failed = submission
            .send_rfc5322(&envelope, b"Subject: test\r\n\r\nbody\r\n", None)
            .await
            .unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].address.address, envelope.recipients[0].address);
        assert_eq!(failed[0].outcome, RecipientOutcome::Failed);
    }

    #[tokio::test]
    async fn invalid_envelope_addresses_are_refused_before_dial() {
        use bifrost_smtp::transport::smtp::ScriptedConnector;
        let cfg = SmtpSubmissionConfig::new(
            "smtp.invalid",
            SubmissionTls::Plaintext,
            bifrost_types::Address::bare("user@example.com"),
        );
        let submission = SubmissionTransport::build_with_connector(
            &cfg,
            &Credentials::password("user", "pass"),
            None,
            bifrost_net::BandwidthBudget::default(),
            Some(ScriptedConnector::new(|| {
                panic!("invalid envelope must not dial")
            })),
        )
        .unwrap();
        let invalid = bifrost_types::Address::bare("a@b.example> NOTIFY=SUCCESS");
        for envelope in [
            SubmissionEnvelope {
                from: invalid.clone(),
                recipients: vec![bifrost_types::Address::bare("to@example.com")],
            },
            SubmissionEnvelope {
                from: cfg.default_from.clone(),
                recipients: vec![invalid],
            },
        ] {
            assert!(
                submission
                    .send_rfc5322(&envelope, b"body", None)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn unrepresentable_hold_is_refused_before_dial() {
        use bifrost_smtp::transport::smtp::ScriptedConnector;
        let cfg = SmtpSubmissionConfig::new(
            "smtp.invalid",
            SubmissionTls::Plaintext,
            bifrost_types::Address::bare("user@example.com"),
        );
        let submission = SubmissionTransport::build_with_connector(
            &cfg,
            &Credentials::password("user", "pass"),
            None,
            bifrost_net::BandwidthBudget::default(),
            Some(ScriptedConnector::new(|| {
                panic!("invalid hold must not dial")
            })),
        )
        .unwrap();
        let envelope = SubmissionEnvelope {
            from: cfg.default_from.clone(),
            recipients: vec![bifrost_types::Address::bare("to@example.com")],
        };
        let at = std::time::UNIX_EPOCH + Duration::from_secs(1_000_000_000_000);
        assert!(jiff::Timestamp::try_from(at).is_err());
        let error = submission
            .send_rfc5322(&envelope, b"body", Some(at))
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed),
        );
    }

    #[test]
    fn scheduled_send_builds_holduntil_mail_parameter() {
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_800_000_000);
        let options = build_send_options(Some(at)).unwrap();
        assert!(
            options.mail_parameters().iter().any(|p| matches!(
                p,
                MailParameter::FutureRelease(FutureReleaseParameter::HoldUntil(_))
            )),
            "scheduled send must emit a FutureRelease(HoldUntil) mail parameter"
        );

        // An immediate send carries no FUTURERELEASE parameter.
        let immediate = build_send_options(None).unwrap();
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
