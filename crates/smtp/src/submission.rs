//! Account-oriented SMTP submission in shared address and error vocabulary.
use crate::transport::smtp::{SendOptions, SubmissionStatus, submission_status};
use crate::{Address as SmtpAddress, AsyncSmtpTransport};
use bifrost_types::error::{AccountError, BatchItem, BatchItemId};
use bifrost_types::mime::SubmissionEnvelope;
use bifrost_types::{RecipientNotAccepted, RecipientOutcome};

/// Shared stream encryption policy.
pub use bifrost_net::credentials::TlsMode as SubmissionTls;

/// Shared zeroizing password or live OAuth submission credentials.
pub use bifrost_net::Credentials as SubmissionCredentials;

/// Fully configurable transport builder for account submission.
/// Retains all low-level SMTP builder options, including metering and test transports.
pub struct SubmissionConfig {
    /// Configured asynchronous SMTP transport builder.
    pub transport: crate::transport::smtp::AsyncSmtpTransportBuilder,
}

impl SubmissionConfig {
    /// Construct submission defaults while retaining access to every transport option.
    pub fn new(host: &str, tls: SubmissionTls) -> Result<Self, crate::transport::smtp::Error> {
        let transport = match tls {
            SubmissionTls::Implicit => AsyncSmtpTransport::relay(host)?,
            SubmissionTls::StartTls => AsyncSmtpTransport::starttls_relay(host)?,
            SubmissionTls::None => {
                AsyncSmtpTransport::builder_dangerous(host).dangerous_allow_insecure_auth(true)
            }
            _ => {
                return Err(crate::transport::smtp::error::invalid_input(
                    "unsupported TLS mode",
                ));
            }
        }
        .port(tls.port_for(bifrost_net::credentials::TlsService::Submission));
        Ok(Self { transport })
    }
}

/// Result of a committed submission, retaining each recipient that was not accepted.
pub struct SubmissionReceipt {
    /// Failed and uncertain recipients; accepted recipients must not be resent.
    pub recipients_not_accepted: Vec<RecipientNotAccepted>,
}

/// SMTP submission facade. Any accepted recipient commits the send; failed and
/// uncertain recipients are returned individually so users do not resend to all.
pub struct Submission {
    transport: AsyncSmtpTransport,
}

impl From<SubmissionConfig> for Submission {
    fn from(config: SubmissionConfig) -> Self {
        Self {
            transport: config.transport.build(),
        }
    }
}

impl Submission {
    /// Close the owned SMTP pool.
    pub async fn shutdown(&self) {
        crate::AsyncTransport::shutdown(&self.transport).await;
    }

    /// Submit the RFC 5322 bytes over SMTP using `envelope` as the
    /// reverse path / recipient set. This is the only place the
    /// `bifrost_types::Address -> bifrost_smtp::Address` conversion
    /// happens. Returns the already-translated `AccountError` from
    /// `bifrost-smtp` on failure, and on success the recipients that were
    /// not accepted while another one was (empty on a clean send). Their
    /// errors are not yet stamped with the caller's operation.
    pub async fn submit(
        &self,
        envelope: &SubmissionEnvelope,
        raw: &[u8],
        hold: Option<std::time::SystemTime>,
    ) -> Result<SubmissionReceipt, AccountError> {
        let from = SmtpAddress::try_from(&envelope.from)
            .map_err(|_| submission_invalid("invalid SMTP reverse-path address"))?;
        let options = build_send_options(hold)?;
        let mut locally_failed = Vec::new();
        let recipients: Vec<BatchItem<(SmtpAddress, bifrost_types::Address)>> = envelope
            .recipients
            .iter()
            .enumerate()
            .filter_map(|(index, address)| match SmtpAddress::try_from(address) {
                Ok(smtp_address) => Some(BatchItem::new(
                    // An opaque caller token; the returned payload owns the
                    // original address, so no reader decodes this spelling.
                    BatchItemId(format!("recipient-{index}")),
                    (smtp_address, address.clone()),
                )),
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
            .send_raw_batch_with_payload(Some(from), recipients, raw, &options)
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
            // A send with no acknowledged recipient remains an error carrying
            // uncertainty: enterprise users need reconciliation before retrying.
            SubmissionStatus::AcceptanceUncertain(error) | SubmissionStatus::Rejected(error) => {
                return Err(error.clone());
            }
            SubmissionStatus::Empty => {
                return Ok(SubmissionReceipt {
                    recipients_not_accepted: locally_failed,
                });
            }
        }
        let mut not_accepted = locally_failed;
        not_accepted.extend(not_accepted_recipients(&outcome));
        #[cfg(feature = "tracing")]
        if !not_accepted.is_empty() {
            tracing::warn!(
                delivered = outcome.succeeded().len(),
                failed = not_accepted
                    .iter()
                    .filter(|r| matches!(r.outcome, RecipientOutcome::Failed))
                    .count(),
                uncertain = not_accepted
                    .iter()
                    .filter(|r| matches!(r.outcome, RecipientOutcome::Uncertain))
                    .count(),
                "SMTP submission partially accepted"
            );
        }
        Ok(SubmissionReceipt {
            recipients_not_accepted: not_accepted,
        })
    }
}

/// Return the original caller address from each failed or uncertain lane.
/// LMTP may omit rejected recipients from its final-status drain, so neither
/// lane position nor an address reparsed from an identifier can identify them.
fn not_accepted_recipients<T>(
    outcome: &bifrost_types::BatchOutcome<T, BatchItem<bifrost_types::Address>>,
) -> Vec<RecipientNotAccepted> {
    let failed = outcome.failed().iter().map(|lane| {
        RecipientNotAccepted::new(
            lane.item.input.clone(),
            RecipientOutcome::Failed,
            lane.error.clone(),
        )
    });
    let uncertain = outcome.uncertain().iter().map(|lane| {
        RecipientNotAccepted::new(
            lane.item.input.clone(),
            RecipientOutcome::Uncertain,
            lane.error.clone(),
        )
    });
    failed.chain(uncertain).collect()
}

/// Build the SMTP `SendOptions` for a submission. When `hold` is
/// `Some`, FUTURERELEASE rides as the absolute-time HOLDUNTIL parameter
/// so the boundary does not race `now()`; the relay computes the delay
/// and decides support (an unsupporting relay yields an
/// `Unsupported(Send)` `AccountError` from the smtp layer).
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
    use bifrost_types::{
        AccountErrorBuilder, AccountOperation, DiagnosticText, Protocol, RequestCause,
    };
    AccountErrorBuilder::new(bifrost_types::PrimaryCause::Request(
        RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail),
        },
    ))
    // The SMTP facade owns envelope and HOLDUNTIL validation, even when
    // composed into IMAP. Support needs the failing transport vocabulary.
    .protocol(Protocol::Smtp)
    .operation(AccountOperation::Send)
    .text(DiagnosticText::support_only(detail))
    .try_build()
    .expect("valid submission input error")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::smtp::extension::{FutureReleaseParameter, MailParameter};
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
}
