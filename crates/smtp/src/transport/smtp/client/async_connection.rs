use std::net::IpAddr;
#[cfg(unix)]
use std::path::Path;
use std::{
    fmt::{Display, Write as _},
    future::Future,
    time::Duration,
};

use bifrost_sasl::ScramChannelBinding;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use zeroize::{Zeroize, Zeroizing};

use super::async_net::AsyncDeadline;
#[cfg(feature = "tracing")]
use super::escape_crlf;
use super::metering::WireMetering;
use super::{
    ClientCodec, ConnectionState, MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES, TlsParameters,
    async_net::AsyncNetworkStream,
    core,
    core::{Op, OpOutcome, ProtocolMachine, Step},
    data_terminator, smtp_data_size,
};
use crate::{
    Envelope,
    address::Address,
    transport::smtp::{
        Protocol,
        authentication::{
            Credentials, Mechanism, ScramExchange, ScramStep, decode_auth_challenge,
            decode_scram_payload, oauth_mechanism, password_mechanism, resolve_scram_binding,
            scram_hash,
        },
        batch::{RecipientProgress, SendProgress, SmtpBatchRecipient},
        commands::{
            Auth, Ehlo, Expn, Lhlo, Mail, Noop, Rcpt, Starttls, Vrfy, build_recipient_commands,
            build_transaction_commands,
        },
        error,
        error::{Error, SmtpCommandPhase, SmtpTransmissionState},
        extension::{
            ClientId, DeliverByMode, Extension, FutureReleaseParameter, MailBodyParameter,
            MailParameter, RcptParameter, SendOptions, ServerInfo,
            addresses_match_for_recipient_options,
        },
        response::{Response, parse_response},
    },
};

macro_rules! try_smtp (
    ($err: expr, $client: ident) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort().await;
                return Err(From::from(err))
            },
        }
    });
    // Phase-tagged variant (mirrors the sync sibling). Stamps the SMTP
    // error with the given SmtpCommandPhase so the translation
    // boundary in `account_error.rs` can route per-phase.
    // Set-if-absent (`or_phase`): a multi-phase callee (the LMTP body
    // upload + final-status drain) stamps its own finer-grained phase
    // inside, and this wrapper must not overwrite it.
    ($err: expr, $client: ident, $phase: expr) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort().await;
                return Err(From::from(err.or_phase($phase)))
            },
        }
    });
);

/// Lift an adapter result into the outcome the protocol core consumes.
fn op_done(result: Result<(), Error>) -> OpOutcome {
    match result {
        Ok(()) => OpOutcome::Done,
        Err(error) => OpOutcome::Failed(error),
    }
}

fn op_reply(result: Result<Response, Error>) -> OpOutcome {
    match result {
        Ok(response) => OpOutcome::Reply(response),
        Err(error) => OpOutcome::Failed(error),
    }
}

#[derive(Clone, Copy, Debug)]
enum TimeoutBudget {
    PerOperation(Option<Duration>),
    SetupDeadline(AsyncDeadline),
}

/// Remaining peer I/O time for a whole reply, or an absolute setup ceiling.
#[derive(Clone, Copy, Debug)]
enum ReplyBudget {
    Own(Option<Duration>),
    Setup(AsyncDeadline),
}
impl ReplyBudget {
    fn new(budget: TimeoutBudget) -> Self {
        match budget {
            TimeoutBudget::PerOperation(timeout) => Self::Own(timeout),
            TimeoutBudget::SetupDeadline(deadline) => Self::Setup(deadline),
        }
    }
    fn budget(self) -> TimeoutBudget {
        match self {
            Self::Own(timeout) => TimeoutBudget::PerOperation(timeout),
            Self::Setup(deadline) => TimeoutBudget::SetupDeadline(deadline),
        }
    }
    fn spend(&mut self, elapsed: Duration) {
        if let Self::Own(Some(left)) = self {
            *left = left.saturating_sub(elapsed);
        }
    }
}

async fn with_timeout<T, F>(
    budget: TimeoutBudget,
    message: &'static str,
    future: F,
) -> Result<T, Error>
where
    F: Future<Output = T>,
{
    let timeout = match budget {
        TimeoutBudget::PerOperation(timeout) => timeout,
        TimeoutBudget::SetupDeadline(deadline) => deadline.remaining(message)?,
    };

    match timeout {
        None => Ok(future.await),
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| error::timeout(message)),
    }
}

/// Hard ceiling on how long an async teardown may await `poll_shutdown`,
/// independent of the connection's configured operation timeout.
///
/// `timeout(None)` means "do not limit my protocol operations" - a caller
/// saying a large message may take as long as it takes. Reading "hang forever
/// tearing down a connection I have finished with" out of that is the bug: past
/// `abort()` there is nothing left to accomplish but a courtesy `close_notify`,
/// and a peer that will not answer it has already said what it needed to say.
/// A TLS `poll_shutdown` waits for the peer's `close_notify`, so an
/// unresponsive peer would otherwise park the caller forever - and
/// `Pool::shutdown` WAITS on the closes it runs concurrently, so one wedged
/// peer would hang pool shutdown, where a hang is least recoverable.
const TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Structure that implements the SMTP client
pub(crate) struct AsyncSmtpConnection {
    /// TCP stream between client and server
    /// Value is None before connection
    stream: BufReader<AsyncNetworkStream>,
    /// Information about the server
    server_info: ServerInfo,
    /// Client identity used for EHLO.
    hello_name: ClientId,
    /// Wire protocol used for this connection.
    protocol: Protocol,
    /// Timeout applied to each async SMTP I/O operation.
    timeout: Option<Duration>,
    /// Reused storage for serializing individual SMTP commands.
    command_buffer: Zeroizing<String>,
    /// Set once an LMTP final-status drain has run: the connection must not
    /// go back into the pool because stream cleanliness cannot be proven.
    retire: bool,
}

impl AsyncSmtpConnection {
    /// Get information about the server
    pub(crate) fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    fn per_operation_budget(&self) -> TimeoutBudget {
        TimeoutBudget::PerOperation(self.timeout)
    }

    /// Budget for teardown: the smaller of the configured operation timeout and
    /// `TEARDOWN_TIMEOUT`, so the bound binds whether or not a timeout is set.
    fn teardown_budget(&self) -> TimeoutBudget {
        TimeoutBudget::PerOperation(Some(
            self.timeout
                .map_or(TEARDOWN_TIMEOUT, |timeout| timeout.min(TEARDOWN_TIMEOUT)),
        ))
    }

    /// Connects to the configured server
    ///
    /// If `tls_parameters` is `Some`, then the connection will use Implicit TLS (sometimes
    /// referred to as `SMTPS`). See also [`AsyncSmtpConnection::starttls`].
    ///
    /// If `local_address` is `Some`, then the address provided shall be used to bind the
    /// connection to a specific local address using [`tokio::net::TcpSocket::bind`].
    ///
    /// Sends EHLO and parses server information
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # use bifrost_smtp::transport::smtp::{AsyncSmtpConnection, TlsParameters, extension::ClientId};
    /// # use tokio::net::ToSocketAddrs as _;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let connection = AsyncSmtpConnection::connect(
    ///     ("example.com", 465),
    ///     Some(Duration::from_secs(10)),
    ///     &ClientId::default(),
    ///     Some(TlsParameters::new("example.com".to_owned())?),
    ///     None,
    /// )
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(test)]
    #[allow(dead_code)]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    pub(crate) async fn connect<T: tokio::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
        local_address: Option<IpAddr>,
    ) -> Result<AsyncSmtpConnection, Error> {
        Self::connect_with_protocol(
            server,
            timeout,
            hello_name,
            tls_parameters,
            local_address,
            Protocol::Smtp,
            WireMetering::disabled(),
            #[cfg(feature = "test-support")]
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn connect_with_protocol<T: tokio::net::ToSocketAddrs>(
        server: T,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<TlsParameters>,
        local_address: Option<IpAddr>,
        protocol: Protocol,
        metering: WireMetering,
        #[cfg(feature = "test-support")] connector: Option<
            crate::transport::smtp::test_support::ScriptedConnector,
        >,
    ) -> Result<AsyncSmtpConnection, Error> {
        let deadline = AsyncDeadline::new(timeout);
        #[cfg(feature = "test-support")]
        if connector.is_some() && tls_parameters.is_some() {
            return Err(error::invalid_input(
                "scripted connectors require plaintext",
            ));
        }
        #[cfg(feature = "test-support")]
        let scripted =
            connector.map(|connector| AsyncNetworkStream::from_transcript(connector.connect()));
        let mut stream = {
            #[cfg(feature = "test-support")]
            if let Some(stream) = scripted {
                stream
            } else {
                AsyncNetworkStream::connect_until(server, deadline, tls_parameters, local_address)
                    .await?
            }
            #[cfg(not(feature = "test-support"))]
            AsyncNetworkStream::connect_until(server, deadline, tls_parameters, local_address)
                .await?
        };
        // Installed on the dialed stream rather than threaded into the
        // dialer: the TCP/TLS handshake is not the account's traffic.
        stream.set_metering(metering);
        Self::connect_impl(
            stream,
            hello_name,
            timeout,
            TimeoutBudget::SetupDeadline(deadline),
            protocol,
        )
        .await
    }

    #[cfg(unix)]
    pub(crate) async fn connect_unix_with_protocol(
        path: &Path,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        protocol: Protocol,
        metering: WireMetering,
    ) -> Result<AsyncSmtpConnection, Error> {
        let deadline = AsyncDeadline::new(timeout);
        let mut stream = AsyncNetworkStream::connect_unix_until(path, deadline).await?;
        stream.set_metering(metering);
        Self::connect_impl(
            stream,
            hello_name,
            timeout,
            TimeoutBudget::SetupDeadline(deadline),
            protocol,
        )
        .await
    }

    async fn connect_impl(
        stream: AsyncNetworkStream,
        hello_name: &ClientId,
        timeout: Option<Duration>,
        setup_budget: TimeoutBudget,
        protocol: Protocol,
    ) -> Result<AsyncSmtpConnection, Error> {
        let stream = BufReader::new(stream);
        let mut conn = AsyncSmtpConnection {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            timeout,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
        };
        let _response = conn.read_response_with_budget(setup_budget).await?;

        conn.hello_with_budget(hello_name, setup_budget).await?;

        // Print server information
        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    // Widened so the pool and transport tests can drive a scripted peer
    // through the public entry points instead of a socket.
    #[cfg(test)]
    pub(in crate::transport::smtp) async fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
    ) -> Result<Self, Error> {
        Self::connect_impl(
            AsyncNetworkStream::from_transcript(transcript),
            hello_name,
            None,
            TimeoutBudget::PerOperation(None),
            protocol,
        )
        .await
    }

    /// Transcript setup with byte accounting installed, so a test can
    /// observe what the real send path would have metered.
    #[cfg(test)]
    pub(in crate::transport::smtp) async fn from_transcript_metered(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
        metering: WireMetering,
    ) -> Result<Self, Error> {
        let mut stream = AsyncNetworkStream::from_transcript(transcript);
        stream.set_metering(metering);
        Self::connect_impl(
            stream,
            hello_name,
            None,
            TimeoutBudget::PerOperation(None),
            protocol,
        )
        .await
    }

    /// Transcript setup with a peer-certificate DER injected on the stream,
    /// so the connection-level channel-binding gate in `auth` (resolve the
    /// binding only when an allowed PLUS mechanism is advertised, and treat a
    /// present-but-unusable certificate as a hard error) is testable without
    /// TLS.
    #[cfg(test)]
    pub(in crate::transport::smtp) async fn from_transcript_with_peer_certificate(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
        peer_certificate_der: Vec<u8>,
    ) -> Result<Self, Error> {
        let mut stream = AsyncNetworkStream::from_transcript(transcript);
        stream.set_test_peer_certificate_der(peer_certificate_der);
        Self::connect_impl(
            stream,
            hello_name,
            None,
            TimeoutBudget::PerOperation(None),
            protocol,
        )
        .await
    }

    /// A connection over an arbitrary scripted stream, with no banner and no
    /// EHLO. For peers whose traffic a `Transcript` cannot express - notably
    /// one that trickles reply lines on a clock, which is what separates a
    /// per-reply read deadline from a per-line one.
    #[cfg(test)]
    pub(in crate::transport::smtp) fn from_raw_stream_for_test(
        stream: Box<dyn super::async_net::AsyncTokioStream>,
        hello_name: &ClientId,
        protocol: Protocol,
        timeout: Option<Duration>,
    ) -> Self {
        AsyncSmtpConnection {
            stream: BufReader::new(AsyncNetworkStream::from_raw_stream_for_test(stream)),
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            timeout,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
        }
    }

    /// Transcript setup that goes through the same single setup deadline the
    /// real `connect` path uses, so banner and EHLO share one budget.
    #[cfg(test)]
    pub(in crate::transport::smtp) async fn from_transcript_with_timeout(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let deadline = AsyncDeadline::new(Some(timeout));
        Self::connect_impl(
            AsyncNetworkStream::from_transcript(transcript),
            hello_name,
            Some(timeout),
            TimeoutBudget::SetupDeadline(deadline),
            protocol,
        )
        .await
    }

    pub(crate) async fn send(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Response, Error> {
        self.send_with_options(envelope, email, &SendOptions::default())
            .await
    }

    /// Build and validate the whole envelope before `MAIL FROM` is written.
    ///
    /// A construction failure raised from inside an open transaction would
    /// unwind past the abort a wire failure runs, leaving the connection `Ok`,
    /// poolable, and holding a half-open transaction. Every send path builds
    /// first for that reason.
    fn build_envelope(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
        allow_binary_mime: bool,
    ) -> Result<(Mail, Vec<Rcpt>), Error> {
        let mail_options = self.mail_options(envelope, email, options, allow_binary_mime)?;
        let rcpt_options = self.rcpt_options(envelope, options)?;
        build_transaction_commands(envelope, mail_options, &rcpt_options)
    }

    pub(crate) async fn send_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        let (mail, recipients) = self.build_envelope(envelope, email, options, false)?;
        let pipelined = self.server_info().supports_pipelining();
        let mut machine = core::DirectSmtp::new(mail, recipients, pipelined, core::BodyKind::Data);
        self.drive(&mut machine, email).await
    }

    pub(crate) async fn send_bdat_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        if !self.server_info().supports_chunking() {
            return Err(error::feature_unsupported(
                "BDAT requires server CHUNKING support",
            ));
        }

        let (mail, recipients) = self.build_envelope(envelope, email, options, true)?;
        // BDAT is never pipelined: the path has no window accounting and has
        // never been driven that way, on either half.
        let mut machine = core::DirectSmtp::new(mail, recipients, false, core::BodyKind::Bdat);
        self.drive(&mut machine, email).await
    }

    pub(crate) async fn send_lmtp(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Vec<Response>, Error> {
        self.send_lmtp_with_options(envelope, email, &SendOptions::default())
            .await
    }

    pub(crate) async fn send_lmtp_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        let (mail, recipients) = self.build_envelope(envelope, email, options, false)?;
        let mut machine = core::DirectLmtp::new(mail, recipients, core::BodyKind::Data);
        self.drive(&mut machine, email).await
    }

    pub(crate) async fn send_lmtp_bdat_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        if !self.server_info().supports_chunking() {
            return Err(error::feature_unsupported(
                "BDAT requires server CHUNKING support",
            ));
        }

        let (mail, recipients) = self.build_envelope(envelope, email, options, true)?;
        let mut machine = core::DirectLmtp::new(mail, recipients, core::BodyKind::Bdat);
        self.drive(&mut machine, email).await
    }

    /// Async account-oriented SMTP multi-recipient send.
    ///
    /// Returns `Ok(progress)` when the command sequence completes (even if
    /// some recipients were rejected). Returns `Err((error, progress))` for
    /// batch-level failures where no recipient-specific outcome can be
    /// attributed.
    pub(crate) async fn send_smtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        let (mail_cmd, rcpt_cmds, progress) =
            self.build_batch(Protocol::Smtp, from, recipients, email, options)?;
        let pipelined = self.server_info().supports_pipelining();
        let mut machine = core::BatchSmtp::new(mail_cmd, rcpt_cmds, pipelined, progress);
        self.drive(&mut machine, email).await
    }

    /// Async account-oriented LMTP multi-recipient send.
    ///
    /// Like `send_smtp_batch` but reads one final status per accepted
    /// recipient after the DATA body.
    pub(crate) async fn send_lmtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        let (mail_cmd, rcpt_cmds, progress) =
            self.build_batch(Protocol::Lmtp, from, recipients, email, options)?;
        let mut machine = core::BatchLmtp::new(mail_cmd, rcpt_cmds, progress);
        self.drive(&mut machine, email).await
    }

    /// Local validation and command construction for a batch send, all of it
    /// before `MAIL FROM` opens a transaction (see `build_envelope`).
    #[allow(clippy::type_complexity)]
    fn build_batch(
        &self,
        protocol: Protocol,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<(Mail, Vec<Rcpt>, SendProgress), (Error, SendProgress)> {
        let progress = SendProgress::new(protocol, recipients);
        let rcpt_options_all = self
            .rcpt_options_for_batch(&progress.recipients, options)
            .map_err(|error| {
                (
                    error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::RcptTo),
                    progress.clone(),
                )
            })?;

        let mail_options = self
            .mail_options_for_batch(from.as_ref(), &progress.recipients, email, options, false)
            .map_err(|error| {
                (
                    error
                        .with_attempt(SmtpTransmissionState::Unsent)
                        .with_phase(SmtpCommandPhase::MailFrom),
                    progress.clone(),
                )
            })?;

        let mail_cmd = Mail::new(from, mail_options).map_err(|error| {
            (
                error
                    .with_attempt(SmtpTransmissionState::Unsent)
                    .with_phase(SmtpCommandPhase::MailFrom),
                progress.clone(),
            )
        })?;
        let rcpt_cmds = build_recipient_commands(
            progress.recipients.iter().map(|r| r.address.clone()),
            &rcpt_options_all,
        )
        .map_err(|e| (e, progress.clone()))?;

        Ok((mail_cmd, rcpt_cmds, progress))
    }

    /// Run a sans-I/O protocol machine to completion.
    ///
    /// This loop is the whole async adapter: it moves bytes under the
    /// per-operation deadline and reports what happened. Which command follows
    /// which, how many replies a window owes, when a transaction is reset
    /// versus abandoned, and which phase decorates a failure are all decided
    /// in `client::core`, so neither half of this crate can drift from the
    /// other.
    ///
    /// Cancel-safety is unchanged by the indirection: a dropped future leaves
    /// the stream in whatever state the op it was inside had set, and every op
    /// that opens a reply group leaves it `Broken` until the group closes.
    async fn drive<M: ProtocolMachine>(&mut self, machine: &mut M, email: &[u8]) -> M::Output {
        let mut outcome = OpOutcome::Done;
        loop {
            match machine.step(outcome) {
                Step::Finish(output) => return output,
                Step::Run(op) => outcome = self.perform(op, email).await,
            }
        }
    }

    async fn perform(&mut self, op: Op, email: &[u8]) -> OpOutcome {
        match op {
            Op::Write(bytes) => op_done(self.write(bytes.as_bytes()).await),
            Op::WriteBody => op_done(self.write_body(email).await),
            Op::WriteBdat => op_done(self.write_bdat_body(email).await),
            Op::OpenReplyGroup => op_done(self.open_reply_group()),
            Op::ReadGrouped => op_reply(
                self.read_response_with_budget_inner(self.per_operation_budget(), true, false)
                    .await,
            ),
            Op::ReadSingle => op_reply(
                self.read_response_with_budget_inner(self.per_operation_budget(), true, true)
                    .await,
            ),
            Op::CloseReplyGroup => op_done(self.finish_reply_group()),
            Op::CloseLmtpDrain { restore_ok } => {
                let result = self.finish_lmtp_final_drain();
                if result.is_ok() && restore_ok {
                    self.stream.get_mut().set_state(ConnectionState::Ok);
                }
                op_done(result)
            }
            Op::Abort => {
                self.abort().await;
                OpOutcome::Done
            }
        }
    }

    /// Hold the stream `Broken` for a whole reply group, so a connection
    /// abandoned mid-drain can never be recycled.
    fn open_reply_group(&mut self) -> Result<(), Error> {
        self.stream.get_ref().state().verify()?;
        self.stream.get_mut().set_state(ConnectionState::Broken);
        Ok(())
    }

    /// The BDAT chunk header and its raw payload, as two writes, which is the
    /// framing the transcript suites pin.
    async fn write_bdat_body(&mut self, email: &[u8]) -> Result<(), Error> {
        self.write(core::bdat_header(email.len()).as_bytes())
            .await?;
        self.write(email).await
    }

    /// Write the DATA body without reading the final reply.
    async fn write_body(&mut self, email: &[u8]) -> Result<(), Error> {
        self.write_body_iter(std::iter::once(email)).await
    }

    /// Compute RCPT TO parameters for a single recipient and the given options.
    fn rcpt_options_single(
        &self,
        addr: &Address,
        options: &SendOptions,
    ) -> Result<Vec<RcptParameter>, Error> {
        let parameters = options.rcpt_parameters_for(addr);
        for parameter in &parameters {
            self.validate_rcpt_parameter(parameter)?;
        }
        Ok(parameters)
    }

    fn rcpt_options_for_batch(
        &self,
        recipients: &[RecipientProgress],
        options: &SendOptions,
    ) -> Result<Vec<Vec<RcptParameter>>, Error> {
        for (recipient, _) in options.recipient_parameters() {
            if !recipients.iter().any(|batch_recipient| {
                addresses_match_for_recipient_options(recipient, &batch_recipient.address)
            }) {
                return Err(error::invalid_input(
                    "recipient-specific RCPT parameters do not match a batch recipient",
                ));
            }
        }

        recipients
            .iter()
            .map(|recipient| self.rcpt_options_single(&recipient.address, options))
            .collect()
    }

    /// Like `mail_options` but takes an explicit sender address instead of an `Envelope`.
    fn mail_options_for_batch(
        &self,
        from: Option<&Address>,
        recipients: &[RecipientProgress],
        email: &[u8],
        options: &SendOptions,
        allow_binary_mime: bool,
    ) -> Result<Vec<MailParameter>, Error> {
        let mut mail_options = vec![];
        let message_size = if allow_binary_mime {
            email.len()
        } else {
            smtp_data_size(email)
        };

        let has_smtputf8 = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::SmtpUtfEight));
        let has_body_parameter = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::Body(_)));

        let has_non_ascii = from.is_some_and(|a| !AsRef::<str>::as_ref(a).is_ascii())
            || recipients
                .iter()
                .any(|recipient| !AsRef::<str>::as_ref(&recipient.address).is_ascii());
        if has_non_ascii && !has_smtputf8 {
            if !self.server_info().supports_feature(Extension::SmtpUtfEight) {
                return Err(error::feature_unsupported(
                    "Envelope contains non-ascii chars but server does not support SMTPUTF8",
                ));
            }
            mail_options.push(MailParameter::SmtpUtfEight);
        }

        if !email.is_ascii() && !has_body_parameter {
            if !self.server_info().supports_feature(Extension::EightBitMime) {
                return Err(error::feature_unsupported(
                    "Message contains non-ascii chars but server does not support 8BITMIME",
                ));
            }
            mail_options.push(MailParameter::Body(MailBodyParameter::EightBitMime));
        }

        if self.server_info().supports_size()
            && !options
                .mail_parameters()
                .iter()
                .any(|parameter| matches!(parameter, MailParameter::Size(_)))
        {
            if self
                .server_info()
                .size_limit()
                .is_some_and(|limit| message_size > limit)
            {
                return Err(error::invalid_input(
                    "Message is larger than the server-advertised SIZE limit",
                ));
            }
            mail_options.push(MailParameter::Size(message_size));
        }

        for parameter in options.mail_parameters() {
            self.validate_mail_parameter(
                parameter,
                message_size,
                email.is_ascii(),
                allow_binary_mime,
            )?;
            mail_options.push(parameter.clone());
        }

        Ok(mail_options)
    }

    fn mail_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
        allow_binary_mime: bool,
    ) -> Result<Vec<MailParameter>, Error> {
        // Mail
        let mut mail_options = vec![];
        let message_size = if allow_binary_mime {
            email.len()
        } else {
            smtp_data_size(email)
        };

        // Internationalization handling
        //
        // * 8BITMIME: https://tools.ietf.org/html/rfc6152
        // * SMTPUTF8: https://tools.ietf.org/html/rfc653

        // Check for non-ascii addresses and use the SMTPUTF8 option if any.
        let has_smtputf8 = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::SmtpUtfEight));
        let has_body_parameter = options
            .mail_parameters()
            .iter()
            .any(|parameter| matches!(parameter, MailParameter::Body(_)));

        if envelope.has_non_ascii_addresses() && !has_smtputf8 {
            if !self.server_info().supports_feature(Extension::SmtpUtfEight) {
                // don't try to send non-ascii addresses (per RFC)
                return Err(error::feature_unsupported(
                    "Envelope contains non-ascii chars but server does not support SMTPUTF8",
                ));
            }
            mail_options.push(MailParameter::SmtpUtfEight);
        }

        // Check for non-ascii content in the message
        if !email.is_ascii() && !has_body_parameter {
            if !self.server_info().supports_feature(Extension::EightBitMime) {
                return Err(error::feature_unsupported(
                    "Message contains non-ascii chars but server does not support 8BITMIME",
                ));
            }
            mail_options.push(MailParameter::Body(MailBodyParameter::EightBitMime));
        }

        if self.server_info().supports_size()
            && !options
                .mail_parameters()
                .iter()
                .any(|parameter| matches!(parameter, MailParameter::Size(_)))
        {
            if self
                .server_info()
                .size_limit()
                .is_some_and(|limit| message_size > limit)
            {
                return Err(error::invalid_input(
                    "Message is larger than the server-advertised SIZE limit",
                ));
            }
            mail_options.push(MailParameter::Size(message_size));
        }

        for parameter in options.mail_parameters() {
            self.validate_mail_parameter(
                parameter,
                message_size,
                email.is_ascii(),
                allow_binary_mime,
            )?;
            mail_options.push(parameter.clone());
        }

        Ok(mail_options)
    }

    fn rcpt_options(
        &self,
        envelope: &Envelope,
        options: &SendOptions,
    ) -> Result<Vec<Vec<RcptParameter>>, Error> {
        for (recipient, _) in options.recipient_parameters() {
            if !envelope.to().iter().any(|envelope_recipient| {
                addresses_match_for_recipient_options(recipient, envelope_recipient)
            }) {
                return Err(error::invalid_input(
                    "recipient-specific RCPT parameters do not match an envelope recipient",
                ));
            }
        }

        envelope
            .to()
            .iter()
            .map(|recipient| {
                let parameters = options.rcpt_parameters_for(recipient);
                for parameter in &parameters {
                    self.validate_rcpt_parameter(parameter)?;
                }
                Ok(parameters)
            })
            .collect()
    }

    fn validate_mail_parameter(
        &self,
        parameter: &MailParameter,
        message_size: usize,
        message_is_ascii: bool,
        allow_binary_mime: bool,
    ) -> Result<(), Error> {
        parameter.validate_syntax()?;

        match parameter {
            MailParameter::Body(MailBodyParameter::SevenBit) => {
                if message_is_ascii {
                    Ok(())
                } else {
                    Err(error::invalid_input(
                        "BODY=7BIT cannot be used with non-ASCII message content",
                    ))
                }
            }
            MailParameter::Body(MailBodyParameter::EightBitMime) => {
                if self.server_info().supports_feature(Extension::EightBitMime) {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "BODY=8BITMIME requires server 8BITMIME support",
                    ))
                }
            }
            MailParameter::Body(MailBodyParameter::BinaryMime) => {
                if !allow_binary_mime {
                    return Err(error::invalid_input(
                        "BODY=BINARYMIME requires a BDAT send path",
                    ));
                }
                if self.server_info().supports_binary_mime() {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "BODY=BINARYMIME requires server BINARYMIME support",
                    ))
                }
            }
            MailParameter::Size(size) => {
                if self
                    .server_info()
                    .size_limit()
                    .is_some_and(|limit| *size > limit || message_size > limit)
                {
                    Err(error::invalid_input(
                        "Message is larger than the server-advertised SIZE limit",
                    ))
                } else {
                    Ok(())
                }
            }
            MailParameter::SmtpUtfEight => {
                if self.server_info().supports_feature(Extension::SmtpUtfEight) {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "SMTPUTF8 requires server SMTPUTF8 support",
                    ))
                }
            }
            MailParameter::RequireTls => {
                if !self.server_info().supports_require_tls() {
                    return Err(error::feature_unsupported(
                        "REQUIRETLS requires server REQUIRETLS support",
                    ));
                }
                if !self.is_encrypted() {
                    return Err(error::policy(
                        "REQUIRETLS requires an encrypted SMTP connection",
                    ));
                }
                Ok(())
            }
            MailParameter::FutureRelease(value) => {
                if !self.server_info().supports_future_release() {
                    return Err(error::feature_unsupported(
                        "FUTURERELEASE requires server FUTURERELEASE support",
                    ));
                }
                match value {
                    FutureReleaseParameter::HoldFor(seconds)
                        if self
                            .server_info()
                            .future_release_max_interval()
                            .is_some_and(|limit| *seconds > limit) =>
                    {
                        return Err(error::parameter_over_limit(
                            "HOLDFOR exceeds the server-advertised FUTURERELEASE limit",
                        ));
                    }
                    _ => {}
                }
                Ok(())
            }
            MailParameter::DeliverBy(value) => {
                if !self.server_info().supports_deliver_by() {
                    return Err(error::feature_unsupported(
                        "BY requires server DELIVERBY support",
                    ));
                }
                if value.mode() == DeliverByMode::Return
                    && value.seconds() > 0
                    && self
                        .server_info()
                        .deliver_by_minimum()
                        .is_some_and(|minimum| value.seconds() < minimum)
                {
                    return Err(error::invalid_input(
                        "BY return deadline is below the server-advertised DELIVERBY minimum",
                    ));
                }
                Ok(())
            }
            MailParameter::MtPriority(_) => {
                if self.server_info().supports_mt_priority() {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "MT-PRIORITY requires server MT-PRIORITY support",
                    ))
                }
            }
            MailParameter::DsnReturn(_) | MailParameter::EnvelopeId(_) => {
                if self.server_info().supports_dsn() {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "DSN MAIL parameters require server DSN support",
                    ))
                }
            }
            MailParameter::Other { .. } | MailParameter::OtherRaw { .. } => Ok(()),
        }
    }

    fn validate_rcpt_parameter(&self, parameter: &RcptParameter) -> Result<(), Error> {
        parameter.validate_syntax()?;

        match parameter {
            RcptParameter::Notify(_) | RcptParameter::OriginalRecipient { .. } => {
                if self.server_info().supports_dsn() {
                    Ok(())
                } else {
                    Err(error::feature_unsupported(
                        "DSN RCPT parameters require server DSN support",
                    ))
                }
            }
            RcptParameter::Other { .. } => Ok(()),
        }
    }

    pub(crate) fn has_broken(&self) -> bool {
        self.stream.get_ref().state() != ConnectionState::Ok
    }

    // Async STARTTLS is only wired when the tokio backend is enabled.
    pub(crate) fn can_starttls(&self) -> bool {
        !self.is_encrypted() && self.server_info.supports_feature(Extension::StartTls)
    }

    /// Upgrade the connection using `STARTTLS`.
    ///
    /// As described in [rfc3207]. Note that this mechanism has been deprecated in [rfc8314].
    ///
    /// [rfc3207]: https://www.rfc-editor.org/rfc/rfc3207
    /// [rfc8314]: https://www.rfc-editor.org/rfc/rfc8314
    // Async STARTTLS is only wired when the tokio backend is enabled.
    pub(crate) async fn starttls(
        &mut self,
        tls_parameters: TlsParameters,
        hello_name: &ClientId,
    ) -> Result<(), Error> {
        if self.server_info.supports_feature(Extension::StartTls) {
            try_smtp!(self.command(Starttls).await, self);
            // Belt and braces at the security boundary. The generic
            // surplus-bytes check inside the reply read normally fires first,
            // but `get_mut()` swaps only the INNER stream and leaves the
            // `BufReader` buffer intact, so any pre-TLS byte still sitting here
            // would be consumed as the first bytes of the TLS session - the
            // classic STARTTLS response-injection. This gate is independent of
            // whichever reply path produced the 220.
            if !self.stream.buffer().is_empty() {
                self.stream.get_mut().set_state(ConnectionState::Broken);
                self.abort().await;
                return Err(error::parse(
                    "SMTP server sent an unsolicited reply before the STARTTLS upgrade",
                ));
            }
            self.stream
                .get_mut()
                .upgrade_tls(tls_parameters, self.timeout)
                .await?;
            #[cfg(feature = "tracing")]
            tracing::debug!("connection encrypted");
            // Send EHLO/LHLO again
            try_smtp!(self.hello(hello_name).await, self);
            self.hello_name = hello_name.clone();
            Ok(())
        } else {
            // A client policy refusal, not caller input: the configuration
            // demanded TLS and this server cannot provide it. Same class as
            // IMAP's `StartTlsUnavailable`, `Authorization(PolicyBlocked)`.
            Err(error::policy("STARTTLS is not supported on this server"))
        }
    }

    /// Send EHLO or LHLO and update server info
    async fn hello(&mut self, hello_name: &ClientId) -> Result<(), Error> {
        self.hello_with_budget(hello_name, self.per_operation_budget())
            .await
    }

    async fn hello_with_budget(
        &mut self,
        hello_name: &ClientId,
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        let response = match self.protocol {
            Protocol::Lmtp => {
                let command = Lhlo::new(hello_name.clone())?;
                try_smtp!(self.command_with_budget(command, budget).await, self)
            }
            _ => {
                let command = Ehlo::new(hello_name.clone())?;
                try_smtp!(self.command_with_budget(command, budget).await, self)
            }
        };
        self.server_info = try_smtp!(ServerInfo::from_response(&response), self);
        Ok(())
    }

    /// Close the connection.
    ///
    /// Bounded by `min(configured timeout, TEARDOWN_TIMEOUT)`, unlike the
    /// blocking half's `abort()`: `poll_shutdown` on a TLS stream sends
    /// `close_notify` and waits for the peer's, so an unresponsive peer would
    /// otherwise hang the caller after the timeout that already fired - and
    /// with `timeout(None)` would hang it forever. See `TEARDOWN_TIMEOUT` for
    /// why teardown has a cap of its own. That is the one deliberate
    /// behavioural difference between the two adapters, and it lives here
    /// because it is I/O rather than protocol: `Shutdown::Both` on a blocking
    /// socket is a syscall that returns immediately, so the blocking half
    /// already satisfies the same rule with no timeout at all.
    pub(crate) async fn abort(&mut self) {
        self.stream.get_mut().set_state(ConnectionState::Broken);
        let _ = with_timeout(
            self.teardown_budget(),
            "SMTP shutdown timed out",
            self.stream.shutdown(),
        )
        .await;
    }

    /// Tells if the underlying stream is currently encrypted
    pub(crate) fn is_encrypted(&self) -> bool {
        self.stream.get_ref().is_encrypted()
    }

    /// DER of the peer (server) certificate. See
    /// [`AsyncNetworkStream::peer_certificate_der`] for the contract.
    ///
    /// Consumed by `auth`'s `resolve_scram_binding` for SCRAM-PLUS channel
    /// binding.
    pub(crate) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        self.stream.get_ref().peer_certificate_der()
    }

    /// Checks if the server is connected using the NOOP SMTP command.
    ///
    /// A failed check marks the connection broken and closes it. A connection
    /// that cannot answer NOOP is not safe to keep in the pool.
    pub(crate) async fn test_connected(&mut self) -> bool {
        match self.command(Noop).await {
            Ok(_) => true,
            Err(_) => {
                self.abort().await;
                false
            }
        }
    }

    /// Sends a VRFY command and returns the server response.
    pub(crate) async fn verify(&mut self, argument: impl Into<String>) -> Result<Response, Error> {
        self.command_accepting_status(Vrfy::new(argument.into())?)
            .await
    }

    /// Sends an EXPN command and returns the server response.
    pub(crate) async fn expand(&mut self, argument: impl Into<String>) -> Result<Response, Error> {
        self.command_accepting_status(Expn::new(argument.into())?)
            .await
    }

    /// Sends an AUTH command, selecting the strongest compatible mechanism.
    ///
    /// Awaited twin of `SmtpConnection::auth`: same SCRAM-aware order, same
    /// RFC 5802 Section 6 downgrade protection, same binding-skip fall-through.
    pub(crate) async fn auth(
        &mut self,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        // OAuth credentials never use SCRAM: select the first advertised
        // mechanism from the caller's order and run the legacy encoder path.
        if let Some(mechanism) = oauth_mechanism(mechanisms, &self.server_info, credentials) {
            return self.auth_legacy(mechanism, credentials).await;
        }

        let plus_candidate = mechanisms.iter().any(|mechanism| {
            matches!(
                mechanism,
                Mechanism::ScramSha1Plus | Mechanism::ScramSha256Plus
            ) && self.server_info.supports_auth_mechanism(*mechanism)
        });
        let binding = if plus_candidate {
            self.resolve_scram_binding()?
        } else {
            None
        };
        let chosen = password_mechanism(mechanisms, &self.server_info, binding.is_some())?;
        match chosen {
            Mechanism::ScramSha1Plus | Mechanism::ScramSha256Plus => {
                let binding = binding.expect("PLUS binding resolved above");
                self.auth_scram(chosen, binding, credentials).await
            }
            Mechanism::ScramSha1 | Mechanism::ScramSha256 => {
                self.auth_scram(chosen, ScramChannelBinding::None, credentials)
                    .await
            }
            _ => self.auth_legacy(chosen, credentials).await,
        }
    }

    /// Resolve the `tls-server-end-point` channel binding for the live
    /// connection. The DER accessor is sync (cached on the TLS stream), so this
    /// is non-awaiting and the binding-skip decision stays network-free.
    fn resolve_scram_binding(&self) -> Result<Option<ScramChannelBinding>, Error> {
        let der = self.peer_certificate_der();
        resolve_scram_binding(der.as_deref())
    }

    /// Run a SCRAM exchange (bound or unbound) as a no-IR `334` challenge walk.
    async fn auth_scram(
        &mut self,
        mechanism: Mechanism,
        binding: ScramChannelBinding,
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        let hash = scram_hash(mechanism).expect("auth_scram only called for SCRAM mechanisms");
        // Every auth-phase stamp in this exchange names the rung that is
        // running, derived from the same `Mechanism` that frames the AUTH
        // command, so the reported name and the wire token cannot disagree.
        let phase = SmtpCommandPhase::Auth {
            mechanism: Some(mechanism.name()),
        };
        let (username, password) = credentials.password_parts()?;
        let mut exchange = ScramExchange::new(hash, binding, username, password.into())?;

        let auth = Auth::new(mechanism, credentials.clone(), None)?;
        let response = try_smtp!(self.command(auth).await, self, phase);
        if !response.has_code(334) {
            self.abort().await;
            return Err(error::status(response).with_phase(phase));
        }
        let client_first = exchange.client_first();
        let response = try_smtp!(
            self.write_auth_continuation(client_first.as_str()).await,
            self,
            phase
        );

        // A malformed continuation is a protocol-class parse error (not an
        // auth failure), so it is not Auth-phase tagged, but it must still
        // abort the connection like every other error path in this exchange.
        let server_first = try_smtp!(decode_auth_challenge(&response), self);
        let response = match try_smtp!(exchange.step(&server_first), self, phase) {
            ScramStep::Reply(client_final) => try_smtp!(
                self.write_auth_continuation(client_final.as_str()).await,
                self,
                phase
            ),
            ScramStep::Complete => {
                self.abort().await;
                return Err(error::parse("SCRAM completed before server-final"));
            }
        };

        // Server-final reply. RFC 4954 carries it on a `334` continuation, with
        // the tagged `235` success following an empty client line. Some servers
        // (field-observed) fold the server-final straight onto the `235`
        // success reply (`235 v=...`). Both shapes are accepted; anything else
        // is an auth failure (or protocol error) and aborts. A `334` here must
        // not be mistaken for success - only a positive reply ends the walk.
        let success = if response.has_code(334) {
            let server_final = try_smtp!(decode_auth_challenge(&response), self);
            try_smtp!(
                Self::verify_scram_server_final(&mut exchange, &server_final),
                self,
                phase
            );
            let final_reply = try_smtp!(self.write_auth_continuation("").await, self, phase);
            if !final_reply.is_positive() {
                self.abort().await;
                return Err(error::status(final_reply).with_phase(phase));
            }
            final_reply
        } else if response.is_positive() {
            let server_final = try_smtp!(decode_scram_payload(&response), self);
            try_smtp!(
                Self::verify_scram_server_final(&mut exchange, &server_final),
                self,
                phase
            );
            response
        } else {
            self.abort().await;
            return Err(error::status(response).with_phase(phase));
        };

        let hello_name = self.hello_name.clone();
        try_smtp!(self.hello(&hello_name).await, self);
        Ok(success)
    }

    /// Verify a decoded SCRAM server-final, mapping a non-terminal step to a
    /// parse-class error. Shared by the `334`-continuation and `235`-folded
    /// server-final shapes.
    fn verify_scram_server_final(
        exchange: &mut ScramExchange,
        server_final: &str,
    ) -> Result<(), Error> {
        match exchange.step(server_final)? {
            ScramStep::Complete => Ok(()),
            ScramStep::Reply(_) => Err(error::parse("unexpected SCRAM reply after server-final")),
        }
    }

    /// Write a base64 SASL continuation line (already encoded) and read the
    /// reply. An empty `line` emits a bare `\r\n`.
    async fn write_auth_continuation(&mut self, line: &str) -> Result<Response, Error> {
        let framed = Zeroizing::new(format!("{line}\r\n"));
        self.write(framed.as_bytes()).await?;
        self.read_response().await
    }

    /// The legacy stateless `Auth::new` / `Auth::new_from_response` 334 loop,
    /// for PLAIN / LOGIN / XOAUTH2 / OAUTHBEARER.
    async fn auth_legacy(
        &mut self,
        mechanism: Mechanism,
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        // Every auth-phase stamp in this exchange names the rung that is
        // running, derived from the same `Mechanism` that frames the AUTH
        // command, so the reported name and the wire token cannot disagree.
        let phase = SmtpCommandPhase::Auth {
            mechanism: Some(mechanism.name()),
        };
        // Resolve the OAuth access token from the shared source once, up
        // front, so the same token frames the initial response and any
        // OAUTHBEARER error-continuation round. Awaiting `current()` here
        // is the single rotation read point; a token refreshed on the
        // source is presented on this (re)connect.
        let oauth_token = if matches!(mechanism, Mechanism::Xoauth2 | Mechanism::OAuthBearer) {
            Some(try_smtp!(credentials.oauth2_token().await, self, phase))
        } else {
            None
        };
        let oauth_token = oauth_token.as_ref().map(|(_, token)| token.as_str());

        // Limit challenges to avoid blocking
        let mut challenges: u8 = 10;
        // Position of the challenge within the exchange. LOGIN answers by
        // position (username, then password) rather than by prompt text, so
        // this counter is the only thing that selects which credential goes
        // out; it must not be derived from the remaining-challenge budget.
        let mut challenge_index: usize = 0;
        let auth = Auth::new(mechanism, credentials.clone(), oauth_token)?;
        let mut response = try_smtp!(self.command(auth).await, self, phase);

        while challenges > 0 && response.has_code(334) {
            challenges -= 1;
            // Build the continuation reply. For XOAUTH2/OAUTHBEARER a 334 here
            // is the server's base64 failure detail, and the encoder returns
            // the dummy-cancel line so the next read surfaces the tagged
            // negative reply on the Auth lane. Any construction error (bad
            // base64, an encoder rejecting an unexpected challenge) must abort
            // and carry the Auth phase, not leak through a bare `?` as an
            // untagged parse error.
            let continuation = try_smtp!(
                Auth::new_from_response_at_index(
                    mechanism,
                    credentials.clone(),
                    &response,
                    challenge_index,
                    oauth_token,
                ),
                self,
                phase
            );
            challenge_index += 1;
            response = try_smtp!(self.command(continuation).await, self, phase);
        }

        if challenges == 0 && response.has_code(334) {
            // The server never completed (or rejected) the exchange within the
            // round-trip budget: an auth-exchange failure, routed on the Auth
            // lane (InvalidInput + Auth -> Authorization), not an untagged
            // Protocol(ParseFailed). Exhausting the budget while sending the
            // tenth answer is fine when that answer receives a final reply.
            self.abort().await;
            Err(error::invalid_input("Unexpected number of challenges").with_phase(phase))
        } else {
            let hello_name = self.hello_name.clone();
            try_smtp!(self.hello(&hello_name).await, self);
            Ok(response)
        }
    }

    /// Write the DATA body and its terminator.
    ///
    /// Consumes an iterator that in its whole represents the message: the
    /// last-two-bytes tracking is what recognizes a final CRLF split across
    /// two items, so the terminator reuses the message's own final CRLF rather
    /// than appending an empty line the sender never wrote.
    async fn write_body_iter<I, B>(&mut self, message: I) -> Result<(), Error>
    where
        I: Iterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut codec = ClientCodec::new();
        let mut last_two = [0_u8; 2];
        let mut seen = 0_usize;
        for message_part in message {
            let message_part = message_part.as_ref();
            if message_part.len() >= 2 {
                last_two.copy_from_slice(&message_part[message_part.len() - 2..]);
                seen = 2;
            } else if let Some(&byte) = message_part.first() {
                last_two[0] = last_two[1];
                last_two[1] = byte;
                seen = (seen + 1).min(2);
            }
            let mut out_buf = Vec::with_capacity(message_part.len());
            codec.encode(message_part, &mut out_buf);
            self.write(out_buf.as_slice()).await?;
        }
        self.write(data_terminator(seen >= 2 && last_two == *b"\r\n"))
            .await
    }

    /// Sends the message content.
    ///
    /// Test-only since the send paths were factored onto `client::core`: they
    /// write the body through `Op::WriteBody` and read the final reply through
    /// `Op::ReadSingle`, over these same writers.
    #[cfg(test)]
    pub(crate) async fn message(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.message_iter(std::iter::once(message)).await
    }

    /// Sends the message content by consuming an iterator that in its whole represents a message.
    #[cfg(test)]
    pub(crate) async fn message_iter<I, B>(&mut self, message: I) -> Result<Response, Error>
    where
        I: Iterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.write_body_iter(message).await?;
        self.read_response().await
    }

    /// Sends an SMTP command
    pub(crate) async fn command<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.command_with_budget(command, self.per_operation_budget())
            .await
    }

    async fn command_with_budget<C: Display>(
        &mut self,
        command: C,
        budget: TimeoutBudget,
    ) -> Result<Response, Error> {
        self.write_command_with_budget(command, budget).await?;
        self.read_response_with_budget(budget).await
    }

    async fn command_accepting_status<C: Display>(
        &mut self,
        command: C,
    ) -> Result<Response, Error> {
        self.write_command(command).await?;
        self.read_response_accepting_status().await
    }

    async fn write_command<C: Display>(&mut self, command: C) -> Result<(), Error> {
        self.write_command_with_budget(command, self.per_operation_budget())
            .await
    }

    async fn write_command_with_budget<C: Display>(
        &mut self,
        command: C,
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        self.command_buffer.zeroize();
        write!(&mut self.command_buffer, "{command}")
            .map_err(|_| error::internal("failed to serialize SMTP command"))?;
        let result = Self::write_stream_with_budget(
            &mut self.stream,
            self.command_buffer.as_bytes(),
            budget,
        )
        .await;
        self.command_buffer.zeroize();
        result
    }

    /// Closes out an LMTP final-status drain.
    ///
    /// A surplus final status that already reached the read buffer is a
    /// protocol violation we can prove: the stream is marked `Broken` and the
    /// caller gets an error. Bytes still below the buffer (in the socket or
    /// TLS record layer) cannot be observed without a read that would block on
    /// a well-behaved peer, so cleanliness is never positively established:
    /// every LMTP drain retires the connection instead of returning it to the
    /// pool. LMTP is local delivery, so a reconnect is cheap next to reusing a
    /// desynchronized stream.
    fn finish_lmtp_final_drain(&mut self) -> Result<(), Error> {
        self.retire = true;

        if self.stream.buffer().is_empty() {
            return Ok(());
        }

        self.stream.get_mut().set_state(ConnectionState::Broken);
        Err(error::parse(
            "LMTP server returned more final statuses than accepted recipients",
        ))
    }

    /// Whether this connection must not be returned to the pool.
    pub(crate) fn should_retire(&self) -> bool {
        self.retire
    }

    /// Writes a string to the server
    async fn write(&mut self, string: &[u8]) -> Result<(), Error> {
        self.write_with_budget(string, self.per_operation_budget())
            .await
    }

    async fn write_with_budget(
        &mut self,
        string: &[u8],
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        Self::write_stream_with_budget(&mut self.stream, string, budget).await
    }

    /// Admit the next write against the outbound cap before a write timeout
    /// is armed.
    ///
    /// Whether the admission wait may run OUTSIDE the budget depends on which
    /// budget it is, and the two variants answer differently - hence the
    /// exhaustive match rather than one unconditional await.
    ///
    /// - `PerOperation` is a bound on ONE write's dealings with the peer. The
    ///   admission wait is this crate's own, imposed at the consumer's request,
    ///   so it runs outside the bound; that is what makes the guarantee
    ///   independent of the ratio between the timeout and one second of cap,
    ///   and what the blocking half gets by sleeping outside `SO_SNDTIMEO`.
    /// - `SetupDeadline` is an ABSOLUTE ceiling shared by DNS, connect, TLS,
    ///   the banner and EHLO. Waiting outside it does not spend the deadline,
    ///   it WAITS PAST it: under a cap small enough to throttle a greeting, an
    ///   EHLO write waits a second per second of cap while the remaining setup
    ///   budget is milliseconds, and the operation would report its timeout
    ///   only after blowing through a ceiling the contract says it cannot
    ///   exceed. So the wait is bounded by the slack left on the deadline, and
    ///   admission that cannot come inside the setup budget fails as a setup
    ///   timeout; the abandoned reservation gives its time back.
    ///
    /// The write and flush that follow re-read the slack for themselves,
    /// because `with_timeout` resolves a `SetupDeadline` at the moment it is
    /// armed. Handing them a duration computed BEFORE the wait would give back
    /// exactly the overrun this bounding exists to remove.
    ///
    /// Teardown does not pass through here: `teardown_budget()` builds a
    /// `PerOperation` budget and applies it directly to `stream.shutdown()`,
    /// never to a write.
    async fn admit_outbound_within(
        stream: &mut BufReader<AsyncNetworkStream>,
        len: usize,
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        const MESSAGE: &str = "SMTP write timed out";

        match budget {
            TimeoutBudget::PerOperation(_) => {
                stream.get_mut().admit_outbound(len).await;
                Ok(())
            }
            TimeoutBudget::SetupDeadline(deadline) => match deadline.remaining(MESSAGE)? {
                None => {
                    stream.get_mut().admit_outbound(len).await;
                    Ok(())
                }
                Some(remaining) => {
                    tokio::time::timeout(remaining, stream.get_mut().admit_outbound(len))
                        .await
                        .map_err(|_| error::timeout(MESSAGE))
                }
            },
        }
    }

    async fn write_stream_with_budget(
        stream: &mut BufReader<AsyncNetworkStream>,
        string: &[u8],
        budget: TimeoutBudget,
    ) -> Result<(), Error> {
        stream.get_ref().state().verify()?;
        stream.get_mut().set_state(ConnectionState::Broken);

        // The per-operation timeout bounds ONE write that makes no progress,
        // not the whole transfer. Arming it once around a `write_all` of the
        // entire buffer makes the bound a function of message size and link
        // speed: a 12 MB body over a slow uplink exceeds a 10 s budget while
        // uploading perfectly happily. The blocking half never had that
        // problem - `SO_SNDTIMEO` is per `write(2)` - so re-arming per
        // progressing write is what keeps the two halves equivalent: a stalled
        // peer still times out, a slow-but-progressing upload does not. A
        // `SetupDeadline` budget still shrinks across the loop, so the shared
        // connect deadline is unaffected.
        //
        // Re-arming alone does NOT cover an outbound bandwidth cap, and must
        // not be read as covering it. Each iteration therefore has two
        // phases. Admission comes first, HERE, before the timeout is armed: it
        // reserves at most one second of cap against the schedule every
        // connection of this transport shares and waits for its slot, so a
        // wait this crate imposed on itself is never charged to the budget
        // that exists to judge the peer. The socket write then runs under the
        // timeout against the installed allowance and never consults the
        // schedule again, so a peer that accepts nothing still times out. A
        // short write leaves the rest of the allowance for the next iteration,
        // whose admission returns at once; the allowance never exceeds what
        // this call has left to write, so a successful call spends it all.
        //
        // The consequence is worth stating where the re-arming happens, because
        // it is the exact shape the READ side went out of its way to close:
        // there is NO total bound on a transfer here. A peer that accepts one
        // byte per (timeout minus epsilon) re-arms this loop forever and keeps
        // an upload alive indefinitely. The reply reader refuses the same trick
        // - it collapses the per-operation budget into ONE deadline spanning
        // the whole reply, so a peer trickling one line per period cannot
        // stretch it - and the asymmetry is deliberate, not an oversight in
        // this loop. A reply is a bounded unit (`MAX_RESPONSE_BYTES`), so "one
        // reply" is expressible as a single deadline; a message body has no
        // such ceiling, and any total bound picked for it would be a function
        // of message size and link speed, which is precisely the bound that
        // failed healthy multi-megabyte uploads on a domestic uplink and made
        // the re-arming loop necessary. A caller who needs a wall-clock ceiling
        // on a whole send imposes it above the transport, where the message
        // size is known. Adding one here is a behaviour change and the
        // repository owner's call, not this loop's.
        let mut rest = string;
        while !rest.is_empty() {
            Self::admit_outbound_within(stream, rest.len(), budget).await?;
            let written =
                with_timeout(budget, "SMTP write timed out", stream.get_mut().write(rest))
                    .await?
                    .map_err(error::network)?;
            if written == 0 {
                return Err(error::network(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "SMTP write accepted no bytes",
                )));
            }
            rest = &rest[written..];
        }
        with_timeout(budget, "SMTP flush timed out", stream.get_mut().flush())
            .await?
            .map_err(error::network)?;
        stream.get_mut().set_state(ConnectionState::Ok);

        #[cfg(feature = "tracing")]
        tracing::debug!("Wrote {} bytes", string.len());
        Ok(())
    }

    /// Gets the SMTP response
    pub(crate) async fn read_response(&mut self) -> Result<Response, Error> {
        self.read_response_with_budget(self.per_operation_budget())
            .await
    }

    async fn read_response_accepting_status(&mut self) -> Result<Response, Error> {
        self.read_response_with_budget_inner(self.per_operation_budget(), true, true)
            .await
    }

    async fn read_response_with_budget(
        &mut self,
        budget: TimeoutBudget,
    ) -> Result<Response, Error> {
        self.read_response_with_budget_inner(budget, false, true)
            .await
    }

    async fn read_response_with_budget_inner(
        &mut self,
        budget: TimeoutBudget,
        accept_negative: bool,
        manage_state: bool,
    ) -> Result<Response, Error> {
        if manage_state {
            self.stream.get_ref().state().verify()?;
            self.stream.get_mut().set_state(ConnectionState::Broken);
        }

        // Every buffer fill spends the same remaining peer time. Admission is
        // separate so the consumer's cap cannot consume that time. Setup keeps
        // its absolute ceiling across both phases.
        let mut budget = ReplyBudget::new(budget);

        let mut buffer = String::with_capacity(100);

        loop {
            let mut line = Vec::with_capacity(100);
            // Admit each fill separately; read_until could perform several
            // socket reads behind one timer and charge admission to the peer.
            let bytes_read = loop {
                let needs_read = self.stream.buffer().is_empty();
                if needs_read {
                    let admission = self.stream.get_mut().admit_inbound(8192);
                    match budget {
                        ReplyBudget::Own(_) => admission.await,
                        ReplyBudget::Setup(deadline) => {
                            with_timeout(
                                TimeoutBudget::SetupDeadline(deadline),
                                "SMTP read timed out",
                                admission,
                            )
                            .await?;
                        }
                    }
                }
                match budget {
                    ReplyBudget::Own(Some(left)) if left.is_zero() => {
                        return Err(error::timeout("SMTP read timed out"));
                    }
                    ReplyBudget::Setup(deadline) => {
                        deadline.remaining("SMTP read timed out")?;
                    }
                    _ => {}
                }
                let peer_started = tokio::time::Instant::now();

                let (consumed, done) = {
                    let available = with_timeout(
                        budget.budget(),
                        "SMTP read timed out",
                        self.stream.fill_buf(),
                    )
                    .await?
                    .map_err(error::network)?;
                    if needs_read {
                        budget.spend(peer_started.elapsed());
                    }

                    if available.is_empty() {
                        (0, true)
                    } else {
                        match available.iter().position(|byte| *byte == b'\n') {
                            Some(index) => {
                                line.extend_from_slice(&available[..=index]);
                                (index + 1, true)
                            }
                            None => {
                                line.extend_from_slice(available);
                                (available.len(), false)
                            }
                        }
                    }
                };
                self.stream.consume(consumed);

                if line.len() > MAX_RESPONSE_LINE_BYTES {
                    return Err(error::parse("SMTP response line too long"));
                }
                if done {
                    break line.len();
                }
            };
            if bytes_read == 0 {
                break;
            }
            if buffer.len() + line.len() > MAX_RESPONSE_BYTES {
                return Err(error::parse("SMTP response too large"));
            }
            let line = std::str::from_utf8(&line)
                .map_err(|_| error::parse("SMTP response is not valid UTF-8"))?;
            buffer.push_str(line);

            #[cfg(feature = "tracing")]
            tracing::debug!("<< {}", escape_crlf(line));
            match parse_response(&buffer) {
                Ok((_remaining, response)) => {
                    if manage_state && !self.stream.buffer().is_empty() {
                        return Err(error::parse("SMTP server sent an unsolicited reply"));
                    }
                    if manage_state {
                        self.stream.get_mut().set_state(ConnectionState::Ok);
                    }

                    return if accept_negative || response.is_positive() {
                        Ok(response)
                    } else {
                        Err(error::status(response))
                    };
                }
                Err(nom::Err::Failure(e)) => {
                    return Err(error::parse(e.to_string()));
                }
                Err(nom::Err::Incomplete(_)) => { /* read more */ }
                Err(nom::Err::Error(e)) => {
                    return Err(error::parse(e.to_string()));
                }
            }
        }

        Err(error::parse("incomplete response"))
    }

    fn finish_reply_group(&mut self) -> Result<(), Error> {
        if !self.stream.buffer().is_empty() {
            return Err(error::parse("SMTP server sent an unsolicited reply"));
        }
        self.stream.get_mut().set_state(ConnectionState::Ok);
        Ok(())
    }
}

#[cfg(all(test, feature = "tokio"))]
mod transcript_tests {
    use std::time::Duration;

    use crate::{
        address::{Address, Envelope},
        transport::smtp::{
            Protocol,
            account_error::{SmtpErrorContext, into_account_error},
            authentication::{Credentials, Mechanism},
            batch::SmtpBatchRecipient,
            commands::Noop,
            error::{SmtpCommandPhase, SmtpTransmissionState},
            extension::{
                ClientId, DeliverByMode, DsnNotify, DsnReturn, Extension, MailBodyParameter,
                MailParameter,
            },
            test_support::{Transcript, scram_enhanced_success_reply, scram_server_first_reply},
        },
    };
    use bifrost_types::error::BatchItemId;

    use super::{
        AsyncDeadline, AsyncNetworkStream, AsyncSmtpConnection, SendOptions, TEARDOWN_TIMEOUT,
        TimeoutBudget,
    };

    const HELLO: &str = "EHLO client.example\r\n";

    #[tokio::test(crate = "tokio")]
    async fn batch_mail_construction_failure_is_stamped_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let recipients = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "recipient@example.com".parse().unwrap(),
        }];

        let Err((error, _progress)) = connection.build_batch(
            Protocol::Smtp,
            Some(Address::new_dangerous("sender\r\nRSET", "example.com")),
            recipients,
            b"body",
            &SendOptions::default(),
        ) else {
            panic!("hostile sender should fail MAIL construction");
        };

        assert_eq!(error.attempt(), Some(SmtpTransmissionState::Unsent));
        assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
        transcript.assert_exhausted();
    }

    /// Finding 4c: ported off `tests/transport_smtp.rs`, which used to bind a
    /// listener that wrote a 4096-byte banner line and then assert that
    /// `test_connection()` returned within five wall-clock seconds. The
    /// invariant is the same and needs no socket: a greeting line past
    /// `MAX_RESPONSE_LINE_BYTES` must surface as a parse error, and must do so
    /// by returning rather than reading forever waiting for a terminator.
    #[tokio::test(crate = "tokio")]
    async fn an_oversized_greeting_line_is_a_parse_error_not_a_hang() {
        // A WELL-FORMED greeting that is merely too long. A line of garbage
        // would fail to parse whether or not the cap exists, so it pins
        // nothing about the cap; this one parses fine once the cap is removed.
        let mut banner = format!("220 {}", "x".repeat(4096));
        banner.push_str("\r\n");
        let transcript = Transcript::new(&banner);

        let error = AsyncSmtpConnection::from_transcript(
            transcript,
            &ClientId::Domain("client.example".to_owned()),
            Protocol::Smtp,
        )
        .await
        .err()
        .expect("an oversized banner line must surface as an error");

        assert!(error.is_parse(), "expected a parse error, got {error:?}");
    }

    #[tokio::test(crate = "tokio")]
    async fn all_recipient_rejection_resets_every_direct_and_batch_transaction() {
        let hello = ClientId::Domain("client.example".to_owned());
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();

        for chunking in [false, true] {
            let greeting = if chunking {
                "250-lmtp.example\r\n250 CHUNKING\r\n"
            } else {
                "250 lmtp.example\r\n"
            };
            let transcript = Transcript::new("220 lmtp.example\r\n")
                .expect("LHLO client.example\r\n", greeting)
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect(
                    "RCPT TO:<recipient@example.com>\r\n",
                    "550 recipient rejected\r\n",
                )
                .expect("RSET\r\n", "250 reset ok\r\n");
            let mut connection =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                    .await
                    .unwrap();
            let statuses = if chunking {
                connection
                    .send_lmtp_bdat_with_options(&envelope, b"body", &Default::default())
                    .await
                    .unwrap()
            } else {
                connection.send_lmtp(&envelope, b"body").await.unwrap()
            };
            assert_eq!(statuses.len(), 1);
            assert!(!statuses[0].is_positive());
            assert!(!connection.has_broken());
            transcript.assert_exhausted();
        }

        for protocol in [Protocol::Smtp, Protocol::Lmtp] {
            let hello_command = if protocol == Protocol::Smtp {
                "EHLO client.example\r\n"
            } else {
                "LHLO client.example\r\n"
            };
            let transcript = Transcript::new("220 server.example\r\n")
                .expect(hello_command, "250 server.example\r\n")
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect(
                    "RCPT TO:<recipient@example.com>\r\n",
                    "550 recipient rejected\r\n",
                )
                .expect("RSET\r\n", "250 reset ok\r\n");
            let batch = vec![SmtpBatchRecipient {
                id: BatchItemId("item-0".to_owned()),
                address: "recipient@example.com".parse().unwrap(),
            }];
            let mut connection =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, protocol)
                    .await
                    .unwrap();
            let progress = if protocol == Protocol::Smtp {
                connection
                    .send_smtp_batch(
                        Some("sender@example.com".parse().unwrap()),
                        batch,
                        b"body",
                        &Default::default(),
                    )
                    .await
            } else {
                connection
                    .send_lmtp_batch(
                        Some("sender@example.com".parse().unwrap()),
                        batch,
                        b"body",
                        &Default::default(),
                    )
                    .await
            }
            .unwrap();
            assert_eq!(progress.resolve().failed().len(), 1);
            assert!(!connection.has_broken());
            transcript.assert_exhausted();
        }
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_server_rejections_carry_their_command_phase() {
        let hello = ClientId::Domain("client.example".to_owned());
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let window = "MAIL FROM:<sender@example.com>\r\nRCPT TO:<recipient@example.com>\r\n";

        // MAIL FROM rejected: no transaction was opened, so no RSET.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "550 sender rejected\r\n250 recipient ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::MailFrom));

        // RCPT TO rejected.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::RcptTo));

        // DATA rejected before the body.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n250 recipient ok\r\n")
            .expect("DATA\r\n", "554 no data\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataCommand));
    }

    /// Finding 7: the configured timeout bounds the REPLY, not each line of
    /// it. Arming it per line let a peer trickling one line per timeout period
    /// stretch one reply to `MAX_RESPONSE_BYTES / line` times the configured
    /// timeout, with only the size caps as a bound. The peer here answers - it
    /// is never silent, so every individual line read completes well inside
    /// the timeout - but the reply as a whole outruns the budget and must
    /// fail.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_trickled_multi_line_reply_cannot_outrun_the_operation_timeout() {
        use std::time::Duration;

        use crate::transport::smtp::test_support::SlowLinePeer;

        let hello = ClientId::Domain("client.example".to_owned());
        // Six lines, 20s apart, under a 30s per-operation timeout. Each line
        // arrives inside the timeout; six of them are 120s of reply.
        let peer = SlowLinePeer::new(
            [
                "250-one\r\n",
                "250-two\r\n",
                "250-three\r\n",
                "250-four\r\n",
                "250-five\r\n",
                "250 six\r\n",
            ],
            Duration::from_secs(20),
        );
        let mut connection = AsyncSmtpConnection::from_raw_stream_for_test(
            Box::new(peer),
            &hello,
            Protocol::Smtp,
            Some(Duration::from_secs(30)),
        );

        let started = tokio::time::Instant::now();
        let error = connection.read_response().await.unwrap_err();

        assert!(error.is_timeout(), "expected a timeout, got {error:?}");
        assert!(
            started.elapsed() <= Duration::from_secs(30),
            "the whole reply must be bounded by one timeout, took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_trickled_reply_across_reads_spends_only_cumulative_peer_time() {
        use crate::transport::smtp::test_support::SlowLinePeer;
        use bifrost_net::{BandwidthBudget, BandwidthDirection};
        let budget = BandwidthBudget::new(Some(1000));
        let mut credit = budget.reserve(BandwidthDirection::Inbound, 1000);
        assert!(credit.offer());
        credit.finish(1000);
        let _sibling = budget.reserve(BandwidthDirection::Inbound, 1000);
        let peer = SlowLinePeer::from_owned_lines(
            b"250 a long trickled reply\r\n".iter().map(|b| vec![*b]),
            Duration::from_millis(50),
        );
        let hello = ClientId::Domain("client.example".into());
        let mut connection = AsyncSmtpConnection::from_raw_stream_for_test(
            Box::new(peer),
            &hello,
            Protocol::Smtp,
            Some(Duration::from_millis(225)),
        );
        connection
            .stream
            .get_mut()
            .set_metering(super::super::metering::WireMetering::new(
                None,
                Some(budget),
            ));
        let started = tokio::time::Instant::now();
        let error = connection.read_response().await.unwrap_err();
        assert!(error.is_timeout());
        assert_eq!(started.elapsed(), Duration::from_millis(2225));
    }

    /// Two 900-byte replies leave the next reservation behind their slots.
    async fn connection_owing_inbound_debt(timeout: Option<Duration>) -> AsyncSmtpConnection {
        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::SlowLinePeer;

        fn padded(prefix: &str) -> Vec<u8> {
            let mut line = prefix.as_bytes().to_vec();
            line.resize(898, b'x');
            line.extend_from_slice(b"\r\n");
            line
        }

        let hello = ClientId::Domain("client.example".to_owned());
        let peer = SlowLinePeer::from_owned_lines(
            [
                padded("250 first"),
                padded("250 second"),
                b"250 third\r\n".to_vec(),
            ],
            Duration::ZERO,
        );
        let mut connection = AsyncSmtpConnection::from_raw_stream_for_test(
            Box::new(peer),
            &hello,
            Protocol::Smtp,
            timeout,
        );
        connection.stream.get_mut().set_metering(WireMetering::new(
            None,
            Some(bifrost_net::BandwidthBudget::new(Some(1000))),
        ));

        for _ in 0..2 {
            connection
                .read_response()
                .await
                .expect("admission does not spend the peer timeout");
        }

        connection
    }

    /// Admission can outlast a peer timeout without consuming it.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_capped_reply_read_is_not_charged_its_own_throttle_debt() {
        let mut connection = connection_owing_inbound_debt(Some(Duration::from_millis(200))).await;

        let started = tokio::time::Instant::now();
        let response = connection
            .read_response()
            .await
            .expect("throttle debt is this crate's own wait, not the peer's silence");

        assert!(response.is_positive());
        assert!(
            started.elapsed() > Duration::from_millis(200),
            "the read must have been throttled past the deadline to mean anything, took {:?}",
            started.elapsed()
        );
    }

    /// And the setup ceiling is NOT postponed by it.
    ///
    /// The shared connect deadline covers DNS, connect, TLS, banner and EHLO
    /// and is absolute, so the argument that buys the per-operation budget its
    /// exemption - "this wait is ours, not the peer's" - buys nothing here:
    /// leaving the ceiling is the whole failure. Debt is paid inside the setup
    /// budget and debt that does not fit fails as a timeout at the ceiling,
    /// which is the same ruling `admit_outbound_within` makes on the write
    /// side.
    ///
    /// The error is not the finding - postponing the deadline also ends in a
    /// timeout eventually. The elapsed time is: 300 ms of ceiling against
    /// 800 ms of debt.
    ///
    /// A PRESERVATION CHECK, not evidence for the refund this batch added: the
    /// reader before it also kept inbound debt inside the shared deadline and
    /// also timed out at 300 ms, so this passes against both. It guards the
    /// branch that must NOT change while the sibling branch does - which is
    /// worth having and is not a bite.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_setup_deadline_is_not_postponed_by_inbound_throttle_debt() {
        let mut connection = connection_owing_inbound_debt(None).await;

        let deadline = AsyncDeadline::new(Some(Duration::from_millis(300)));
        let started = tokio::time::Instant::now();
        let error = connection
            .read_response_with_budget(TimeoutBudget::SetupDeadline(deadline))
            .await
            .expect_err("debt that outlasts the setup ceiling must fail as a timeout");

        assert!(error.is_timeout(), "expected a timeout, got {error:?}");
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(300),
            "the setup deadline is a ceiling however the time is spent"
        );
    }

    /// Finding 2: without PIPELINING, a routine negative reply must be handled
    /// exactly as the pipelined path handles it - RSET (or nothing, for a
    /// rejected MAIL FROM) and a reusable connection - not `abort()`.
    #[tokio::test(crate = "tokio")]
    async fn unpipelined_server_rejections_keep_the_connection_reusable() {
        let hello = ClientId::Domain("client.example".to_owned());
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let mail = "MAIL FROM:<sender@example.com>\r\n";
        let rcpt = "RCPT TO:<recipient@example.com>\r\n";

        // MAIL FROM rejected: no transaction was opened, so no RSET.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect(mail, "550 sender rejected\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::MailFrom));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();

        // RCPT TO rejected: RSET clears the open transaction.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect(mail, "250 sender ok\r\n")
            .expect(rcpt, "550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::RcptTo));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();

        // DATA rejected before the body: RSET, connection survives.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect(mail, "250 sender ok\r\n")
            .expect(rcpt, "250 recipient ok\r\n")
            .expect("DATA\r\n", "554 no data\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataCommand));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();

        // The BDAT path shares the same envelope runner.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 CHUNKING\r\n")
            .expect(mail, "250 sender ok\r\n")
            .expect(rcpt, "550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let error = connection
            .send_bdat_with_options(&envelope, b"body", &SendOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::RcptTo));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_body_failure_carries_data_body_phase() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(
                "MAIL FROM:<sender@example.com>\r\nRCPT TO:<recipient@example.com>\r\n",
                "250 sender ok\r\n250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect_then_close("body", "");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection.send(&envelope, b"body").await.unwrap_err();

        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataBody));
    }

    #[tokio::test(crate = "tokio")]
    async fn data_terminator_preserves_exact_message_bytes() {
        let hello = ClientId::Domain("client.example".to_owned());
        let cases: &[(&[u8], &[u8])] = &[
            (b"body\r\n", b".\r\n"),
            (b"body", b"\r\n.\r\n"),
            (b"", b"\r\n.\r\n"),
            (b"body\r", b"\r\n.\r\n"),
            (b"body\n", b"\r\n.\r\n"),
        ];

        for &(body, terminator) in cases {
            let mut transcript =
                Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
            if !body.is_empty() {
                transcript = transcript.expect(body, "");
            }
            transcript = transcript.expect(terminator, "250 queued\r\n");
            let mut connection =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                    .await
                    .unwrap();

            connection.message(body).await.unwrap();
            transcript.assert_exhausted();
        }

        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("body\r", "")
            .expect("\n", "")
            .expect(".\r\n", "250 queued\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        connection
            .message_iter([b"body\r".as_slice(), b"\n".as_slice()].into_iter())
            .await
            .unwrap();
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelining_drains_each_recipient_window_before_writing_the_next() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients: Vec<crate::address::Address> = (0..33)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect();
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.extend(
            recipients[..32]
                .iter()
                .map(|recipient| format!("RCPT TO:<{recipient}>\r\n")),
        );
        let mut first_replies = "250 sender ok\r\n".to_owned();
        first_replies.push_str(&"250 recipient ok\r\n".repeat(32));
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                "EHLO client.example\r\n",
                "250-smtp.example\r\n250 PIPELINING\r\n",
            )
            .expect(first_window, first_replies)
            .expect(
                format!("RCPT TO:<{}>\r\n", recipients[32]),
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        connection.send(&envelope, b"body").await.unwrap();
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_rcpt_reply_read_failure_reports_unsent_not_uncertain() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = (0..32)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect();
        let mut window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        window.extend(
            addresses
                .iter()
                .map(|recipient| format!("RCPT TO:<{recipient}>\r\n")),
        );
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(
                window,
                "250 sender ok\r\n250 first accepted\r\n550 second rejected\r\n",
            );
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert!(outcome.uncertain().is_empty());
        assert_eq!(outcome.failed().len(), 32);
        assert_eq!(outcome.failed()[1].item.0, "item-1");
    }

    #[tokio::test(crate = "tokio")]
    async fn rcpt_reply_read_failure_has_no_false_account_scope() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = [
            "accepted@example.com",
            "rejected@example.com",
            "unanswered@example.com",
        ]
        .into_iter()
        .map(str::parse)
        .collect::<Result<_, _>>()
        .unwrap();
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<accepted@example.com>\r\n", "250 accepted\r\n")
            .expect("RCPT TO:<rejected@example.com>\r\n", "550 rejected\r\n")
            .expect("RCPT TO:<unanswered@example.com>\r\n", "");
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert!(outcome.uncertain().is_empty());
        assert_eq!(outcome.failed().len(), 3);
        assert_eq!(outcome.failed()[1].item.0, "item-1");
        assert!(outcome.failed()[0].error.scope().is_none());
        assert!(outcome.failed()[2].error.scope().is_none());
    }

    #[tokio::test(crate = "tokio")]
    async fn later_pipelining_window_write_failure_is_unsent() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = (0..33)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect();
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.extend(
            addresses[..32]
                .iter()
                .map(|recipient| format!("RCPT TO:<{recipient}>\r\n")),
        );
        let mut replies = "250 sender ok\r\n".to_owned();
        replies.push_str(&"250 recipient ok\r\n".repeat(31));
        replies.push_str("550 recipient rejected\r\n");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(first_window, replies);
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert!(outcome.uncertain().is_empty());
        assert!(outcome.succeeded().is_empty());
        assert_eq!(outcome.failed().len(), 33);
        assert_eq!(outcome.failed()[31].item.0, "item-31");
    }

    #[tokio::test(crate = "tokio")]
    async fn starttls_downgrade_is_refused_without_a_wire_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(!connection.can_starttls());
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(tls, &hello)
            .await
            .expect_err("a server without the STARTTLS capability must not be upgraded");
        assert!(
            error.to_string().contains("STARTTLS is not supported"),
            "expected a capability refusal, got: {error}"
        );
        assert!(error.is_policy(), "expected a policy refusal, got: {error}");

        // The refusal happened before any byte hit the wire: the very next
        // scripted step is NOOP, so a stray STARTTLS write would be rejected.
        connection.command(Noop).await.unwrap();
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn starttls_refuses_plaintext_bytes_buffered_after_the_reply() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect_coalesced(
                "STARTTLS\r\n",
                "220 go ahead\r\n250 attacker-controlled capabilities\r\n",
            );
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection
            .starttls(tls, &hello)
            .await
            .expect_err("pre-TLS injected bytes must refuse the upgrade");

        assert!(error.to_string().contains("unsolicited reply"));
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn advertised_starttls_writes_command_before_upgrade() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "220 ready to start tls\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection.starttls(tls, &hello).await.unwrap_err();

        assert!(error.to_string().contains("only supported on TCP"));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn refused_starttls_reply_does_not_upgrade() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "454 TLS temporarily unavailable\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection.starttls(tls, &hello).await.unwrap_err();

        assert!(
            error
                .smtp_response()
                .is_some_and(|response| response.has_code(454))
        );
        assert!(!connection.is_encrypted());
        transcript.assert_exhausted();
    }

    // The tests below replace the socket-listener tests this harness retired.
    // Same behaviors, same assertions, no listener or thread.

    #[tokio::test(crate = "tokio")]
    async fn abort_closes_without_quit_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        connection.abort().await;
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn peer_certificate_der_is_none_on_plaintext() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let connection = AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
            .await
            .unwrap();

        assert!(
            connection.peer_certificate_der().is_none(),
            "plaintext connection must have no peer certificate DER"
        );
    }

    #[tokio::test(crate = "tokio")]
    async fn failed_test_connected_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(!connection.test_connected().await);
        assert!(connection.has_broken());
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn abort_is_bounded_when_tls_style_shutdown_never_completes() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .stall_shutdown();
        let mut connection = AsyncSmtpConnection::from_transcript_with_timeout(
            transcript,
            &hello,
            Protocol::Smtp,
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        let mut abort = Box::pin(connection.abort());
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(abort.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::timeout(Duration::ZERO, abort)
            .await
            .expect("abort must finish when its operation timeout expires");
    }

    /// The companion to the test above, for the case it cannot reach: a
    /// transport built with `timeout(None)` has no operation timeout to bound
    /// teardown, so only the independent `TEARDOWN_TIMEOUT` cap can end the
    /// await. Without that cap this parks forever, and `Pool::shutdown` - which
    /// waits on the closes it runs - parks with it.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn abort_is_bounded_by_the_teardown_cap_when_no_operation_timeout_is_set() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .stall_shutdown();
        // `from_transcript` builds the connection with `timeout: None`.
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let mut abort = Box::pin(connection.abort());
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(abort.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        // Still pending just short of the cap: the bound is the cap, not zero.
        tokio::time::advance(TEARDOWN_TIMEOUT - Duration::from_millis(1)).await;
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(abort.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::time::timeout(Duration::ZERO, abort)
            .await
            .expect("abort must finish on the teardown cap with no operation timeout set");
    }

    #[tokio::test(crate = "tokio")]
    async fn explicit_mail_parameters_are_not_duplicated() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-PIPELINING\r\n250-SIZE 1024\r\n250-SMTPUTF8\r\n250 8BITMIME\r\n",
            )
            .expect(
                "MAIL FROM:<sender@example.com> SIZE=25 SMTPUTF8 BODY=8BITMIME\r\nRCPT TO:<recipient@exämple.com>\r\n",
                "250 sender ok\r\n250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("Subject: test\r\n\r\nHéllo", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![crate::address::Address::new_dangerous(
                "recipient",
                "exämple.com",
            )],
        )
        .unwrap();
        let options = SendOptions::new()
            .mail_parameter(MailParameter::SmtpUtfEight)
            .mail_parameter(MailParameter::Body(MailBodyParameter::EightBitMime));

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let response = connection
            .send_with_options(&envelope, "Subject: test\r\n\r\nHéllo".as_bytes(), &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn batch_eai_recipient_requires_smtputf8_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: crate::address::Address::new_dangerous("üser", "example.com"),
        }];
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let (error, _) = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .expect_err("EAI recipient must be rejected before MAIL FROM");

        assert!(error.to_string().contains("SMTPUTF8"));
        assert!(connection.test_connected().await);
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn bdat_is_length_framed_without_dot_stuffing() {
        let hello = ClientId::Domain("client.example".to_owned());
        let message = b"Subject: test\r\n\r\n.Line\r\nBinary\0";
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-CHUNKING\r\n250 BINARYMIME\r\n",
            )
            .expect(
                "MAIL FROM:<sender@example.com> BODY=BINARYMIME\r\n",
                "250 sender ok\r\n",
            )
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect(format!("BDAT {} LAST\r\n", message.len()), "")
            .expect(message, "250 queued\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let options =
            SendOptions::new().mail_parameter(MailParameter::Body(MailBodyParameter::BinaryMime));
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .send_bdat_with_options(&envelope, message, &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn unsupported_dsn_is_refused_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["first@example.com".parse().unwrap()],
        )
        .unwrap();
        let options = SendOptions::new().notify([DsnNotify::Failure]).unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection
            .send_with_options(&envelope, b"body", &options)
            .await
            .expect_err("DSN must be refused locally when it was not advertised");
        assert!(error.to_string().contains("require server DSN support"));
        assert!(connection.test_connected().await);
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn hostile_unchecked_sender_never_reaches_the_wire() {
        // `is_err()` alone would hold even without the guard, because the
        // transcript has no expectation for the smuggled `MAIL FROM` and would
        // fail the write instead. The load-bearing assertions are that nothing
        // was written and the connection is still clean.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 ok\r\n");
        let envelope = Envelope::new(
            Some(crate::address::Address::new_dangerous(
                "sender\r\nRSET",
                "example.com",
            )),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .await
                .is_err()
        );
        assert!(
            !connection.has_broken(),
            "a rejected sender must not break the connection"
        );
        assert!(
            connection.test_connected().await,
            "the connection stays reusable"
        );
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn hostile_unchecked_recipient_leaves_no_open_transaction() {
        // The sender is well-formed, so validation cannot short-circuit before
        // `MAIL FROM` for the reason the sender test covers. What must hold is
        // that the recipient is rejected while the connection is still clean:
        // no `MAIL FROM` on the wire, no abort needed, and the connection is
        // still usable afterwards rather than being pooled mid-transaction.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 ok\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "good@example.com".parse().unwrap(),
                crate::address::Address::new_dangerous("hostile\r\nRSET", "example.com"),
            ],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .await
                .is_err()
        );
        assert!(
            !connection.has_broken(),
            "a rejection before MAIL FROM must not break the connection"
        );
        assert!(
            connection.test_connected().await,
            "the connection must still be reusable, not stranded in a transaction"
        );
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn pipelined_send_rsets_when_a_recipient_is_rejected() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-PIPELINING\r\n250 SIZE 1024\r\n",
            )
            .expect(
                "MAIL FROM:<sender@example.com> SIZE=24\r\nRCPT TO:<recipient@example.com>\r\n",
                "250 sender ok\r\n550 recipient rejected\r\n",
            )
            .expect("RSET\r\n", "250 reset ok\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .await
                .is_err()
        );
        assert!(!connection.has_broken());
        assert!(connection.test_connected().await);
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn whitespace_only_ehlo_extension_is_a_parse_error() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250- \r\n250 AUTH PLAIN\r\n");

        let error =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .err()
                .expect("whitespace-only EHLO text must be rejected");

        assert!(error.is_parse(), "expected parse error, got {error:?}");
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn scram_client_final_is_sent_once_and_enhanced_235_verifies() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH SCRAM-SHA-256\r\n")
            .expect("AUTH SCRAM-SHA-256\r\n", "334 \r\n")
            .expect_generated(scram_server_first_reply)
            .expect_generated(scram_enhanced_success_reply)
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .auth(
                &[Mechanism::ScramSha256],
                &Credentials::password("user", "pencil"),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn auth_succeeds_when_the_tenth_legacy_challenge_is_answered() {
        let hello = ClientId::Domain("client.example".to_owned());
        let credentials = Credentials::oauth2("user", "token");
        let payload = bifrost_sasl::oauthbearer_payload("user", "token");
        let initial = format!(
            "AUTH OAUTHBEARER {}\r\n",
            crate::base64::encode(payload.as_bytes())
        );
        let mut transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(initial, "334 YQ==\r\n");
        for _ in 0..9 {
            transcript = transcript.expect("AQ==\r\n", "334 YQ==\r\n");
        }
        let transcript = transcript
            .expect("AQ==\r\n", "235 authenticated\r\n")
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .auth(&[Mechanism::OAuthBearer], &credentials)
            .await
            .unwrap();

        assert!(response.has_code(235));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn auth_refreshes_server_info_with_ehlo() {
        let hello = ClientId::Domain("client.example".to_owned());
        let plain = format!(
            "AUTH PLAIN {}\r\n",
            crate::base64::encode("\u{0}user\u{0}pass")
        );
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-AUTH PLAIN\r\n250 SIZE 100\r\n",
            )
            .expect(plain, "235 authenticated\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 8BITMIME\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        assert!(
            connection
                .server_info()
                .supports_auth_mechanism(Mechanism::Plain)
        );
        let response = connection
            .auth(
                &[Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));
        assert!(
            !connection
                .server_info()
                .supports_auth_mechanism(Mechanism::Plain)
        );
        assert!(
            connection
                .server_info()
                .supports_feature(Extension::EightBitMime)
        );
        transcript.assert_exhausted();
    }

    /// A minimal but structurally valid DER `Certificate` whose
    /// `signatureAlgorithm` is sha256WithRSAEncryption, matching the fixture
    /// shape bifrost-sasl's own channel-binding tests use. The parser only
    /// walks to the signatureAlgorithm OID, so this is a usable certificate
    /// for `resolve_scram_binding`.
    fn usable_peer_certificate() -> Vec<u8> {
        vec![
            0x30, 0x12, // Certificate SEQUENCE
            0x30, 0x00, // tbsCertificate: empty SEQUENCE
            0x30, 0x0b, // signatureAlgorithm SEQUENCE
            0x06, 0x09, // OID, 9 bytes
            0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01,
            0x0b, // 1.2.840.113549.1.1.11 sha256WithRSAEncryption
            0x03, 0x01, 0x00, // signatureValue: empty BIT STRING
        ]
    }

    #[tokio::test(crate = "tokio")]
    async fn plus_advertising_server_with_certificate_is_answered_with_scram_plus() {
        // Pins the connection-level `plus_candidate` gate in the true
        // direction: with a usable peer certificate and SCRAM-SHA-256-PLUS
        // advertised, the AUTH command on the wire must be the PLUS mechanism,
        // never PLAIN. The SCRAM client-first is nonce-random and cannot be
        // scripted, so the exchange is cut short with a 535 on the AUTH
        // command itself; exhausting the transcript proves the PLUS command
        // was written.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250 AUTH SCRAM-SHA-256-PLUS PLAIN\r\n",
            )
            .expect("AUTH SCRAM-SHA-256-PLUS\r\n", "535 rejected\r\n");
        let mut connection = AsyncSmtpConnection::from_transcript_with_peer_certificate(
            transcript.clone(),
            &hello,
            Protocol::Smtp,
            usable_peer_certificate(),
        )
        .await
        .unwrap();

        let error = connection
            .auth(
                &[Mechanism::ScramSha256Plus, Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap_err();

        assert!(
            error.is_permanent(),
            "expected the scripted 535, got {error:?}"
        );
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn a_refused_scram_plus_rung_is_named_in_support_diagnostics() {
        // The async half of the server-rejection naming, on the rung whose
        // name an operator most needs: the `-PLUS` suffix distinguishes the
        // channel-bound rung from the unbound one, and the name is derived
        // from the same `Mechanism` that framed the AUTH command, so the
        // reported rung cannot drift from the one on the wire.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250 AUTH SCRAM-SHA-256-PLUS PLAIN\r\n",
            )
            .expect(
                "AUTH SCRAM-SHA-256-PLUS\r\n",
                "535 5.7.8 credentials rejected\r\n",
            );
        let mut connection = AsyncSmtpConnection::from_transcript_with_peer_certificate(
            transcript.clone(),
            &hello,
            Protocol::Smtp,
            usable_peer_certificate(),
        )
        .await
        .unwrap();

        let error = connection
            .auth(
                &[Mechanism::ScramSha256Plus, Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.phase(),
            Some(super::SmtpCommandPhase::Auth {
                mechanism: Some("SCRAM-SHA-256-PLUS")
            })
        );
        let account = into_account_error(error, SmtpErrorContext::send(Protocol::Smtp));
        assert!(
            matches!(
                account.kind(),
                bifrost_types::error::AccountErrorKind::Authentication(
                    bifrost_types::error::AuthErrorKind::ReauthorizationRequired
                )
            ),
            "naming the mechanism must not move the classification: {:?}",
            account.kind()
        );
        let consented = account.support_consented();
        assert!(
            consented
                .support_text
                .iter()
                .any(|text| text.contains("mechanism SCRAM-SHA-256-PLUS")),
            "support text did not name the refused rung: {:?}",
            consented.support_text
        );
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn present_but_unusable_certificate_fails_auth_before_any_wire_write() {
        // A peer certificate that exists but cannot produce a channel binding
        // (truncated DER) must be a hard parse-class error, not a silent
        // downgrade to PLAIN-over-TLS. The transcript scripts no AUTH step, so
        // any attempt to write a fallback AUTH command would fail the
        // transcript instead of returning the typed error asserted here.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n").expect(
            HELLO,
            "250-smtp.example\r\n250 AUTH SCRAM-SHA-256-PLUS PLAIN\r\n",
        );
        let mut connection = AsyncSmtpConnection::from_transcript_with_peer_certificate(
            transcript.clone(),
            &hello,
            Protocol::Smtp,
            vec![0x30, 0x01, 0x00],
        )
        .await
        .unwrap();

        let error = connection
            .auth(
                &[Mechanism::ScramSha256Plus, Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap_err();

        assert!(
            error.is_parse(),
            "expected the typed binding parse error, got {error:?}"
        );
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn plus_advertised_without_certificate_falls_through_to_plain() {
        // No certificate at all (plaintext transcript, nothing injected):
        // the PLUS rung is skipped as binding-unavailable and PLAIN is the
        // answer. This is the only fall-through `resolve_scram_binding`
        // permits.
        let hello = ClientId::Domain("client.example".to_owned());
        let plain = format!(
            "AUTH PLAIN {}\r\n",
            crate::base64::encode("\u{0}user\u{0}pass")
        );
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250 AUTH SCRAM-SHA-256-PLUS PLAIN\r\n",
            )
            .expect(plain, "235 authenticated\r\n")
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .auth(
                &[Mechanism::ScramSha256Plus, Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn login_answers_challenges_by_position_not_by_prompt_text() {
        // Both prompts are deliberately outside the English wordlist the driver
        // used to match on. A prompt-matching implementation rejects the
        // exchange with "Unrecognized challenge"; a positional one answers
        // username then password regardless of wording.
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH LOGIN\r\n")
            .expect("AUTH LOGIN\r\n", {
                let prompt = crate::base64::encode("Nom d'utilisateur :");
                format!("334 {prompt}\r\n")
            })
            .expect(format!("{}\r\n", crate::base64::encode("user")), {
                let prompt = crate::base64::encode("Mot de passe :");
                format!("334 {prompt}\r\n")
            })
            .expect(
                format!("{}\r\n", crate::base64::encode("pass")),
                "235 authenticated\r\n",
            )
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .auth(
                &[Mechanism::Login],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn oauthbearer_auth_sends_initial_response_and_refreshes_ehlo() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=us=2Cer=3Done,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "235 authenticated\r\n",
            )
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("us,er=one", "token"),
            )
            .await
            .unwrap();

        assert!(response.has_code(235));
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn oauthbearer_immediate_rejection_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "535 rejected\r\n",
            );
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("user", "token"),
            )
            .await
            .unwrap_err();

        assert!(
            error.is_permanent(),
            "expected permanent SMTP error: {error:?}"
        );
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn oauthbearer_failed_challenge_sends_cancel_response() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(format!("AUTH OAUTHBEARER {initial}\r\n"), "334 e30=\r\n")
            .expect("AQ==\r\n", "535 rejected\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("user", "token"),
            )
            .await
            .unwrap_err();

        assert!(
            error.is_permanent(),
            "expected permanent SMTP error: {error:?}"
        );
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn connect_times_out_waiting_for_banner() {
        let hello = ClientId::Domain("client.example".to_owned());
        // The peer accepts and then says nothing at all.
        let Err(error) = AsyncSmtpConnection::from_transcript_with_timeout(
            Transcript::silent(),
            &hello,
            Protocol::Smtp,
            Duration::from_millis(50),
        )
        .await
        else {
            panic!("connect must time out while waiting for banner");
        };

        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn connect_setup_uses_single_deadline_for_banner_and_ehlo() {
        let hello = ClientId::Domain("client.example".to_owned());
        // The banner arrives, EHLO is written, and then the peer goes silent.
        // A per-operation timeout would restart the clock at EHLO; a single
        // setup deadline must still fire.
        let transcript = Transcript::new("220 smtp.example\r\n").expect_then_stall(HELLO);
        let Err(error) = AsyncSmtpConnection::from_transcript_with_timeout(
            transcript,
            &hello,
            Protocol::Smtp,
            Duration::from_millis(120),
        )
        .await
        else {
            panic!("connect must use one setup deadline across banner and EHLO");
        };

        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn command_times_out_waiting_for_response() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_then_stall("NOOP\r\n");
        let mut connection = AsyncSmtpConnection::from_transcript_with_timeout(
            transcript,
            &hello,
            Protocol::Smtp,
            Duration::from_millis(50),
        )
        .await
        .unwrap();

        let error = connection
            .command(Noop)
            .await
            .expect_err("NOOP must time out while waiting for response");
        assert!(error.is_timeout(), "expected timeout, got {error:?}");
    }

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn cancelled_command_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_then_stall("NOOP\r\n");
        let mut connection = AsyncSmtpConnection::from_transcript_with_timeout(
            transcript,
            &hello,
            Protocol::Smtp,
            Duration::from_secs(2),
        )
        .await
        .unwrap();

        // Dropping a read mid-response loses stream position, so the
        // connection must not be reused.
        let result =
            tokio::time::timeout(Duration::from_millis(50), connection.command(Noop)).await;
        assert!(result.is_err(), "command future must be cancelled");
        assert!(connection.has_broken());

        let error = connection.command(Noop).await.unwrap_err();
        assert!(
            error.is_connection(),
            "expected connection error: {error:?}"
        );
    }

    #[tokio::test(crate = "tokio")]
    async fn lmtp_drain_retires_the_connection_and_breaks_it_on_a_surplus_status() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = vec![
            "first@example.com".parse().unwrap(),
            "second@example.com".parse().unwrap(),
        ];
        let clean = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n",
            );
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            recipients.clone(),
        )
        .unwrap();
        let mut connection = AsyncSmtpConnection::from_transcript(clean, &hello, Protocol::Lmtp)
            .await
            .unwrap();
        assert_eq!(
            connection
                .send_lmtp(&envelope, b"body")
                .await
                .unwrap()
                .len(),
            2
        );
        // Cleanliness below the read buffer is unprovable, so the connection
        // is retired rather than recycled - but it is not an error.
        assert!(connection.should_retire());
        assert!(!connection.has_broken());

        // Surplus bytes that arrive in the same segment do reach the read
        // buffer, and that case is a hard error.
        let surplus = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect_coalesced(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n250 surplus response\r\n",
            );
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();
        let mut connection = AsyncSmtpConnection::from_transcript(surplus, &hello, Protocol::Lmtp)
            .await
            .unwrap();
        let error = connection
            .send_lmtp(&envelope, b"body")
            .await
            .expect_err("a surplus final status desynchronizes the stream");
        assert!(
            error.to_string().contains("more final statuses"),
            "expected an honest LMTP final-status error, got: {error}"
        );
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }

    /// `Op::Abort` is a request until the adapter performs it. The core's
    /// batch `MAIL FROM` rejection asks for one; what has to be true at the
    /// wire afterwards is that the stream can never be handed out again.
    #[tokio::test(crate = "tokio")]
    async fn an_aborting_batch_failure_leaves_the_stream_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        // Nothing follows the rejection: an abort writes no QUIT.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect(
                "MAIL FROM:<sender@example.com>\r\n",
                "550 sender rejected\r\n",
            );
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "first@example.com".parse().unwrap(),
        }];

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .await
            .expect_err("a rejected batch MAIL FROM is a batch-level failure");

        assert!(
            connection.has_broken(),
            "an aborted connection must never pass state().verify() again"
        );
        assert!(
            !connection.should_retire(),
            "retirement is the LMTP drain's flag, not an abort's"
        );
        transcript.assert_exhausted();
    }

    /// `OpenReplyGroup` holds the stream `Broken` for the whole window and
    /// `CloseReplyGroup` restores it. A rejected pipelined `MAIL FROM` is the
    /// case that proves the restore happens at the close: the window still
    /// drains, no RSET is sent, nothing is aborted, and the connection is
    /// reusable at the end even though the send failed.
    #[tokio::test(crate = "tokio")]
    async fn a_rejected_pipelined_mail_from_restores_the_stream_at_the_group_close() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = ["first@example.com", "second@example.com"]
            .into_iter()
            .map(|address| address.parse().unwrap())
            .collect();
        let window = "MAIL FROM:<sender@example.com>\r\n\
                      RCPT TO:<first@example.com>\r\n\
                      RCPT TO:<second@example.com>\r\n";
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect_coalesced(
                window,
                "550 sender rejected\r\n250 recipient ok\r\n250 recipient ok\r\n",
            );
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), addresses).unwrap();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        connection
            .send_with_options(&envelope, b"body", &SendOptions::default())
            .await
            .expect_err("MAIL FROM was rejected");

        assert!(
            !connection.has_broken(),
            "the group close restores a stream whose replies all drained"
        );
        assert!(!connection.should_retire());
        transcript.assert_exhausted();
    }

    /// `CloseLmtpDrain { restore_ok: false }` is the batch LMTP drain, and the
    /// `false` has to reach the wire: unlike a direct LMTP send, a clean batch
    /// drain leaves the stream `Broken` as well as retired.
    #[tokio::test(crate = "tokio")]
    async fn a_clean_lmtp_batch_drain_leaves_the_stream_broken_and_retired() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n",
            );
        let batch = ["first@example.com", "second@example.com"]
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address: address.parse().unwrap(),
            })
            .collect();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                .await
                .unwrap();
        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert_eq!(outcome.succeeded().len(), 2);
        assert!(connection.should_retire());
        assert!(
            connection.has_broken(),
            "an LMTP batch drain never restores the stream"
        );
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn lmtp_too_few_final_statuses_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect_then_close("\r\n.\r\n", "250 first delivered\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "first@example.com".parse().unwrap(),
                "second@example.com".parse().unwrap(),
            ],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                .await
                .unwrap();

        assert!(connection.send_lmtp(&envelope, b"body").await.is_err());
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[tokio::test(crate = "tokio")]
    async fn peer_closing_after_data_acceptance_leaves_recipient_uncertain() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect_then_close("DATA\r\n", "354 send body\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "first@example.com".parse().unwrap(),
        }];
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .expect("a body-write failure is represented per recipient")
            .resolve();

        assert!(outcome.succeeded().is_empty());
        assert_eq!(outcome.uncertain().len(), 1);
        assert_eq!(outcome.uncertain()[0].item.0, "item-0");
        assert!(connection.has_broken());
    }

    /// Every envelope-level parameter this crate can emit, on one pipelined
    /// peer, in one window. This is the widest wire-shape assertion in the
    /// crate: SIZE, HOLDFOR, BY, MT-PRIORITY, RET and the xtext-escaped ENVID
    /// on `MAIL FROM`, uniform NOTIFY plus a per-recipient ORCPT on the RCPT
    /// lines, and all of it written before any reply is read.
    #[tokio::test(crate = "tokio")]
    async fn send_with_options_pipelines_envelope_parameters_in_one_window() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(
                HELLO,
                "250-smtp.example\r\n250-PIPELINING\r\n250-SIZE 1024\r\n250-FUTURERELEASE 3600\r\n250-DELIVERBY 240\r\n250-MT-PRIORITY\r\n250 DSN\r\n",
            )
            .expect(
                concat!(
                    "MAIL FROM:<sender@example.com> SIZE=24 HOLDFOR=60 BY=300;R MT-PRIORITY=-1 RET=HDRS ENVID=env+3D1\r\n",
                    "RCPT TO:<first@example.com> NOTIFY=FAILURE,DELAY ORCPT=rfc822;alias+3Dfirst@example.com\r\n",
                    "RCPT TO:<second@example.com> NOTIFY=FAILURE,DELAY\r\n",
                ),
                "250 sender ok\r\n250 first ok\r\n250 second ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("Subject: test\r\n\r\nHello", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let first_recipient: crate::address::Address = "first@example.com".parse().unwrap();
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                first_recipient.clone(),
                "second@example.com".parse().unwrap(),
            ],
        )
        .unwrap();
        let options = SendOptions::new()
            .hold_for(60)
            .deliver_by(300, DeliverByMode::Return, false)
            .unwrap()
            .mt_priority(-1)
            .unwrap()
            .dsn_return(DsnReturn::Headers)
            .envelope_id("env=1")
            .notify([DsnNotify::Failure, DsnNotify::Delay])
            .unwrap()
            .recipient_original_recipient(first_recipient, "rfc822", "alias=first@example.com");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let response = connection
            .send_with_options(&envelope, b"Subject: test\r\n\r\nHello", &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    /// Per-recipient DSN parameters override the uniform ones on the matching
    /// RCPT line only, and the batch path emits the same wire shape as the
    /// envelope path. Sequential (non-PIPELINING) peer, so each RCPT line is
    /// pinned as its own write.
    #[tokio::test(crate = "tokio")]
    async fn batch_rcpt_options_are_emitted_per_recipient_in_order() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 DSN\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect(
                "RCPT TO:<first@example.com> NOTIFY=NEVER ORCPT=rfc822;alias+3Dfirst@example.com\r\n",
                "250 first ok\r\n",
            )
            .expect(
                "RCPT TO:<second@example.com> NOTIFY=FAILURE,DELAY\r\n",
                "250 second ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let first: crate::address::Address = "first@example.com".parse().unwrap();
        let options = SendOptions::new()
            .notify([DsnNotify::Failure, DsnNotify::Delay])
            .unwrap()
            .recipient_never_notify(first.clone())
            .recipient_original_recipient(first.clone(), "rfc822", "alias=first@example.com");
        let batch = ["first@example.com", "second@example.com"]
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address: address.parse().unwrap(),
            })
            .collect();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &options,
            )
            .await
            .expect("the transaction completes")
            .resolve();

        assert_eq!(outcome.succeeded().len(), 2);
        transcript.assert_exhausted();
    }

    /// Recipient-specific parameters are keyed by address. A parameter naming
    /// an address that is not in the batch is a caller bug and must be caught
    /// before `MAIL FROM`, leaving the connection clean and reusable.
    #[tokio::test(crate = "tokio")]
    async fn batch_rcpt_parameters_for_an_unknown_recipient_fail_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 DSN\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let options = SendOptions::new()
            .recipient_notify("stranger@example.com".parse().unwrap(), [DsnNotify::Never])
            .unwrap();
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "first@example.com".parse().unwrap(),
        }];
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let (error, progress) = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &options,
            )
            .await
            .expect_err("unmatched recipient parameters are a pre-wire input error");
        assert!(
            error.to_string().contains("do not match a batch recipient"),
            "expected an unmatched-recipient refusal, got: {error}"
        );
        assert!(progress.resolve().succeeded().is_empty());

        // Nothing was written: the next scripted step is still NOOP.
        connection.command(Noop).await.unwrap();
        transcript.assert_exhausted();
    }

    /// A recipient rejected in the FIRST pipelining window must be reported
    /// against its own batch id while later-window recipients still succeed.
    /// The window boundary is where an implementation that re-indexes per
    /// window misattributes the rejection.
    #[tokio::test(crate = "tokio")]
    async fn pipelined_batch_keeps_original_indexes_across_a_window_boundary() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses: Vec<crate::address::Address> = (0..33)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect();
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.extend(
            addresses[..32]
                .iter()
                .map(|recipient| format!("RCPT TO:<{recipient}>\r\n")),
        );
        let mut first_replies = "250 sender ok\r\n".to_owned();
        first_replies.push_str(&"250 recipient ok\r\n".repeat(31));
        first_replies.push_str("550 recipient rejected\r\n");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(first_window, first_replies)
            .expect(
                format!("RCPT TO:<{}>\r\n", addresses[32]),
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "250 queued\r\n");
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "item-31");
        assert_eq!(outcome.succeeded().last().unwrap().item.0, "item-32");
        transcript.assert_exhausted();
    }

    /// One final status per accepted recipient, mapped positionally: the
    /// second recipient's rejection must not be reported against the first.
    #[tokio::test(crate = "tokio")]
    async fn lmtp_drains_one_final_status_per_accepted_recipient() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect(
                "\r\n.\r\n",
                "250 first delivered\r\n550 second rejected\r\n",
            );
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec![
                "first@example.com".parse().unwrap(),
                "second@example.com".parse().unwrap(),
            ],
        )
        .unwrap();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                .await
                .unwrap();

        let statuses = connection.send_lmtp(&envelope, b"body").await.unwrap();

        assert_eq!(statuses.len(), 2);
        assert!(statuses[0].is_positive());
        assert!(!statuses[1].is_positive());
        // Surplus bytes below the read buffer are undetectable, so a completed
        // LMTP drain retires the connection rather than recycling it.
        assert!(connection.should_retire());
        transcript.assert_exhausted();
    }

    /// The batch LMTP path, unlike `send_lmtp`, must keep the per-recipient
    /// outcomes it already proved even when a surplus status desynchronizes
    /// the stream. Both deliveries are known; only the connection is lost.
    #[tokio::test(crate = "tokio")]
    async fn lmtp_batch_surplus_final_status_retires_the_connection_and_keeps_outcomes() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect_coalesced(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n250 surplus response\r\n",
            );
        let batch = ["first@example.com", "second@example.com"]
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address: address.parse().unwrap(),
            })
            .collect();
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Lmtp)
                .await
                .unwrap();

        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert_eq!(outcome.succeeded().len(), 2);
        assert!(outcome.uncertain().is_empty());
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }

    /// A reply line cut short by a close is a parse failure, not a usable
    /// connection carrying a half-read capability set.
    #[tokio::test(crate = "tokio")]
    async fn peer_closing_mid_reply_line_is_a_parse_failure() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect_then_close(HELLO, "250-smtp.example\r\n250 PIPELI");

        let Err(error) =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).await
        else {
            panic!("a truncated EHLO reply cannot yield a usable connection");
        };
        assert!(
            error.to_string().contains("incomplete response"),
            "expected an incomplete-response parse error, got: {error}"
        );
    }

    /// An unsolicited reply coalesced with a requested reply breaks the
    /// connection before it can be mistaken for the next command's answer.
    #[tokio::test(crate = "tokio")]
    async fn unsolicited_reply_coalesced_with_an_answer_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_coalesced("NOOP\r\n", "250 noop ok\r\n421 service closing\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection
            .command(Noop)
            .await
            .expect_err("the surplus reply must break the connection immediately");
        assert!(
            error.to_string().contains("unsolicited reply"),
            "expected a desynchronization error, got: {error}"
        );
        assert!(connection.has_broken());
    }

    /// A negative reply to the end-of-data terminator is an ANSWER, not a
    /// transport failure: the server read the whole message and refused it, so
    /// the transaction is complete (RFC 5321 4.1.1.4), every accepted
    /// recipient is a `failed` lane classified under `DataFinal`, and the
    /// connection stays reusable. Routing it through the `Failed` arm put the
    /// recipients in `uncertain` (asking the caller to reconcile a delivery the
    /// server explicitly refused) and threw away a healthy connection.
    #[tokio::test(crate = "tokio")]
    async fn a_rejected_data_final_reply_fails_the_recipients_and_keeps_the_connection() {
        use bifrost_types::error::{AccountErrorKind, ServerErrorKind};

        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "554 Message rejected as spam\r\n");
        let batch = ["first@example.com", "second@example.com"]
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address: address.parse().unwrap(),
            })
            .collect();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .expect("an answered transaction is not a batch-level failure")
            .resolve();

        assert_eq!(outcome.failed().len(), 2);
        assert!(outcome.succeeded().is_empty());
        assert!(
            outcome.uncertain().is_empty(),
            "the server answered, so nothing is uncertain"
        );
        assert!(matches!(
            outcome.failed()[0].error.kind(),
            AccountErrorKind::Server(ServerErrorKind::Error { status: Some(554) })
        ));
        // An SMTP 5xx is permanent (RFC 5321 4.2.1); see the blocking twin.
        assert_eq!(
            outcome.failed()[0].error.recovery(),
            &bifrost_types::RecoveryClass::ProviderRefused,
            "a permanent SMTP refusal must not be advice to resend"
        );
        assert!(
            !connection.has_broken(),
            "the transaction completed; the connection is still clean"
        );
        transcript.assert_exhausted();
    }

    /// The direct path takes the same rule: a refused message on an answered
    /// transaction is a status error on a reusable connection, not an abort.
    #[tokio::test(crate = "tokio")]
    async fn a_rejected_data_final_reply_keeps_the_direct_connection_reusable() {
        let hello = ClientId::Domain("client.example".to_owned());
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "554 Message rejected as spam\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.status().map(u16::from), Some(554));
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataFinal));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    /// The end-of-data completion rule is RFC 5321's, and it is about DATA.
    /// `BDAT ... LAST` reaches the same `FinalReply` stage, but RFC 3030
    /// promises nothing about the transaction after a refused chunk - its own
    /// failure example sends RSET after the negative reply - so a strict peer
    /// may still consider the transaction open. Finishing without a reset
    /// parked such a connection `Ok`, and the next checkout's `MAIL FROM`
    /// landed inside a live transaction. RSET-and-keep instead: the reset must
    /// appear on the wire and the connection survives only because the peer
    /// acknowledged it.
    #[tokio::test(crate = "tokio")]
    async fn a_rejected_bdat_last_resets_the_transaction_before_the_connection_is_reused() {
        let hello = ClientId::Domain("client.example".to_owned());
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 CHUNKING\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect("BDAT 4 LAST\r\n", "")
            .expect("body", "452 4.3.1 out of storage\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection
            .send_bdat_with_options(&envelope, b"body", &SendOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.status().map(u16::from), Some(452));
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::BdatBody));
        assert!(
            !connection.has_broken(),
            "the peer acknowledged the reset, so the connection is reusable"
        );
        transcript.assert_exhausted();
    }

    /// A rejected end-of-data reply keeps the connection - unless the peer
    /// said it is going away. 421 is "closing transmission channel", so
    /// parking it would hand the next checkout a dead socket. The status
    /// error is still what the caller sees.
    #[tokio::test(crate = "tokio")]
    async fn a_421_data_final_reply_aborts_instead_of_parking_a_dying_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "421 closing transmission channel\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();

        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.status().map(u16::from), Some(421));
        assert!(
            connection.has_broken(),
            "a peer closing the channel must not leave a reusable connection"
        );
        transcript.assert_exhausted();
    }

    /// The batch machine takes the same 421 rule, and it is the path that
    /// matters most: `send_smtp_batch` is the account-level send, so a
    /// connection parked after the peer announced it is closing goes back
    /// into the pool, and the next checkout under `test_on_checkout(false)`
    /// writes `MAIL FROM` into a dead socket. The lanes still resolve from
    /// the answer - `DataFinal` `failed`, never `uncertain` - only the
    /// connection is retired.
    #[tokio::test(crate = "tokio")]
    async fn a_421_data_final_reply_aborts_the_batch_connection_too() {
        use bifrost_types::error::{AccountErrorKind, ServerErrorKind};

        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "421 closing transmission channel\r\n");
        let batch = ["first@example.com", "second@example.com"]
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address: address.parse().unwrap(),
            })
            .collect();

        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                .await
                .unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .await
            .expect("an answered transaction is not a batch-level failure")
            .resolve();

        assert_eq!(outcome.failed().len(), 2);
        assert!(
            outcome.uncertain().is_empty(),
            "the server answered, so nothing is uncertain"
        );
        assert!(matches!(
            outcome.failed()[0].error.kind(),
            AccountErrorKind::Server(ServerErrorKind::Unavailable)
        ));
        assert!(
            connection.has_broken(),
            "a peer closing the channel must not leave a poolable connection"
        );
        transcript.assert_exhausted();
    }

    /// The per-operation timeout bounds one write that makes no progress, not
    /// the whole transfer. Armed once around the entire body it becomes a
    /// function of message size and link speed - a large message over a slow
    /// uplink times out while uploading perfectly happily and lands every
    /// accepted recipient in `uncertain`. The blocking half never had this,
    /// because `SO_SNDTIMEO` is per `write(2)`.
    ///
    /// This is the SLOW-LINK half only. The bandwidth-cap half of the same
    /// hazard - a throttle wait spent inside a write's timeout - is held by the
    /// write loop waiting for admission before it arms the timeout, pinned by
    /// `a_capped_upload_outlives_a_write_timeout_shorter_than_its_throttle_debt`
    /// below, with the size of a single admission bounded in the socket funnel
    /// by `a_capped_write_offers_at_most_one_second_of_cap`.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_slow_but_progressing_body_upload_outlives_the_operation_timeout() {
        use crate::transport::smtp::test_support::SlowSinkPeer;

        let hello = ClientId::Domain("client.example".to_owned());
        // 16 KiB per second against a 10 s timeout: 256 KiB takes 16 s of
        // virtual time, and every individual write completes in one second.
        let peer = SlowSinkPeer::new(16 * 1024, Duration::from_secs(1));
        let mut connection = AsyncSmtpConnection::from_raw_stream_for_test(
            Box::new(peer),
            &hello,
            Protocol::Smtp,
            Some(Duration::from_secs(10)),
        );

        let body = vec![b'x'; 256 * 1024];
        let started = tokio::time::Instant::now();
        connection
            .write_body_iter(std::iter::once(body.as_slice()))
            .await
            .expect("a peer that keeps accepting bytes must not time out");

        assert!(
            started.elapsed() > Duration::from_secs(10),
            "the upload must have outlasted one operation timeout to mean anything, took {:?}",
            started.elapsed()
        );
    }

    /// A bandwidth cap must not consume the per-operation write timeout, at
    /// ANY timeout.
    ///
    /// Bounding each write at about one second of cap keeps the timeout
    /// meaningful only while the timeout is comfortably larger than a second.
    /// It is not: with a sub-second timeout, every capped write after the first
    /// once waited out its throttle inside `with_timeout` and failed as "SMTP
    /// write timed out" - a healthy upload, throttled by this crate at the
    /// consumer's own request, reported as a dead peer with every recipient
    /// uncertain. The write loop waits for outbound admission before arming
    /// the timeout, so the budget is only ever spent on the socket. The cap
    /// itself is unchanged: the upload below still takes longer than the
    /// timeout, it just does not fail.
    ///
    /// The elapsed assertion is a floor, not a duration: it says the upload
    /// was throttled past the timeout at all, which is what makes the
    /// successful return mean something. It is deliberately not a claim about
    /// how long 5 kB at 1000 B/s takes.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_capped_upload_outlives_a_write_timeout_shorter_than_its_throttle_debt() {
        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::SlowSinkPeer;

        let hello = ClientId::Domain("client.example".to_owned());
        // A peer that accepts everything offered, instantly: the only thing
        // that can delay this upload is the cap.
        let mut connection = AsyncSmtpConnection::from_raw_stream_for_test(
            Box::new(SlowSinkPeer::new(usize::MAX, Duration::ZERO)),
            &hello,
            Protocol::Smtp,
            Some(Duration::from_millis(200)),
        );
        // 1000 B/s against a 200 ms timeout. The first write burns the bucket's
        // initial second of tokens; every one after it parks at least a full
        // second of debt, five times the timeout.
        let cap = bifrost_net::BandwidthBudget::new(Some(1000));
        connection
            .stream
            .get_mut()
            .set_metering(WireMetering::new(None, Some(cap)));

        let body = vec![b'x'; 5_000];
        let started = tokio::time::Instant::now();
        connection
            .write_body_iter(std::iter::once(body.as_slice()))
            .await
            .expect("throttle debt is this crate's own wait, not the peer's silence");

        assert!(
            started.elapsed() > Duration::from_millis(200),
            "the upload must have been throttled past the timeout to mean anything, took {:?}",
            started.elapsed()
        );
    }

    /// A stream whose next write waits a little over a second for admission,
    /// and whose peer then wedges for good.
    ///
    /// At a 1000 B/s cap the stream's own 1000-byte write spends the
    /// schedule's second of credit, and a SIBLING connection sharing the same
    /// metering holds the next second - returned so it stays held. The
    /// stream's next write therefore queues behind both: a 21-byte EHLO waits
    /// 1021 ms. The peer accepts the one write and no more, which is what lets
    /// the tests below make the write AFTER admission fail on its own - a
    /// second peer is not an option, since the stream owns one.
    async fn stream_queued_behind_a_sibling() -> (
        tokio::io::BufReader<AsyncNetworkStream>,
        crate::transport::smtp::client::metering::Reservation,
    ) {
        use crate::transport::smtp::client::metering::WireMetering;
        use crate::transport::smtp::test_support::SlowSinkPeer;

        let metering = WireMetering::new(None, Some(bifrost_net::BandwidthBudget::new(Some(1000))));
        let mut stream = AsyncNetworkStream::from_raw_stream_for_test(Box::new(
            SlowSinkPeer::accepting(1, 100_000),
        ));
        stream.set_metering(metering.clone());
        let mut stream = tokio::io::BufReader::new(stream);

        AsyncSmtpConnection::write_stream_with_budget(
            &mut stream,
            &vec![b'x'; 1_000],
            TimeoutBudget::PerOperation(None),
        )
        .await
        .expect("an unbounded write cannot fail against a peer that is still accepting");
        let sibling = metering.reserve_out(1_000);

        (stream, sibling)
    }

    /// An admission wait that runs outside a `SetupDeadline` does not SPEND
    /// the deadline, it waits PAST it.
    ///
    /// The setup deadline is absolute and shared across DNS, connect, TLS, the
    /// banner and EHLO, so "this wait is ours, not the peer's" - the reason a
    /// per-operation write timeout may be left entirely - buys nothing here:
    /// leaving the ceiling is the whole failure. Under a cap small enough to
    /// throttle a greeting, an EHLO write waits for its slot while the
    /// remaining setup budget is milliseconds, and the connect would report a
    /// timeout only after blowing through the ceiling the contract says it
    /// cannot exceed. The wait is therefore bounded by the slack left on the
    /// deadline, and admission that cannot come inside the setup budget fails
    /// as a setup timeout.
    ///
    /// The ERROR is not what bites here - an unbounded wait also ends in a
    /// timeout, because the write it eventually reaches finds the deadline
    /// already spent. The elapsed time is the whole finding: 300 ms of ceiling
    /// against a 1021 ms wait.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_setup_deadline_bounds_the_admission_wait_rather_than_waiting_past_it() {
        let (mut stream, _sibling) = stream_queued_behind_a_sibling().await;

        // 300 ms of setup budget against a slot a second away.
        let deadline = AsyncDeadline::new(Some(Duration::from_millis(300)));
        let started = tokio::time::Instant::now();
        let error = AsyncSmtpConnection::write_stream_with_budget(
            &mut stream,
            b"EHLO client.example\r\n",
            TimeoutBudget::SetupDeadline(deadline),
        )
        .await
        .expect_err("admission that outlasts the setup budget must fail as a setup timeout");

        assert!(error.is_timeout(), "expected a timeout, got {error:?}");
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(300),
            "the deadline is a ceiling on setup however the time is spent"
        );
    }

    /// And the write after admission gets the slack that is LEFT, recomputed.
    ///
    /// The easy mistake in bounding the wait is to read the remaining slack
    /// once, spend part of it waiting, and then arm the write with that same
    /// pre-wait duration - which hands back exactly the overrun the bounding
    /// removed, just in smaller pieces. Passing the deadline itself through is
    /// what avoids it: `with_timeout` resolves a `SetupDeadline` at the moment
    /// it arms the timer.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn the_write_after_a_bounded_admission_gets_the_recomputed_slack() {
        // The peer has stopped accepting, so the write that follows admission
        // ends on its own timeout and the total says which one it was armed
        // with.
        let (mut stream, _sibling) = stream_queued_behind_a_sibling().await;

        // 1200 ms total: 1021 ms waiting for the slot, 179 ms left for the write.
        let deadline = AsyncDeadline::new(Some(Duration::from_millis(1_200)));
        let started = tokio::time::Instant::now();
        let error = AsyncSmtpConnection::write_stream_with_budget(
            &mut stream,
            b"EHLO client.example\r\n",
            TimeoutBudget::SetupDeadline(deadline),
        )
        .await
        .expect_err("a wedged peer must still hit the write timeout");

        assert!(error.is_timeout(), "expected a timeout, got {error:?}");
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(1_200),
            "the write must be armed with the slack left AFTER admission, not before it"
        );
    }

    /// The other half of the same rule: a peer that accepts nothing at all
    /// must still hit the write timeout, so the re-arming loop above is not a
    /// licence to hang.
    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn a_stalled_body_upload_still_hits_the_write_timeout() {
        use crate::transport::smtp::test_support::SlowSinkPeer;

        let hello = ClientId::Domain("client.example".to_owned());
        let mut connection = AsyncSmtpConnection::from_raw_stream_for_test(
            Box::new(SlowSinkPeer::stalled()),
            &hello,
            Protocol::Smtp,
            Some(Duration::from_secs(10)),
        );

        let body = vec![b'x'; 256 * 1024];
        let error = connection
            .write_body_iter(std::iter::once(body.as_slice()))
            .await
            .expect_err("a peer accepting no bytes must time out");
        assert!(error.is_timeout(), "expected a timeout, got {error:?}");
    }
}
