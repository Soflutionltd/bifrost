use crate::transport::smtp::authentication::SmtpCredentialsExt;
#[cfg(feature = "account-error")]
use crate::transport::smtp::batch::{SendProgress, SmtpBatchRecipient};
#[cfg(all(test, feature = "account-error"))]
use crate::transport::smtp::error::SmtpCommandPhase;
use std::net::IpAddr;
#[cfg(unix)]
use std::path::Path;
use std::{
    fmt::{Display, Write as _},
    future::Future,
    time::Duration,
};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use zeroize::{Zeroize, Zeroizing};

use super::async_net::AsyncDeadline;
use super::metering::WireMetering;
use super::{
    ClientCodec, ConnectionState, TlsParameters,
    async_net::AsyncNetworkStream,
    core,
    core::{Op, OpOutcome, ProtocolMachine, Step},
    data_terminator,
};
use super::{ReplyMode, reply::ReplyAssembler, session::Session};
#[cfg(feature = "account-error")]
use crate::address::Address;
use crate::{
    Envelope,
    transport::smtp::{
        Dialect,
        authentication::{Credentials, Mechanism},
        commands::{Expn, Mail, Noop, Rcpt, Vrfy},
        error,
        error::Error,
        extension::{ClientId, Extension, SendOptions, ServerInfo},
        response::Response,
    },
};

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
    protocol: Dialect,
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
            Dialect::Smtp,
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
        protocol: Dialect,
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
        protocol: Dialect,
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
        protocol: Dialect,
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
        let mut machine = Session::greeting(protocol, hello_name);
        let (info, _) = conn
            .drive_with_budget(&mut machine, &[], setup_budget)
            .await?;
        conn.server_info = info;

        // Print server information
        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    // Widened so the pool and transport tests can drive a scripted peer
    // through the public entry points instead of a socket.
    #[cfg(test)]
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(in crate::transport::smtp) async fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Dialect,
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
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(in crate::transport::smtp) async fn from_transcript_metered(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Dialect,
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
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(in crate::transport::smtp) async fn from_transcript_with_peer_certificate(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Dialect,
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
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(in crate::transport::smtp) fn from_raw_stream_for_test(
        stream: Box<dyn super::async_net::AsyncTokioStream>,
        hello_name: &ClientId,
        protocol: Dialect,
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
        protocol: Dialect,
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
        super::envelope::EnvelopeBuilder {
            server_info: self.server_info(),
            encrypted: self.is_encrypted(),
        }
        .build_envelope(envelope, email, options, allow_binary_mime)
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
        let mut machine = core::DirectSmtp::new(
            mail,
            recipients,
            self.server_info.supports_pipelining(),
            core::BodyKind::Bdat,
        );
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
        let mut machine = core::DirectLmtp::new(
            mail,
            recipients,
            self.server_info.supports_pipelining(),
            core::BodyKind::Data,
        );
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
        let mut machine = core::DirectLmtp::new(
            mail,
            recipients,
            self.server_info.supports_pipelining(),
            core::BodyKind::Bdat,
        );
        self.drive(&mut machine, email).await
    }

    /// Async account-oriented SMTP multi-recipient send.
    ///
    /// Returns `Ok(progress)` when the command sequence completes (even if
    /// some recipients were rejected). Returns `Err((error, progress))` for
    /// batch-level failures where no recipient-specific outcome can be
    /// attributed.
    #[cfg(feature = "account-error")]
    pub(crate) async fn send_smtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        let (mail_cmd, rcpt_cmds, progress) =
            self.build_batch(Dialect::Smtp, from, recipients, email, options)?;
        let pipelined = self.server_info().supports_pipelining();
        let mut machine = core::BatchTransaction::batch(
            mail_cmd,
            rcpt_cmds,
            pipelined,
            core::BodyKind::Data,
            progress,
        );
        self.drive(&mut machine, email).await
    }

    /// Async account-oriented LMTP multi-recipient send.
    ///
    /// Like `send_smtp_batch` but reads one final status per accepted
    /// recipient after the DATA body.
    #[cfg(feature = "account-error")]
    pub(crate) async fn send_lmtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        let (mail_cmd, rcpt_cmds, progress) =
            self.build_batch(Dialect::Lmtp, from, recipients, email, options)?;
        let mut machine = core::BatchTransaction::batch(
            mail_cmd,
            rcpt_cmds,
            self.server_info.supports_pipelining(),
            core::BodyKind::Data,
            progress,
        );
        self.drive(&mut machine, email).await
    }

    /// Local validation and command construction for a batch send, all of it
    /// before `MAIL FROM` opens a transaction (see `build_envelope`).
    #[allow(clippy::type_complexity)]
    #[cfg(feature = "account-error")]
    fn build_batch(
        &self,
        protocol: Dialect,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<(Mail, Vec<Rcpt>, SendProgress), (Error, SendProgress)> {
        super::envelope::EnvelopeBuilder {
            server_info: self.server_info(),
            encrypted: self.is_encrypted(),
        }
        .build_batch(protocol, from, recipients, email, options)
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
        self.drive_with_budget(machine, email, self.per_operation_budget())
            .await
    }

    async fn drive_with_budget<M: ProtocolMachine>(
        &mut self,
        machine: &mut M,
        email: &[u8],
        budget: TimeoutBudget,
    ) -> M::Output {
        let mut outcome = OpOutcome::Done;
        loop {
            match machine.step(outcome) {
                Step::Finish(output) => return output,
                Step::Run(op) => outcome = self.perform(op, email, budget).await,
            }
        }
    }

    async fn perform(&mut self, op: Op, email: &[u8], budget: TimeoutBudget) -> OpOutcome {
        match op {
            Op::Write(bytes) => op_done(self.write_with_budget(bytes.as_bytes(), budget).await),
            Op::WriteSecret(bytes) => {
                op_done(self.write_with_budget(bytes.as_bytes(), budget).await)
            }
            Op::ResolveToken(credentials) => match credentials.oauth2_token().await {
                Ok((_, token)) => OpOutcome::Token(token.into()),
                Err(error) => OpOutcome::Failed(error),
            },
            Op::UpgradeTls(parameters) => {
                let result =
                    match ReplyAssembler::check_surplus(!self.stream.buffer().is_empty(), true) {
                        Ok(()) => {
                            self.stream
                                .get_mut()
                                .upgrade_tls(parameters, self.timeout)
                                .await
                        }
                        Err(error) => {
                            self.stream.get_mut().set_state(ConnectionState::Broken);
                            Err(error)
                        }
                    };
                op_done(result)
            }
            Op::WriteBody => op_done(self.write_body(email).await),
            Op::WriteBdat => op_done(self.write_bdat_body(email).await),
            Op::OpenReplyGroup => op_done(self.open_reply_group()),
            Op::ReadGrouped => op_reply(
                self.read_response_with_budget_inner(budget, ReplyMode::Grouped)
                    .await,
            ),
            Op::ReadSingle => op_reply(
                self.read_response_with_budget_inner(budget, ReplyMode::AnyStatus)
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
        let mut machine = Session::starttls(
            self.protocol,
            hello_name,
            self.server_info.clone(),
            tls_parameters,
        );
        let (info, _) = self.drive(&mut machine, &[]).await?;
        self.server_info = info;
        self.hello_name = hello_name.clone();
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

    pub(crate) async fn auth(
        &mut self,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        let mut machine = Session::auth(
            self.protocol,
            &self.hello_name,
            self.server_info.clone(),
            mechanisms,
            credentials,
            self.peer_certificate_der(),
        );
        let (info, response) = self.drive(&mut machine, &[]).await?;
        self.server_info = info;
        response.ok_or_else(|| error::internal("AUTH completed without a response"))
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
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(crate) async fn message(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.message_iter(std::iter::once(message)).await
    }

    /// Sends the message content by consuming an iterator that in its whole represents a message.
    #[cfg(test)]
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
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
    #[cfg(test)]
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(crate) async fn read_response(&mut self) -> Result<Response, Error> {
        self.read_response_with_budget(self.per_operation_budget())
            .await
    }

    async fn read_response_accepting_status(&mut self) -> Result<Response, Error> {
        self.read_response_with_budget_inner(self.per_operation_budget(), ReplyMode::AnyStatus)
            .await
    }

    async fn read_response_with_budget(
        &mut self,
        budget: TimeoutBudget,
    ) -> Result<Response, Error> {
        self.read_response_with_budget_inner(budget, ReplyMode::Positive)
            .await
    }

    async fn read_response_with_budget_inner(
        &mut self,
        budget: TimeoutBudget,
        mode: ReplyMode,
    ) -> Result<Response, Error> {
        if mode.manages_state() {
            self.stream.get_ref().state().verify()?;
            self.stream.get_mut().set_state(ConnectionState::Broken);
        }

        // Every buffer fill spends the same remaining peer time. Admission is
        // separate so the consumer's cap cannot consume that time. Setup keeps
        // its absolute ceiling across both phases.
        let mut budget = ReplyBudget::new(budget);

        let mut assembler = ReplyAssembler::default();
        loop {
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
            let started = tokio::time::Instant::now();
            let (consumed, response) = {
                let bytes = with_timeout(
                    budget.budget(),
                    "SMTP read timed out",
                    self.stream.fill_buf(),
                )
                .await?
                .map_err(error::network)?;
                if needs_read {
                    budget.spend(started.elapsed());
                }
                assembler.feed(bytes)?
            };
            self.stream.consume(consumed);
            if let Some(response) = response {
                if mode.manages_state() {
                    ReplyAssembler::check_surplus(!self.stream.buffer().is_empty(), false)?;
                    self.stream.get_mut().set_state(ConnectionState::Ok);
                }
                return ReplyAssembler::complete(response, mode);
            }
        }
    }

    fn finish_reply_group(&mut self) -> Result<(), Error> {
        ReplyAssembler::check_surplus(!self.stream.buffer().is_empty(), false)?;
        self.stream.get_mut().set_state(ConnectionState::Ok);
        Ok(())
    }
}

#[cfg(all(test, feature = "tokio", feature = "account-error"))]
mod transcript_tests {
    use std::time::Duration;

    use crate::transport::smtp::{
        Dialect,
        account_error::{SmtpErrorContext, into_account_error},
        authentication::{Credentials, Mechanism},
        commands::Noop,
        extension::ClientId,
        test_support::Transcript,
    };

    use super::{
        AsyncDeadline, AsyncNetworkStream, AsyncSmtpConnection, TEARDOWN_TIMEOUT, TimeoutBudget,
    };

    const HELLO: &str = "EHLO client.example\r\n";

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
            Dialect::Smtp,
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
            Dialect::Smtp,
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
            Dialect::Smtp,
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

    // The tests below replace the socket-listener tests this harness retired.
    // Same behaviors, same assertions, no listener or thread.

    #[tokio::test(crate = "tokio")]
    async fn abort_closes_without_quit_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Dialect::Smtp)
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
        let connection = AsyncSmtpConnection::from_transcript(transcript, &hello, Dialect::Smtp)
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
            AsyncSmtpConnection::from_transcript(transcript, &hello, Dialect::Smtp)
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
            Dialect::Smtp,
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
            AsyncSmtpConnection::from_transcript(transcript, &hello, Dialect::Smtp)
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
            Dialect::Smtp,
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
            Dialect::Smtp,
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
                mechanism: Some(crate::transport::smtp::authentication::Mechanism::ScramSha256Plus)
            })
        );
        let account = into_account_error(error, SmtpErrorContext::send(Dialect::Smtp));
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
            Dialect::Smtp,
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
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Dialect::Smtp)
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
            AsyncSmtpConnection::from_transcript(transcript.clone(), &hello, Dialect::Smtp)
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

    #[tokio::test(crate = "tokio", start_paused = true)]
    async fn connect_times_out_waiting_for_banner() {
        let hello = ClientId::Domain("client.example".to_owned());
        // The peer accepts and then says nothing at all.
        let Err(error) = AsyncSmtpConnection::from_transcript_with_timeout(
            Transcript::silent(),
            &hello,
            Dialect::Smtp,
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
            Dialect::Smtp,
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
            Dialect::Smtp,
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
            Dialect::Smtp,
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
            Dialect::Smtp,
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
            Dialect::Smtp,
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
            Dialect::Smtp,
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
