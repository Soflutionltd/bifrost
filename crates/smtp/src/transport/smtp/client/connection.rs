#[cfg(feature = "account-error")]
use crate::transport::smtp::batch::{SendProgress, SmtpBatchRecipient};
#[cfg(unix)]
use std::path::Path;
use std::{
    fmt::{Display, Write as _},
    io::{self, BufRead, BufReader, Write},
    net::{IpAddr, ToSocketAddrs},
    time::Duration,
};

use zeroize::{Zeroize, Zeroizing};

use super::metering::WireMetering;
use super::{
    ClientCodec, ConnectionState, DEFAULT_TLS_HANDSHAKE_TIMEOUT, NetworkStream, TlsParameters,
    core,
    core::{Op, OpOutcome, ProtocolMachine, Step},
    data_terminator,
};
use super::{ReplyMode, reply::ReplyAssembler, session::Session};
#[cfg(feature = "account-error")]
use crate::address::Address;
use crate::{
    address::Envelope,
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

/// Structure that implements the SMTP client
pub(crate) struct SmtpConnection {
    /// TCP stream between client and server
    /// Value is None before connection
    stream: BufReader<NetworkStream>,
    /// Information about the server
    server_info: ServerInfo,
    /// Client identity used for EHLO.
    hello_name: ClientId,
    /// Wire protocol used for this connection.
    protocol: Dialect,
    /// Reused storage for serializing individual SMTP commands.
    command_buffer: Zeroizing<String>,
    /// Set once an LMTP final-status drain has run: the connection must not
    /// go back into the pool because stream cleanliness cannot be proven.
    retire: bool,
    /// The per-operation read timeout this connection was configured with.
    ///
    /// The socket option is per read SYSCALL, so a multi-line reply armed with
    /// it alone re-arms the full timeout for every line - a peer trickling one
    /// line per period stretches one reply to many times the configured
    /// timeout. `read_response_inner` starts a cumulative peer budget which
    /// the stream spends per syscall, re-arming the socket after admission.
    /// The configured value is kept here to restore it after each reply.
    read_timeout: Option<Duration>,
}

impl SmtpConnection {
    /// Get information about the server
    pub(crate) fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    /// Connects to the configured server
    ///
    /// Sends EHLO and parses server information
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn connect<A: ToSocketAddrs + Send + 'static>(
        server: A,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<&TlsParameters>,
        local_address: Option<IpAddr>,
    ) -> Result<SmtpConnection, Error> {
        Self::connect_with_protocol(
            server,
            timeout,
            hello_name,
            tls_parameters,
            local_address,
            Dialect::Smtp,
            WireMetering::disabled(),
        )
    }

    // Widened so the pool and transport tests can drive a scripted peer
    // through the public entry points instead of a socket.
    #[cfg(test)]
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(in crate::transport::smtp) fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Dialect,
    ) -> Result<Self, Error> {
        Self::from_scripted_peer(
            transcript,
            hello_name,
            protocol,
            None,
            WireMetering::disabled(),
        )
    }

    #[cfg(any(test, feature = "test-support"))]
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(in crate::transport::smtp) fn from_scripted_peer(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Dialect,
        timeout: Option<Duration>,
        metering: WireMetering,
    ) -> Result<Self, Error> {
        let mut network = NetworkStream::from_transcript(transcript);
        network.set_metering(metering);
        let stream = BufReader::new(network);
        let mut conn = Self {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
            read_timeout: None,
        };
        conn.set_timeout(timeout).map_err(error::connection_io)?;
        conn.initialize(hello_name)?;
        Ok(conn)
    }

    pub(crate) fn connect_with_protocol<A: ToSocketAddrs + Send + 'static>(
        server: A,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<&TlsParameters>,
        local_address: Option<IpAddr>,
        protocol: Dialect,
        metering: WireMetering,
    ) -> Result<SmtpConnection, Error> {
        let mut stream = NetworkStream::connect(server, timeout, tls_parameters, local_address)?;
        // Installed on the dialed stream rather than threaded into the
        // dialer: the TCP/TLS handshake is not the account's traffic.
        stream.set_metering(metering);
        let stream = BufReader::new(stream);
        let mut conn = SmtpConnection {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
            read_timeout: None,
        };
        conn.set_timeout(timeout).map_err(error::network)?;
        conn.initialize(hello_name)?;

        // Print server information
        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    #[cfg(unix)]
    pub(crate) fn connect_unix_with_protocol(
        path: &Path,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        protocol: Dialect,
        metering: WireMetering,
    ) -> Result<SmtpConnection, Error> {
        let mut stream = NetworkStream::connect_unix(path, timeout)?;
        stream.set_metering(metering);
        let stream = BufReader::new(stream);
        let mut conn = SmtpConnection {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
            read_timeout: None,
        };
        conn.set_timeout(timeout).map_err(error::network)?;
        conn.initialize(hello_name)?;

        #[cfg(feature = "tracing")]
        tracing::debug!("server {}", conn.server_info);
        Ok(conn)
    }

    pub(crate) fn send(&mut self, envelope: &Envelope, email: &[u8]) -> Result<Response, Error> {
        self.send_with_options(envelope, email, &SendOptions::default())
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

    pub(crate) fn send_with_options(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        let (mail, recipients) = self.build_envelope(envelope, email, options, false)?;
        let pipelined = self.server_info().supports_pipelining();
        let mut machine = core::DirectSmtp::new(mail, recipients, pipelined, core::BodyKind::Data);
        self.drive(&mut machine, email)
    }

    pub(crate) fn send_bdat_with_options(
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
        self.drive(&mut machine, email)
    }

    pub(crate) fn send_lmtp(
        &mut self,
        envelope: &Envelope,
        email: &[u8],
    ) -> Result<Vec<Response>, Error> {
        self.send_lmtp_with_options(envelope, email, &SendOptions::default())
    }

    pub(crate) fn send_lmtp_with_options(
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
        self.drive(&mut machine, email)
    }

    pub(crate) fn send_lmtp_bdat_with_options(
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
        self.drive(&mut machine, email)
    }

    /// Account-oriented SMTP multi-recipient send.
    ///
    /// Returns `Ok(progress)` when the command sequence completes (even if
    /// some recipients were rejected). Returns `Err((error, progress))` for
    /// batch-level failures where no recipient-specific outcome can be
    /// attributed: MAIL FROM rejection, transport drop before any command, or
    /// pre-MAIL local validation errors. In the `Err` case the tracker holds
    /// whatever state was accumulated before the abort.
    #[cfg(feature = "account-error")]
    pub(crate) fn send_smtp_batch(
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
        self.drive(&mut machine, email)
    }

    /// Account-oriented LMTP multi-recipient send.
    ///
    /// Like `send_smtp_batch` but reads one final status per accepted
    /// recipient after the DATA body.
    #[cfg(feature = "account-error")]
    pub(crate) fn send_lmtp_batch(
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
        self.drive(&mut machine, email)
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
    /// This loop is the whole blocking adapter: it moves bytes and reports
    /// what happened. Which command follows which, how many replies a window
    /// owes, when a transaction is reset versus abandoned, and which phase
    /// decorates a failure are all decided in `client::core`, so neither half
    /// of this crate can drift from the other.
    fn drive<M: ProtocolMachine>(&mut self, machine: &mut M, email: &[u8]) -> M::Output {
        let mut outcome = OpOutcome::Done;
        loop {
            match machine.step(outcome) {
                Step::Finish(output) => return output,
                Step::Run(op) => outcome = self.perform(op, email),
            }
        }
    }

    fn perform(&mut self, op: Op, email: &[u8]) -> OpOutcome {
        match op {
            Op::Write(bytes) => op_done(self.write(bytes.as_bytes())),
            Op::WriteSecret(bytes) => op_done(self.write(bytes.as_bytes())),
            Op::ResolveToken(credentials) => match credentials.oauth2_token_blocking() {
                Ok((_, token)) => OpOutcome::Token(token.into()),
                Err(error) => OpOutcome::Failed(error),
            },
            Op::UpgradeTls(parameters) => {
                let result =
                    match ReplyAssembler::check_surplus(!self.stream.buffer().is_empty(), true) {
                        Ok(()) => self.upgrade_tls_bounded(&parameters),
                        Err(error) => {
                            self.stream.get_mut().set_state(ConnectionState::Broken);
                            Err(error)
                        }
                    };
                op_done(result)
            }
            Op::WriteBody => op_done(self.write_body(email)),
            Op::WriteBdat => op_done(self.write_bdat_body(email)),
            Op::OpenReplyGroup => op_done(self.open_reply_group()),
            Op::ReadGrouped => op_reply(self.read_response_inner(ReplyMode::Grouped)),
            Op::ReadSingle => op_reply(self.read_response_inner(ReplyMode::AnyStatus)),
            Op::CloseReplyGroup => op_done(self.finish_reply_group()),
            Op::CloseLmtpDrain { restore_ok } => {
                let result = self.finish_lmtp_final_drain();
                if result.is_ok() && restore_ok {
                    self.stream.get_mut().set_state(ConnectionState::Ok);
                }
                op_done(result)
            }
            Op::Abort => {
                self.abort();
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
    fn write_bdat_body(&mut self, email: &[u8]) -> Result<(), Error> {
        self.write(core::bdat_header(email.len()).as_bytes())?;
        self.write(email)
    }

    /// Write the DATA body without reading the final reply.
    fn write_body(&mut self, email: &[u8]) -> Result<(), Error> {
        self.write_body_iter(std::iter::once(email))
    }

    pub(crate) fn has_broken(&self) -> bool {
        self.stream.get_ref().state() != ConnectionState::Ok
    }

    pub(crate) fn can_starttls(&self) -> bool {
        !self.is_encrypted() && self.server_info.supports_feature(Extension::StartTls)
    }

    pub(crate) fn starttls(
        &mut self,
        tls_parameters: &TlsParameters,
        hello_name: &ClientId,
    ) -> Result<(), Error> {
        let mut machine = Session::starttls(
            self.protocol,
            hello_name,
            self.server_info.clone(),
            tls_parameters.clone(),
        );
        let (info, _) = self.drive(&mut machine, &[])?;
        self.server_info = info;
        self.hello_name = hello_name.clone();
        Ok(())
    }

    /// The STARTTLS handshake, never unbounded.
    ///
    /// The handshake blocks on the socket, so its only bound is the socket's
    /// own `SO_RCVTIMEO` / `SO_SNDTIMEO`. With a configured timeout those are
    /// already armed by `set_timeout`, and the handshake runs under them as it
    /// stands. With none, they are unset and a peer that never answers the
    /// `ClientHello` would block the thread forever, so the default is armed for
    /// the handshake alone and the unbounded setting restored afterwards.
    /// This is a per-read bound, like every other blocking timeout here, not a
    /// shared deadline (see "Transport types" in `reference/smtp.md`).
    fn upgrade_tls_bounded(&mut self, tls_parameters: &TlsParameters) -> Result<(), Error> {
        if self.read_timeout.is_some() {
            return self.stream.get_mut().upgrade_tls(tls_parameters);
        }

        self.set_timeout(Some(DEFAULT_TLS_HANDSHAKE_TIMEOUT))
            .map_err(error::network)?;
        let result = self.stream.get_mut().upgrade_tls(tls_parameters);
        // Restore even after a failure. A failed handshake has usually taken the
        // socket with it, in which case this fails too and there is nothing to
        // restore; the connection is broken either way.
        let restored = self.set_timeout(None);
        result?;
        restored.map_err(error::network)
    }

    fn initialize(&mut self, hello_name: &ClientId) -> Result<(), Error> {
        let mut machine = Session::greeting(self.protocol, hello_name);
        let (info, _) = self.drive(&mut machine, &[])?;
        self.server_info = info;
        Ok(())
    }

    /// Close the connection.
    ///
    /// Unlike the async half this needs no timeout: `Shutdown::Both` on a
    /// blocking socket is a syscall that returns immediately, where the async
    /// half's `poll_shutdown` sends TLS `close_notify` and waits for the
    /// peer's. The state transition is mirrored on both sides so an aborted
    /// connection can never pass `state().verify()` again.
    ///
    /// This is the ONE deliberate asymmetry between the blocking and async
    /// transport halves, which are otherwise held in step on purpose - a fix
    /// landing in one half only has been this crate's recurring defect. An
    /// audit finding "the async abort honours the operation timeout and this
    /// one does not" has found the intended state; there is nothing here to
    /// bound.
    pub(crate) fn abort(&mut self) {
        self.stream.get_mut().set_state(ConnectionState::Broken);
        let _ = self.stream.get_mut().shutdown(std::net::Shutdown::Both);
    }

    /// Tells if the underlying stream is currently encrypted
    pub(crate) fn is_encrypted(&self) -> bool {
        self.stream.get_ref().is_encrypted()
    }

    /// DER of the peer (server) certificate. See
    /// [`NetworkStream::peer_certificate_der`] for the contract.
    ///
    /// Consumed by `auth`'s `resolve_scram_binding` for SCRAM-PLUS channel
    /// binding.
    pub(crate) fn peer_certificate_der(&self) -> Option<Vec<u8>> {
        self.stream.get_ref().peer_certificate_der()
    }

    /// Set timeout
    pub(crate) fn set_timeout(&mut self, duration: Option<Duration>) -> io::Result<()> {
        self.read_timeout = duration;
        self.stream.get_mut().set_read_timeout(duration)?;
        self.stream.get_mut().set_write_timeout(duration)
    }

    /// Checks if the server is connected using the NOOP SMTP command.
    ///
    /// A failed check marks the connection broken and closes it. A connection
    /// that cannot answer NOOP is not safe to keep in the pool.
    pub(crate) fn test_connected(&mut self) -> bool {
        match self.command(Noop) {
            Ok(_) => true,
            Err(_) => {
                self.abort();
                false
            }
        }
    }

    /// Sends a VRFY command and returns the server response.
    pub(crate) fn verify(&mut self, argument: impl Into<String>) -> Result<Response, Error> {
        self.command_accepting_status(Vrfy::new(argument.into())?)
    }

    /// Sends an EXPN command and returns the server response.
    pub(crate) fn expand(&mut self, argument: impl Into<String>) -> Result<Response, Error> {
        self.command_accepting_status(Expn::new(argument.into())?)
    }

    pub(crate) fn auth(
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
        let (info, response) = self.drive(&mut machine, &[])?;
        self.server_info = info;
        response.ok_or_else(|| error::internal("AUTH completed without a response"))
    }

    /// Write the DATA body and its terminator.
    ///
    /// Consumes an iterator that in its whole represents the message: the
    /// last-two-bytes tracking is what recognizes a final CRLF split across
    /// two items, so the terminator reuses the message's own final CRLF rather
    /// than appending an empty line the sender never wrote.
    fn write_body_iter<I, B>(&mut self, message: I) -> Result<(), Error>
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
            self.write(out_buf.as_slice())?;
        }
        self.write(data_terminator(seen >= 2 && last_two == *b"\r\n"))
    }

    /// Sends the message content.
    ///
    /// Test-only since the send paths were factored onto `client::core`: they
    /// write the body through `Op::WriteBody` and read the final reply through
    /// `Op::ReadSingle`, over these same writers.
    #[cfg(test)]
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(crate) fn message(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.message_iter(std::iter::once(message))
    }

    /// Sends the message content by consuming an iterator that in its whole represents a message.
    #[cfg(test)]
    #[cfg_attr(not(feature = "account-error"), allow(dead_code))]
    pub(crate) fn message_iter<I, B>(&mut self, message: I) -> Result<Response, Error>
    where
        I: Iterator<Item = B>,
        B: AsRef<[u8]>,
    {
        self.write_body_iter(message)?;
        self.read_response()
    }

    /// Sends an SMTP command
    pub(crate) fn command<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.write_command(command)?;
        self.read_response()
    }

    fn command_accepting_status<C: Display>(&mut self, command: C) -> Result<Response, Error> {
        self.write_command(command)?;
        self.read_response_accepting_status()
    }

    fn write_command<C: Display>(&mut self, command: C) -> Result<(), Error> {
        self.command_buffer.zeroize();
        write!(&mut self.command_buffer, "{command}")
            .map_err(|_| error::internal("failed to serialize SMTP command"))?;
        let result = Self::write_stream(&mut self.stream, self.command_buffer.as_bytes());
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
    fn write(&mut self, string: &[u8]) -> Result<(), Error> {
        Self::write_stream(&mut self.stream, string)
    }

    fn write_stream(stream: &mut BufReader<NetworkStream>, string: &[u8]) -> Result<(), Error> {
        stream.get_ref().state().verify()?;
        stream.get_mut().set_state(ConnectionState::Broken);

        stream.get_mut().write_all(string).map_err(error::network)?;
        stream.get_mut().flush().map_err(error::network)?;
        stream.get_mut().set_state(ConnectionState::Ok);

        #[cfg(feature = "tracing")]
        tracing::debug!("Wrote {} bytes", string.len());
        Ok(())
    }

    /// Gets the SMTP response
    pub(crate) fn read_response(&mut self) -> Result<Response, Error> {
        self.read_response_inner(ReplyMode::Positive)
    }

    fn read_response_accepting_status(&mut self) -> Result<Response, Error> {
        self.read_response_inner(ReplyMode::AnyStatus)
    }

    fn read_response_inner(&mut self, mode: ReplyMode) -> Result<Response, Error> {
        let configured = self.read_timeout;
        let result = self.read_response_until(mode, configured);
        if configured.is_some() {
            // Restore the full per-operation timeout for the next reply,
            // whichever way this one ended.
            let _ = self.stream.get_mut().set_read_timeout(configured);
        }
        result
    }

    /// One reply, bounded as a whole rather than per line. See the
    /// `read_timeout` field for why the deadline lives here.
    fn read_response_until(
        &mut self,
        mode: ReplyMode,
        configured: Option<Duration>,
    ) -> Result<Response, Error> {
        self.stream.get_mut().begin_reply(configured);

        if mode.manages_state() {
            self.stream.get_ref().state().verify()?;
            self.stream.get_mut().set_state(ConnectionState::Broken);
        }

        let mut assembler = ReplyAssembler::default();
        loop {
            let (consumed, response) =
                assembler.feed(self.stream.fill_buf().map_err(error::network)?)?;
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

#[cfg(all(test, feature = "account-error"))]
mod transcript_tests {
    use crate::transport::smtp::{Dialect, extension::ClientId, test_support::Transcript};

    use super::{DEFAULT_TLS_HANDSHAKE_TIMEOUT, SmtpConnection};

    const HELLO: &str = "EHLO client.example\r\n";

    /// Finding 7: the configured timeout bounds the REPLY, not each line of
    /// it. The socket option is per read syscall, so the driver must re-arm it
    /// with the slack left on a per-reply deadline; otherwise a peer trickling
    /// one line per timeout period stretches one reply to many times the
    /// configured timeout. The transcript records every armed value, so the
    /// per-line arm (a constant) and the per-reply arm (strictly decreasing
    /// within a reply, reset between replies) are distinguishable.
    #[test]
    fn multi_line_reply_reads_share_one_deadline() {
        use std::time::Duration;

        use crate::transport::smtp::commands::Noop;

        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect(
                "NOOP\r\n",
                "250-first\r\n250-second\r\n250-third\r\n250 fourth\r\n",
            );
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Dialect::Smtp).unwrap();
        connection
            .set_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        transcript.take_read_timeouts();

        connection.command(Noop).unwrap();

        // One arm per line read, then the restore. Every arm inside the reply
        // must be strictly shorter than the previous one: they all draw from
        // the same deadline. A per-line re-arm would repeat 30s verbatim.
        let armed = transcript.take_read_timeouts();
        let restore = armed.last().copied().flatten();
        assert_eq!(
            restore,
            Some(Duration::from_secs(30)),
            "the full per-operation timeout must be restored after the reply"
        );
        let within: Vec<Duration> = armed[..armed.len() - 1]
            .iter()
            .map(|armed| armed.expect("a bounded read must arm a bounded timeout"))
            .collect();
        assert!(
            within.len() >= 2,
            "the multi-line reply must have taken more than one read: {within:?}"
        );
        assert!(
            within.windows(2).all(|pair| pair[1] <= pair[0]),
            "each line must draw from the reply's remaining slack: {within:?}"
        );
        assert!(
            within[within.len() - 1] < within[0],
            "the slack must actually shrink across the reply, not re-arm: {within:?}"
        );
        assert!(
            within.iter().all(|armed| *armed <= Duration::from_secs(30)),
            "no line may be armed with more than the configured timeout: {within:?}"
        );
    }

    // The tests below replace the socket-listener tests this harness retired.
    // Same behaviors, same assertions, no listener or thread.

    #[test]
    fn abort_closes_without_quit_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        // No step after EHLO: any byte `abort` writes is a transcript failure.
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Dialect::Smtp).unwrap();

        connection.abort();
        transcript.assert_exhausted();
    }

    #[test]
    fn peer_certificate_der_is_none_on_plaintext() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let connection =
            SmtpConnection::from_transcript(transcript, &hello, Dialect::Smtp).unwrap();

        assert!(
            connection.peer_certificate_der().is_none(),
            "plaintext connection must have no peer certificate DER"
        );
    }

    #[test]
    fn failed_test_connected_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        // The NOOP probe is scripted, but the peer never answers it.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "");
        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Dialect::Smtp).unwrap();

        assert!(!connection.test_connected());
        assert!(connection.has_broken());
    }

    /// Reads back what the blocking driver armed on the socket around the
    /// handshake. The transcript stream cannot complete a handshake, but the
    /// timeouts are set before it is attempted and restored after it fails, so
    /// they are observable without a socket.
    fn starttls_armed_timeouts(
        configured: Option<std::time::Duration>,
    ) -> Vec<Option<std::time::Duration>> {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "220 ready to start tls\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Dialect::Smtp).unwrap();
        connection.set_timeout(configured).unwrap();
        transcript.take_read_timeouts();

        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        connection
            .starttls(&tls, &hello)
            .expect_err("the in-process transcript stream cannot complete a TLS handshake");
        transcript.take_read_timeouts()
    }

    /// With no timeout configured the handshake would block the thread forever
    /// on a peer that never answers, so the default is armed for the handshake
    /// and the unbounded setting is put back afterwards.
    #[test]
    fn an_unconfigured_starttls_handshake_is_bounded_and_then_unbounded_again() {
        let armed = starttls_armed_timeouts(None);
        assert_eq!(
            armed.first(),
            Some(&Some(DEFAULT_TLS_HANDSHAKE_TIMEOUT)),
            "the default must be armed before the handshake: {armed:?}"
        );
        assert_eq!(
            armed.last(),
            Some(&None),
            "the unbounded setting must be restored after it: {armed:?}"
        );
    }

    /// A configured timeout already governs the handshake through the armed
    /// socket, so the default must not be layered over it.
    #[test]
    fn a_configured_starttls_timeout_is_left_alone() {
        let configured = std::time::Duration::from_secs(7);
        let armed = starttls_armed_timeouts(Some(configured));
        assert!(
            !armed.contains(&Some(DEFAULT_TLS_HANDSHAKE_TIMEOUT)),
            "the default replaced a configured timeout: {armed:?}"
        );
        // The reply read shrinks the armed value as its per-reply deadline
        // runs down, so "at most the configured timeout" is the invariant.
        assert!(
            armed
                .iter()
                .all(|timeout| timeout.is_some_and(|armed| armed <= configured)),
            "nothing beyond the configured timeout may be armed: {armed:?}"
        );
    }
}
