#[cfg(unix)]
use std::path::Path;
use std::{
    fmt::{Display, Write as _},
    io::{self, BufRead, BufReader, Read, Write},
    net::{IpAddr, ToSocketAddrs},
    time::Duration,
};

use bifrost_sasl::ScramChannelBinding;
use zeroize::{Zeroize, Zeroizing};

#[cfg(feature = "tracing")]
use super::escape_crlf;
use super::metering::WireMetering;
use super::{
    ClientCodec, ConnectionState, DEFAULT_TLS_HANDSHAKE_TIMEOUT, MAX_RESPONSE_BYTES,
    MAX_RESPONSE_LINE_BYTES, NetworkStream, TlsParameters, core,
    core::{Op, OpOutcome, ProtocolMachine, Step},
    data_terminator,
};
use crate::{
    address::{Address, Envelope},
    transport::smtp::{
        Protocol,
        authentication::{
            Credentials, Mechanism, ScramExchange, ScramStep, decode_auth_challenge,
            decode_scram_payload, oauth_mechanism, password_mechanism, resolve_scram_binding,
            scram_hash,
        },
        batch::{SendProgress, SmtpBatchRecipient},
        commands::{Auth, Ehlo, Expn, Lhlo, Mail, Noop, Rcpt, Starttls, Vrfy},
        error,
        error::{Error, SmtpCommandPhase},
        extension::{ClientId, Extension, SendOptions, ServerInfo},
        response::{Response, parse_response},
    },
};

macro_rules! try_smtp (
    ($err: expr, $client: ident) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort();
                return Err(From::from(err))
            },
        }
    });
    // Phase-tagged variant. Stamps the SMTP error with the given
    // SmtpCommandPhase so the translation boundary in `account_error.rs`
    // can route per-phase (e.g. AUTH-time failures -> PolicyBlocked,
    // MAIL FROM phase tagged on telemetry, etc.). Use this in the
    // legacy non-batch send paths and the AUTH command exchange.
    // Set-if-absent (`or_phase`): a multi-phase callee (the LMTP body
    // upload + final-status drain) stamps its own finer-grained phase
    // inside, and this wrapper must not overwrite it.
    ($err: expr, $client: ident, $phase: expr) => ({
        match $err {
            Ok(val) => val,
            Err(err) => {
                $client.abort();
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
    protocol: Protocol,
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
            Protocol::Smtp,
            WireMetering::disabled(),
        )
    }

    // Widened so the pool and transport tests can drive a scripted peer
    // through the public entry points instead of a socket.
    #[cfg(test)]
    pub(in crate::transport::smtp) fn from_transcript(
        transcript: crate::transport::smtp::test_support::Transcript,
        hello_name: &ClientId,
        protocol: Protocol,
    ) -> Result<Self, Error> {
        let stream = BufReader::new(NetworkStream::from_transcript(transcript));
        let mut conn = Self {
            stream,
            server_info: ServerInfo::default(),
            hello_name: hello_name.clone(),
            protocol,
            command_buffer: Zeroizing::new(String::new()),
            retire: false,
            read_timeout: None,
        };
        let _response = conn.read_response()?;
        conn.hello(hello_name)?;
        Ok(conn)
    }

    pub(crate) fn connect_with_protocol<A: ToSocketAddrs + Send + 'static>(
        server: A,
        timeout: Option<Duration>,
        hello_name: &ClientId,
        tls_parameters: Option<&TlsParameters>,
        local_address: Option<IpAddr>,
        protocol: Protocol,
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
        let _response = conn.read_response()?;

        conn.hello(hello_name)?;

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
        protocol: Protocol,
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
        let _response = conn.read_response()?;

        conn.hello(hello_name)?;

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
        // BDAT is never pipelined: the path has no window accounting and has
        // never been driven that way, on either half.
        let mut machine = core::DirectSmtp::new(mail, recipients, false, core::BodyKind::Bdat);
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
        let mut machine = core::DirectLmtp::new(mail, recipients, core::BodyKind::Data);
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
        let mut machine = core::DirectLmtp::new(mail, recipients, core::BodyKind::Bdat);
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
    pub(crate) fn send_smtp_batch(
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
        self.drive(&mut machine, email)
    }

    /// Account-oriented LMTP multi-recipient send.
    ///
    /// Like `send_smtp_batch` but reads one final status per accepted
    /// recipient after the DATA body.
    pub(crate) fn send_lmtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        let (mail_cmd, rcpt_cmds, progress) =
            self.build_batch(Protocol::Lmtp, from, recipients, email, options)?;
        let mut machine = core::BatchLmtp::new(mail_cmd, rcpt_cmds, progress);
        self.drive(&mut machine, email)
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
            Op::WriteBody => op_done(self.write_body(email)),
            Op::WriteBdat => op_done(self.write_bdat_body(email)),
            Op::OpenReplyGroup => op_done(self.open_reply_group()),
            Op::ReadGrouped => op_reply(self.read_response_inner(true, false)),
            Op::ReadSingle => op_reply(self.read_response_inner(true, true)),
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
        if self.server_info.supports_feature(Extension::StartTls) {
            try_smtp!(self.command(Starttls), self);
            // Belt and braces at the security boundary. The generic
            // surplus-bytes check inside the reply read normally fires first,
            // but `get_mut()` swaps only the INNER stream and leaves the
            // `BufReader` buffer intact, so any pre-TLS byte still sitting here
            // would be consumed as the first bytes of the TLS session - the
            // classic STARTTLS response-injection. This gate is independent of
            // whichever reply path produced the 220.
            if !self.stream.buffer().is_empty() {
                self.stream.get_mut().set_state(ConnectionState::Broken);
                self.abort();
                return Err(error::parse(
                    "SMTP server sent an unsolicited reply before the STARTTLS upgrade",
                ));
            }
            self.upgrade_tls_bounded(tls_parameters)?;
            #[cfg(feature = "tracing")]
            tracing::debug!("connection encrypted");
            // Send EHLO/LHLO again
            try_smtp!(self.hello(hello_name), self);
            self.hello_name = hello_name.clone();
            Ok(())
        } else {
            // A client policy refusal, not caller input: the configuration
            // demanded TLS and this server cannot provide it. Same class as
            // IMAP's `StartTlsUnavailable`, `Authorization(PolicyBlocked)`.
            Err(error::policy("STARTTLS is not supported on this server"))
        }
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

    /// Send EHLO or LHLO and update server info
    fn hello(&mut self, hello_name: &ClientId) -> Result<(), Error> {
        let response = match self.protocol {
            Protocol::Lmtp => {
                let command = Lhlo::new(hello_name.clone())?;
                try_smtp!(self.command(command), self)
            }
            _ => {
                let command = Ehlo::new(hello_name.clone())?;
                try_smtp!(self.command(command), self)
            }
        };
        self.server_info = try_smtp!(ServerInfo::from_response(&response), self);
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

    /// Sends an AUTH command, selecting the strongest compatible mechanism.
    ///
    /// [`password_mechanism`] is SCRAM-aware, applies RFC 5802 Section 6
    /// downgrade protection against the advertised set, and skips a PLUS rung
    /// whose channel binding cannot resolve, falling through to the next safe
    /// rung (the other PLUS hash, then PLAIN). Only binding-unavailability
    /// falls through; a wire-level rejection `?`-propagates and is final, since
    /// walking down to an unbound mechanism after a 535 is the silent downgrade
    /// RFC 5802 Section 6 exists to prevent.
    ///
    /// `tls-server-end-point` is a property of the certificate, not of the
    /// SCRAM hash, so the binding is resolved at most once per authentication
    /// and shared by either PLUS choice.
    pub(crate) fn auth(
        &mut self,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        // OAuth credentials never use SCRAM: select the first advertised
        // mechanism from the caller's order and run the legacy encoder path.
        if let Some(mechanism) = oauth_mechanism(mechanisms, &self.server_info, credentials) {
            return self.auth_legacy(mechanism, credentials);
        }

        let scram_candidate = mechanisms.iter().any(|mechanism| {
            matches!(
                mechanism,
                Mechanism::ScramSha1Plus
                    | Mechanism::ScramSha256Plus
                    | Mechanism::ScramSha1
                    | Mechanism::ScramSha256
            ) && self.server_info.supports_auth_mechanism(*mechanism)
        });
        let binding = if scram_candidate {
            self.resolve_scram_binding()?
        } else {
            None
        };
        let chosen = password_mechanism(mechanisms, &self.server_info, binding.is_some())?;
        match chosen {
            Mechanism::ScramSha1Plus | Mechanism::ScramSha256Plus => {
                let binding = binding.expect("PLUS binding resolved above");
                self.auth_scram(chosen, binding, credentials)
            }
            Mechanism::ScramSha1 | Mechanism::ScramSha256 => {
                self.auth_scram(
                    chosen,
                    {
                        // Preserve the shared GS2 downgrade signal when PLUS was not advertised.
                        let hash = if chosen == Mechanism::ScramSha1 {
                            bifrost_sasl::ScramHash::Sha1
                        } else {
                            bifrost_sasl::ScramHash::Sha256
                        };
                        let data = match &binding {
                            Some(ScramChannelBinding::TlsServerEndPoint(data)) => {
                                Some(data.as_slice())
                            }
                            _ => None,
                        };
                        bifrost_sasl::select_scram_rung(
                            hash,
                            false,
                            |name| {
                                mechanisms.iter().any(|candidate| {
                                    candidate.name() == name
                                        && self.server_info.supports_auth_mechanism(*candidate)
                                })
                            },
                            data,
                        )
                        .expect("selected SCRAM rung remains advertised")
                        .1
                    },
                    credentials,
                )
            }
            _ => self.auth_legacy(chosen, credentials),
        }
    }

    /// Resolve the `tls-server-end-point` channel binding for the live
    /// connection: fetch the peer certificate DER (already cached on the TLS
    /// stream, no wire I/O) and hash it. Returns `Ok(None)` only when the DER
    /// is absent (plaintext). If a certificate is present but
    /// [`bifrost_sasl::tls_server_end_point`] cannot process it (for example,
    /// an `EdDSA` leaf, malformed DER, or an unsupported signature algorithm),
    /// its typed error propagates and cannot silently downgrade authentication.
    fn resolve_scram_binding(&self) -> Result<Option<ScramChannelBinding>, Error> {
        let der = self.peer_certificate_der();
        resolve_scram_binding(der.as_deref())
    }

    /// Run a SCRAM exchange (bound or unbound) as a no-IR `334` challenge walk.
    fn auth_scram(
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

        // Bare `AUTH <mechanism>`: SCRAM has no initial response, so
        // `Mechanism::response` is never invoked and `Display` emits the bare
        // command.
        let auth = Auth::new(mechanism, credentials.clone(), None)?;
        let response = try_smtp!(self.command(auth), self, phase);
        // Server's first 334 (empty challenge): send client-first.
        if !response.has_code(334) {
            self.abort();
            return Err(error::status(response).with_phase(phase));
        }
        let client_first = exchange.client_first();
        let response = try_smtp!(
            self.write_auth_continuation(client_first.as_str()),
            self,
            phase
        );

        // 334 carrying server-first -> client-final. A malformed continuation
        // is a protocol-class parse error (not an auth failure), so it is not
        // Auth-phase tagged, but it must still abort the connection like every
        // other error path in this exchange.
        let server_first = try_smtp!(decode_auth_challenge(&response), self);
        let response = match try_smtp!(exchange.step(&server_first), self, phase) {
            ScramStep::Reply(client_final) => {
                try_smtp!(
                    self.write_auth_continuation(client_final.as_str()),
                    self,
                    phase
                )
            }
            ScramStep::Complete => {
                self.abort();
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
            let final_reply = try_smtp!(self.write_auth_continuation(""), self, phase);
            if !final_reply.is_positive() {
                self.abort();
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
            self.abort();
            return Err(error::status(response).with_phase(phase));
        };

        let hello_name = self.hello_name.clone();
        try_smtp!(self.hello(&hello_name), self);
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
    fn write_auth_continuation(&mut self, line: &str) -> Result<Response, Error> {
        let framed = Zeroizing::new(format!("{line}\r\n"));
        self.write(framed.as_bytes())?;
        self.read_response()
    }

    /// The legacy stateless `Auth::new` / `Auth::new_from_response` 334 loop,
    /// for PLAIN / LOGIN / XOAUTH2 / OAUTHBEARER.
    fn auth_legacy(
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
        // The blocking transport has no async context, so it resolves the
        // OAuth token by polling the source once (see
        // `oauth2_token_blocking`): a `StaticTokenSource` or an
        // already-fresh `OAuthRefresher` resolves immediately; a source
        // needing a network refresh is rejected with a clear error.
        let oauth_token = if matches!(mechanism, Mechanism::Xoauth2 | Mechanism::OAuthBearer) {
            Some(try_smtp!(credentials.oauth2_token_blocking(), self, phase))
        } else {
            None
        };
        let oauth_token = oauth_token.as_ref().map(|(_, token)| token.as_str());

        // Limit challenges to avoid blocking
        let mut challenges = 10;
        // Position of the challenge within the exchange. LOGIN answers by
        // position (username, then password) rather than by prompt text, so
        // this counter is the only thing that selects which credential goes
        // out; it must not be derived from the remaining-challenge budget.
        let mut challenge_index: usize = 0;
        let auth = Auth::new(mechanism, credentials.clone(), oauth_token)?;
        let mut response = try_smtp!(self.command(auth), self, phase);

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
            response = try_smtp!(self.command(continuation), self, phase);
        }

        if challenges == 0 && response.has_code(334) {
            // The server never completed (or rejected) the exchange within the
            // round-trip budget: an auth-exchange failure, routed on the Auth
            // lane (InvalidInput + Auth -> Authorization), not an untagged
            // Protocol(ParseFailed). Exhausting the budget while sending the
            // tenth answer is fine when that answer receives a final reply.
            self.abort();
            Err(error::invalid_input("Unexpected number of challenges").with_phase(phase))
        } else {
            let hello_name = self.hello_name.clone();
            try_smtp!(self.hello(&hello_name), self);
            Ok(response)
        }
    }

    // NB: SCRAM continuation decoding lives in the free `decode_auth_challenge`
    // helper below so the sync and async drivers share one base64/UTF-8 path.

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
    pub(crate) fn message(&mut self, message: &[u8]) -> Result<Response, Error> {
        self.message_iter(std::iter::once(message))
    }

    /// Sends the message content by consuming an iterator that in its whole represents a message.
    #[cfg(test)]
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
        self.read_response_inner(false, true)
    }

    fn read_response_accepting_status(&mut self) -> Result<Response, Error> {
        self.read_response_inner(true, true)
    }

    fn read_response_inner(
        &mut self,
        accept_negative: bool,
        manage_state: bool,
    ) -> Result<Response, Error> {
        let configured = self.read_timeout;
        let result = self.read_response_until(accept_negative, manage_state, configured);
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
        accept_negative: bool,
        manage_state: bool,
        configured: Option<Duration>,
    ) -> Result<Response, Error> {
        self.stream.get_mut().begin_reply(configured);

        if manage_state {
            self.stream.get_ref().state().verify()?;
            self.stream.get_mut().set_state(ConnectionState::Broken);
        }

        let mut buffer = String::with_capacity(100);

        loop {
            let mut line = Vec::with_capacity(100);
            let bytes_read = {
                let mut limited = (&mut self.stream).take((MAX_RESPONSE_LINE_BYTES + 1) as u64);
                limited
                    .read_until(b'\n', &mut line)
                    .map_err(error::network)?
            };
            if bytes_read == 0 {
                break;
            }
            if line.len() > MAX_RESPONSE_LINE_BYTES {
                return Err(error::parse("SMTP response line too long"));
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

#[cfg(test)]
mod transcript_tests {
    use crate::{
        address::{Address, Envelope},
        transport::smtp::{
            Protocol,
            account_error::{SmtpErrorContext, into_account_error},
            authentication::{Credentials, Mechanism},
            batch::SmtpBatchRecipient,
            error::{SmtpCommandPhase, SmtpTransmissionState},
            extension::{
                ClientId, DeliverByMode, DsnNotify, DsnReturn, Extension, MailBodyParameter,
                MailParameter,
            },
            test_support::{Transcript, scram_enhanced_success_reply, scram_server_first_reply},
        },
    };
    use bifrost_types::error::BatchItemId;

    use super::{DEFAULT_TLS_HANDSHAKE_TIMEOUT, SendOptions, SmtpConnection};

    const HELLO: &str = "EHLO client.example\r\n";

    #[test]
    fn batch_mail_construction_failure_is_stamped_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
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

    /// The blocking mirror of the async oversized-banner pin (finding 4c): a
    /// greeting line past `MAX_RESPONSE_LINE_BYTES` is a parse error, and the
    /// reader returns it instead of reading on for a terminator that a
    /// pathological peer may never send.
    #[test]
    fn an_oversized_greeting_line_is_a_parse_error_not_a_hang() {
        // A WELL-FORMED greeting that is merely too long. A line of garbage
        // would fail to parse whether or not the cap exists, so it pins
        // nothing about the cap; this one parses fine once the cap is removed.
        let mut banner = format!("220 {}", "x".repeat(4096));
        banner.push_str("\r\n");
        let transcript = Transcript::new(&banner);

        let error = SmtpConnection::from_transcript(
            transcript,
            &ClientId::Domain("client.example".to_owned()),
            Protocol::Smtp,
        )
        .err()
        .expect("an oversized banner line must surface as an error");

        assert!(error.is_parse(), "expected a parse error, got {error:?}");
    }

    #[test]
    fn all_recipient_rejection_resets_every_direct_and_batch_transaction() {
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
                SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp)
                    .unwrap();
            let statuses = if chunking {
                connection
                    .send_lmtp_bdat_with_options(&envelope, b"body", &Default::default())
                    .unwrap()
            } else {
                connection.send_lmtp(&envelope, b"body").unwrap()
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
                SmtpConnection::from_transcript(transcript.clone(), &hello, protocol).unwrap();
            let progress = if protocol == Protocol::Smtp {
                connection.send_smtp_batch(
                    Some("sender@example.com".parse().unwrap()),
                    batch,
                    b"body",
                    &Default::default(),
                )
            } else {
                connection.send_lmtp_batch(
                    Some("sender@example.com".parse().unwrap()),
                    batch,
                    b"body",
                    &Default::default(),
                )
            }
            .unwrap();
            assert_eq!(progress.resolve().failed().len(), 1);
            assert!(!connection.has_broken());
            transcript.assert_exhausted();
        }
    }

    #[test]
    fn pipelined_server_rejections_carry_their_command_phase() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::MailFrom));

        // RCPT TO rejected.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::RcptTo));

        // DATA rejected before the body.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n250 recipient ok\r\n")
            .expect("DATA\r\n", "554 no data\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataCommand));
    }

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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
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

    /// Finding 2: without PIPELINING, a routine negative reply must be handled
    /// exactly as the pipelined path handles it - RSET (or nothing, for a
    /// rejected MAIL FROM) and a reusable connection - not `abort()`.
    #[test]
    fn unpipelined_server_rejections_keep_the_connection_reusable() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let error = connection
            .send_bdat_with_options(&envelope, b"body", &SendOptions::default())
            .unwrap_err();
        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::RcptTo));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn pipelined_body_failure_carries_data_body_phase() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();

        let error = connection.send(&envelope, b"body").unwrap_err();

        assert_eq!(error.phase(), Some(super::SmtpCommandPhase::DataBody));
    }

    fn recipients(count: usize) -> Vec<crate::address::Address> {
        (0..count)
            .map(|index| format!("recipient-{index}@example.com").parse().unwrap())
            .collect()
    }

    fn recipient_commands(recipients: &[crate::address::Address]) -> String {
        recipients
            .iter()
            .map(|recipient| format!("RCPT TO:<{recipient}>\r\n"))
            .collect()
    }

    /// RFC 5321 4.1.1.4: the terminator's leading CRLF is the message's own
    /// final CRLF. The blocking writers must reuse an existing one rather than
    /// appending an empty line the sender never wrote, and the chunked writer
    /// must carry the last two bytes across iterator items so a CRLF
    /// straddling a chunk boundary is still recognized.
    #[test]
    fn data_terminator_preserves_exact_message_bytes() {
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
                SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
                    .unwrap();

            connection.message(body).unwrap();
            transcript.assert_exhausted();
        }

        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("body\r", "")
            .expect("\n", "")
            .expect(".\r\n", "250 queued\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        connection
            .message_iter([b"body\r".as_slice(), b"\n".as_slice()].into_iter())
            .unwrap();
        transcript.assert_exhausted();

        // `write_body` is the single-buffer writer the LMTP batch path uses,
        // and it must reuse the caller's final CRLF too. The SIZE declaration
        // is pinned alongside it so the RFC 1870 count stays honest.
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect(
                "LHLO client.example\r\n",
                "250-lmtp.example\r\n250 SIZE 1024\r\n",
            )
            .expect(
                "MAIL FROM:<sender@example.com> SIZE=6\r\n",
                "250 sender ok\r\n",
            )
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body\r\n", "")
            .expect(".\r\n", "250 delivered\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "recipient@example.com".parse().unwrap(),
        }];
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();

        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body\r\n",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert_eq!(outcome.succeeded().len(), 1);
        transcript.assert_exhausted();

        // `message_lmtp_iter` is the chunked LMTP writer behind `send_lmtp`.
        // It carries the same last-two-bytes state, so a CRLF ending the final
        // chunk is reused rather than duplicated.
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect(
                "RCPT TO:<recipient@example.com>\r\n",
                "250 recipient ok\r\n",
            )
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body\r\n", "")
            .expect(".\r\n", "250 delivered\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();

        let statuses = connection.send_lmtp(&envelope, b"body\r\n").unwrap();

        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].is_positive());
        transcript.assert_exhausted();
    }

    #[test]
    fn pipelining_drains_each_recipient_window_before_writing_the_next() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = recipients(33);
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.push_str(&recipient_commands(&recipients[..32]));
        let mut first_replies = "250 sender ok\r\n".to_owned();
        first_replies.push_str(&"250 recipient ok\r\n".repeat(32));
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        connection.send(&envelope, b"body").unwrap();
        transcript.assert_exhausted();
    }

    /// A 550 followed by a 421 inside one pipelined RCPT window reports the 550
    /// (the first refusal) but still retires the connection, and no RSET is
    /// scripted or written into the channel the peer said it is closing. The
    /// biting assertion on the exact op stream is
    /// `a_421_later_in_an_rcpt_window_aborts_and_reports_the_first_refusal`
    /// in the core tests; this one pins the same exchange through the driver.
    #[test]
    fn a_421_after_a_550_in_one_rcpt_window_retires_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = recipients(2);
        let mut window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        window.push_str(&recipient_commands(&recipients));
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(
                window,
                "250 sender ok\r\n550 recipient rejected\r\n421 closing transmission channel\r\n",
            );
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let error = connection.send(&envelope, b"body").unwrap_err();

        assert!(
            error
                .smtp_response()
                .is_some_and(|response| response.has_code(550)),
            "the first refusal is the reported one: {error:?}"
        );
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn pipelined_batch_keeps_original_indexes_across_a_window_boundary() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses = recipients(33);
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.push_str(&recipient_commands(&addresses[..32]));
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
        let recipients = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                recipients,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "item-31");
        assert_eq!(outcome.succeeded().last().unwrap().item.0, "item-32");
        transcript.assert_exhausted();
    }

    /// A real PIPELINING peer answers the whole window in one TCP segment, so
    /// after the MAIL reply is parsed the buffer still holds every RCPT reply.
    /// The window drain must defer the surplus-bytes check to
    /// `finish_reply_group`, exactly like the envelope pipelined path and the
    /// async twin; a per-reply surplus check misreads a healthy exchange as an
    /// unsolicited reply and fails the batch.
    #[test]
    fn pipelined_batch_drains_coalesced_window_replies() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses = recipients(3);
        let mut window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        window.push_str(&recipient_commands(&addresses));
        let mut replies = "250 sender ok\r\n".to_owned();
        replies.push_str(&"250 recipient ok\r\n".repeat(3));
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect_coalesced(window, replies)
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert_eq!(outcome.succeeded().len(), 3);
        assert!(outcome.failed().is_empty());
        assert!(outcome.uncertain().is_empty());
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    /// A later recipient window fails to write. `DATA` was never issued, so no
    /// content can have reached the peer: every still-open recipient must land
    /// in the `failed` lane with `Unsent` evidence, and the RCPT rejection the
    /// server already gave must survive. `uncertain` here would be a false
    /// claim that the message might have been delivered.
    #[test]
    fn later_window_write_failure_reports_unsent_not_uncertain() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses = recipients(33);
        let mut first_window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        first_window.push_str(&recipient_commands(&addresses[..32]));
        let mut first_replies = "250 sender ok\r\n".to_owned();
        first_replies.push_str(&"250 recipient ok\r\n".repeat(31));
        first_replies.push_str("550 recipient rejected\r\n");
        // No step for the second window: its write fails at the transport.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(first_window, first_replies);
        let batch = addresses
            .into_iter()
            .enumerate()
            .map(|(index, address)| SmtpBatchRecipient {
                id: BatchItemId(format!("item-{index}")),
                address,
            })
            .collect();

        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert!(
            outcome.uncertain().is_empty(),
            "DATA was never issued, so nothing may be reported as uncertain"
        );
        assert_eq!(outcome.succeeded().len(), 0);
        assert_eq!(outcome.failed().len(), 33);
        assert_eq!(outcome.failed()[31].item.0, "item-31");
    }

    #[test]
    fn rcpt_reply_read_failure_reports_unsent_not_uncertain() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        assert!(outcome.uncertain().is_empty());
        assert_eq!(outcome.failed().len(), 3);
        assert_eq!(outcome.failed()[1].item.0, "item-1");
        assert!(outcome.failed()[0].error.scope().is_none());
        assert!(outcome.failed()[2].error.scope().is_none());
    }

    #[test]
    fn lmtp_drains_one_final_status_per_accepted_recipient() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = vec![
            "first@example.com".parse().unwrap(),
            "second@example.com".parse().unwrap(),
        ];
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
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();
        let statuses = connection.send_lmtp(&envelope, b"body").unwrap();

        assert!(statuses[0].is_positive());
        assert!(!statuses[1].is_positive());
        // Surplus bytes below the read buffer are undetectable without a
        // blocking read, so a completed LMTP drain retires the connection.
        assert!(connection.should_retire());
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn lmtp_too_few_final_statuses_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = vec![
            "first@example.com".parse().unwrap(),
            "second@example.com".parse().unwrap(),
        ];
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            .expect("\r\n.\r\n", "250 first delivered\r\n");
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();
        assert!(connection.send_lmtp(&envelope, b"body").is_err());
        // `has_broken()` after an abort is sharp against the adapter
        // dropping the `Op::Abort` arm entirely, and only that: it does NOT
        // bite against removing `abort()`'s own `set_state(Broken)`, because
        // the shutdown `abort()` also performs sets `Closed`, which fails
        // `verify()` just as well. The same holds for every abort assertion
        // in both transcript suites.
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn lmtp_too_many_final_statuses_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let recipients = vec![
            "first@example.com".parse().unwrap(),
            "second@example.com".parse().unwrap(),
        ];
        let transcript = Transcript::new("220 lmtp.example\r\n")
            .expect("LHLO client.example\r\n", "250 lmtp.example\r\n")
            .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
            .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
            .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
            .expect("DATA\r\n", "354 send body\r\n")
            .expect("body", "")
            // One segment: the `BufReader` prefetches the surplus reply along
            // with the last expected one, which is what production sees.
            .expect_coalesced(
                "\r\n.\r\n",
                "250 first delivered\r\n250 second delivered\r\n250 surplus response\r\n",
            );
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();
        let error = connection
            .send_lmtp(&envelope, b"body")
            .expect_err("a surplus final status desynchronizes the stream");
        assert!(
            error.to_string().contains("more final statuses"),
            "expected an honest LMTP final-status error, got: {error}"
        );
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }

    #[test]
    fn lmtp_batch_surplus_final_status_retires_the_connection_and_keeps_outcomes() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Lmtp).unwrap();
        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
            .unwrap()
            .resolve();

        // Both deliveries are known, so the batch result stands; the stream is
        // what cannot be reused.
        assert_eq!(outcome.succeeded().len(), 2);
        assert!(outcome.uncertain().is_empty());
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }

    /// `Op::Abort` is a request until the adapter performs it. The core's
    /// batch `MAIL FROM` rejection asks for one; what has to be true at the
    /// wire afterwards is that the stream can never be handed out again.
    #[test]
    fn an_aborting_batch_failure_leaves_the_stream_broken() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
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
    #[test]
    fn a_rejected_pipelined_mail_from_restores_the_stream_at_the_group_close() {
        let hello = ClientId::Domain("client.example".to_owned());
        let addresses = recipients(2);
        let mut window = "MAIL FROM:<sender@example.com>\r\n".to_owned();
        window.push_str(&recipient_commands(&addresses));
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect_coalesced(
                window,
                "550 sender rejected\r\n250 recipient ok\r\n250 recipient ok\r\n",
            );
        let envelope =
            Envelope::new(Some("sender@example.com".parse().unwrap()), addresses).unwrap();

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        connection
            .send_with_options(&envelope, b"body", &SendOptions::default())
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
    #[test]
    fn a_clean_lmtp_batch_drain_leaves_the_stream_broken_and_retired() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Lmtp).unwrap();
        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &Default::default(),
            )
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

    // The tests below replace the socket-listener tests this harness retired.
    // Same behaviors, same assertions, no listener or thread.

    #[test]
    fn abort_closes_without_quit_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        // No step after EHLO: any byte `abort` writes is a transcript failure.
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        connection.abort();
        transcript.assert_exhausted();
    }

    #[test]
    fn peer_certificate_der_is_none_on_plaintext() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();

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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();

        assert!(!connection.test_connected());
        assert!(connection.has_broken());
    }

    #[test]
    fn send_with_options_pipelines_envelope_parameters_in_one_window() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let response = connection
            .send_with_options(&envelope, b"Subject: test\r\n\r\nHello", &options)
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[test]
    fn pipelined_send_rsets_when_a_recipient_is_rejected() {
        let hello = ClientId::Domain("client.example".to_owned());
        // DATA is no longer pipelined, so a rejected RCPT is cleaned up with
        // RSET before any DATA command is issued, and the connection stays
        // reusable.
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .is_err()
        );
        assert!(!connection.has_broken());
        assert!(connection.test_connected());
        transcript.assert_exhausted();
    }

    #[test]
    fn explicit_mail_parameters_are_not_duplicated() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let response = connection
            .send_with_options(&envelope, "Subject: test\r\n\r\nHéllo".as_bytes(), &options)
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[test]
    fn batch_eai_recipient_requires_smtputf8_before_mail_from() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: crate::address::Address::new_dangerous("üser", "example.com"),
        }];
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let (error, _) = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .expect_err("EAI recipient must be rejected before MAIL FROM");

        assert!(error.to_string().contains("SMTPUTF8"));
        assert!(connection.test_connected());
        transcript.assert_exhausted();
    }

    #[test]
    fn hostile_unchecked_sender_never_reaches_the_wire() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .is_err()
        );
        assert!(
            !connection.has_broken(),
            "a rejected sender must not break the connection"
        );
        assert!(connection.test_connected(), "the connection stays reusable");
        transcript.assert_exhausted();
    }

    #[test]
    fn hostile_unchecked_recipient_leaves_no_open_transaction() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        assert!(
            connection
                .send(&envelope, b"Subject: test\r\n\r\nHello")
                .is_err()
        );
        assert!(
            !connection.has_broken(),
            "a rejection before MAIL FROM must not break the connection"
        );
        assert!(
            connection.test_connected(),
            "the connection must still be reusable, not stranded in a transaction"
        );
        transcript.assert_exhausted();
    }

    #[test]
    fn send_bdat_uses_chunking_without_dot_stuffing() {
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
            // The leading dot is transmitted verbatim: BDAT is length-framed,
            // so dot-stuffing would corrupt the body.
            .expect(message, "250 queued\r\n");
        let envelope = Envelope::new(
            Some("sender@example.com".parse().unwrap()),
            vec!["recipient@example.com".parse().unwrap()],
        )
        .unwrap();
        let options =
            SendOptions::new().mail_parameter(MailParameter::Body(MailBodyParameter::BinaryMime));

        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let response = connection
            .send_bdat_with_options(&envelope, message, &options)
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }

    #[test]
    fn whitespace_only_ehlo_extension_is_a_parse_error() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250- \r\n250 AUTH PLAIN\r\n");

        let error = SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp)
            .err()
            .expect("whitespace-only EHLO text must be rejected");

        assert!(error.is_parse(), "expected parse error, got {error:?}");
        transcript.assert_exhausted();
    }

    #[test]
    fn scram_client_final_is_sent_once_and_enhanced_235_verifies() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH SCRAM-SHA-256\r\n")
            .expect("AUTH SCRAM-SHA-256\r\n", "334 \r\n")
            .expect_generated(scram_server_first_reply)
            .expect_generated(scram_enhanced_success_reply)
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let response = connection
            .auth(
                &[Mechanism::ScramSha256],
                &Credentials::password("user", "pencil"),
            )
            .unwrap();

        assert!(response.has_code(235));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn auth_succeeds_when_the_tenth_legacy_challenge_is_answered() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let response = connection
            .auth(&[Mechanism::OAuthBearer], &credentials)
            .unwrap();

        assert!(response.has_code(235));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn auth_refreshes_server_info_with_ehlo() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

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
            .unwrap();

        assert!(response.has_code(235));
        // The post-AUTH EHLO replaced the capability set wholesale.
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

    #[test]
    fn oauthbearer_auth_sends_initial_response_and_refreshes_ehlo() {
        let hello = ClientId::Domain("client.example".to_owned());
        // GS2 header escaping of the authzid is part of the wire contract.
        let initial = crate::base64::encode("n,a=us=2Cer=3Done,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "235 authenticated\r\n",
            )
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let response = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("us,er=one", "token"),
            )
            .unwrap();

        assert!(response.has_code(235));
        transcript.assert_exhausted();
    }

    #[test]
    fn a_server_credential_rejection_names_the_mechanism_in_support_diagnostics() {
        // The blocking half of the server-rejection naming. A 535 says only
        // that authentication failed; without the rung on the phase stamp the
        // consumer cannot tell an operator WHICH mechanism the server
        // refused, while the local-policy path has always named mechanisms.
        // The name rides the support-only tier and must not disturb the
        // classification, which is still read from the reply code.
        let hello = ClientId::Domain("client.example".to_owned());
        let plain = format!(
            "AUTH PLAIN {}\r\n",
            crate::base64::encode("\u{0}user\u{0}pass")
        );
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH PLAIN\r\n")
            .expect(plain, "535 5.7.8 credentials rejected\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let error = connection
            .auth(
                &[Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .unwrap_err();

        assert_eq!(
            error.phase(),
            Some(super::SmtpCommandPhase::Auth {
                mechanism: Some("PLAIN")
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
                .any(|text| text.contains("mechanism PLAIN")),
            "support text did not name the refused mechanism: {:?}",
            consented.support_text
        );
        transcript.assert_exhausted();
    }

    #[test]
    fn oauthbearer_immediate_rejection_marks_connection_broken() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "535 rejected\r\n",
            );
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let error = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("user", "token"),
            )
            .unwrap_err();

        assert!(
            error.is_permanent(),
            "expected permanent SMTP error: {error:?}"
        );
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn oauthbearer_failed_challenge_sends_cancel_response() {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(format!("AUTH OAUTHBEARER {initial}\r\n"), "334 e30=\r\n")
            // RFC 7628 requires the client to answer a failure challenge with
            // the single `0x01` cancel byte before reading the final reply.
            .expect("AQ==\r\n", "535 rejected\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let error = connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2("user", "token"),
            )
            .unwrap_err();

        assert!(
            error.is_permanent(),
            "expected permanent SMTP error: {error:?}"
        );
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    #[test]
    fn starttls_downgrade_is_refused_without_a_wire_command() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        assert!(!connection.can_starttls());
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .expect_err("a server without the STARTTLS capability must not be upgraded");
        assert!(
            error.to_string().contains("STARTTLS is not supported"),
            "expected a capability refusal, got: {error}"
        );
        assert!(error.is_policy(), "expected a policy refusal, got: {error}");

        // The refusal happened before any byte hit the wire: the very next
        // scripted step is NOOP, so a stray STARTTLS write would be rejected.
        connection
            .command(crate::transport::smtp::commands::Noop)
            .unwrap();
        transcript.assert_exhausted();
    }

    #[test]
    fn starttls_refuses_plaintext_bytes_buffered_after_the_reply() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect_coalesced(
                "STARTTLS\r\n",
                "220 go ahead\r\n250 attacker-controlled capabilities\r\n",
            );
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection
            .starttls(&tls, &hello)
            .expect_err("pre-TLS injected bytes must refuse the upgrade");

        assert!(error.to_string().contains("unsolicited reply"));
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }

    /// The capability is advertised, so the driver must actually issue
    /// `STARTTLS` and only then attempt the upgrade. The transcript stream is
    /// not a TCP socket, so the upgrade stops at the handshake boundary: that
    /// is exactly as far as a hermetic test can follow, and it still pins the
    /// wire step and its ordering.
    #[test]
    fn advertised_starttls_writes_the_command_before_the_handshake() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "220 ready to start tls\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        assert!(connection.can_starttls());
        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .expect_err("the in-process transcript stream cannot complete a TLS handshake");
        assert!(
            error.to_string().contains("only supported on TCP"),
            "expected the upgrade to fail at the handshake boundary, got: {error}"
        );
        // The STARTTLS step was consumed, and no post-upgrade EHLO was sent.
        transcript.assert_exhausted();
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
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

    /// A server that advertises STARTTLS may still refuse it. The refusal must
    /// surface as the server's reply, and no upgrade may be attempted.
    #[test]
    fn refused_starttls_reply_does_not_upgrade_the_stream() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "454 TLS temporarily unavailable\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let tls = super::TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .expect_err("a refused STARTTLS must not be treated as an upgrade");
        assert!(
            error
                .smtp_response()
                .is_some_and(|response| response.has_code(454)),
            "expected the 454 reply to survive, got: {error}"
        );
        assert!(!connection.is_encrypted());
        transcript.assert_exhausted();
    }

    /// A peer that hangs up in the middle of a multiline reply must produce a
    /// parse failure, not a hang and not a half-parsed capability set.
    #[test]
    fn peer_closing_mid_reply_line_is_a_parse_failure() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect_then_close(HELLO, "250-smtp.example\r\n250 PIPELI");

        let Err(error) = SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp) else {
            panic!("a truncated EHLO reply cannot yield a usable connection");
        };
        assert!(
            error.to_string().contains("incomplete response"),
            "expected an incomplete-response parse error, got: {error}"
        );
    }

    /// The peer closes right after accepting `DATA`. The body write fails, so
    /// the message may or may not have been seen: every recipient must land in
    /// the `uncertain` lane, never `succeeded`.
    #[test]
    fn peer_closing_after_the_data_command_leaves_recipients_uncertain() {
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
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
            .expect("a mid-body drop is a per-recipient outcome, not a batch-level error")
            .resolve();

        assert!(outcome.succeeded().is_empty());
        assert_eq!(outcome.uncertain().len(), 1);
        assert_eq!(outcome.uncertain()[0].item.0, "item-0");
        assert!(connection.has_broken());
    }

    /// An unsolicited reply coalesced with a requested reply breaks the
    /// connection before it can be mistaken for the next command's answer.
    ///
    /// The coalesced segment is not an incidental choice of harness feature:
    /// it is the ONLY shape in which an unsolicited reply is observable at
    /// all. Outside the LMTP final-status drain, SMTP has no surplus-reply
    /// detection that runs while no reply is owed - a reply that arrives on an
    /// idle connection is simply not read until the next command asks for one.
    /// Detection therefore happens exactly when the surplus has already
    /// crossed into the `BufReader` alongside an answer that was owed, which
    /// is what `expect_coalesced` reproduces. A test scripting the surplus as
    /// a separate segment would pin nothing; that is an accepted limit of the
    /// protocol, not of the harness.
    #[test]
    fn unsolicited_reply_coalesced_with_an_answer_breaks_the_connection() {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_coalesced("NOOP\r\n", "250 noop ok\r\n421 service closing\r\n");
        let mut connection =
            SmtpConnection::from_transcript(transcript, &hello, Protocol::Smtp).unwrap();

        let error = connection
            .command(crate::transport::smtp::commands::Noop)
            .expect_err("the surplus reply must break the connection immediately");
        assert!(
            error.to_string().contains("unsolicited reply"),
            "expected a desynchronization error, got: {error}"
        );
        assert!(connection.has_broken());
    }

    /// DSN RCPT parameters are refused before `MAIL FROM` when the server did
    /// not advertise DSN. The refusal must be local: no envelope byte may hit
    /// the wire, so the transaction can be retried on the same connection.
    #[test]
    fn dsn_rcpt_parameters_are_refused_before_any_envelope_byte() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let error = connection
            .send_with_options(&envelope, b"body", &options)
            .expect_err("DSN parameters must not be emitted to a server without DSN");
        assert!(
            error.to_string().contains("require server DSN support"),
            "expected a local DSN refusal, got: {error}"
        );
        // A server that lacks the extension the request needs: `Unsupported`,
        // not the caller's malformed input, per the local-refusal rule in
        // `reference/error-model.md`.
        assert_eq!(
            error.kind(),
            &crate::transport::smtp::error::ErrorKind::FeatureUnsupported
        );

        // Nothing was written: the next scripted step is still NOOP.
        connection
            .command(crate::transport::smtp::commands::Noop)
            .unwrap();
        transcript.assert_exhausted();
    }

    /// Recipient-specific parameters are keyed by address. A parameter naming
    /// an address that is not in the batch is a caller bug and must be caught
    /// before `MAIL FROM`, never silently dropped.
    #[test]
    fn batch_rcpt_parameters_for_an_unknown_recipient_fail_before_mail_from() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let (error, progress) = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &options,
            )
            .expect_err("unmatched recipient parameters are a pre-wire input error");
        assert!(
            error
                .to_string()
                .contains("do not match an envelope recipient"),
            "expected an unmatched-recipient refusal, got: {error}"
        );
        let outcome = progress.resolve();
        assert!(outcome.succeeded().is_empty());

        connection
            .command(crate::transport::smtp::commands::Noop)
            .unwrap();
        transcript.assert_exhausted();
    }

    /// Per-recipient DSN parameters override the uniform ones on the matching
    /// RCPT line only, and the batch path emits the same wire shape as the
    /// envelope path. Sequential (non-PIPELINING) peer, so each RCPT line is
    /// pinned as its own write.
    #[test]
    fn batch_rcpt_options_are_emitted_per_recipient_in_order() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &options,
            )
            .expect("the transaction completes")
            .resolve();

        assert_eq!(outcome.succeeded().len(), 2);
        transcript.assert_exhausted();
    }

    /// A negative reply to the end-of-data terminator is an ANSWER, not a
    /// transport failure: the server read the whole message and refused it, so
    /// the transaction is complete (RFC 5321 4.1.1.4), every accepted
    /// recipient is a `failed` lane classified under `DataFinal`, and the
    /// connection stays reusable. Routing it through the `Failed` arm put the
    /// recipients in `uncertain` (asking the caller to reconcile a delivery the
    /// server explicitly refused) and threw away a healthy connection.
    #[test]
    fn a_rejected_data_final_reply_fails_the_recipients_and_keeps_the_connection() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
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
        // An SMTP 5xx is permanent (RFC 5321 4.2.1). Before the protocol-aware
        // row in `derive_server` this lane derived `Retry(SameRequest)` - an
        // instruction to resend the whole body to a server that had just
        // permanently refused it.
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
    #[test]
    fn a_rejected_data_final_reply_keeps_the_direct_connection_reusable() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let error = connection.send(&envelope, b"body").unwrap_err();
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
    #[test]
    fn a_rejected_bdat_last_resets_the_transaction_before_the_connection_is_reused() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let error = connection
            .send_bdat_with_options(&envelope, b"body", &SendOptions::default())
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
    #[test]
    fn a_421_data_final_reply_aborts_instead_of_parking_a_dying_connection() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();

        let error = connection.send(&envelope, b"body").unwrap_err();
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
    #[test]
    fn a_421_data_final_reply_aborts_the_batch_connection_too() {
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
            SmtpConnection::from_transcript(transcript.clone(), &hello, Protocol::Smtp).unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body",
                &SendOptions::default(),
            )
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
}
