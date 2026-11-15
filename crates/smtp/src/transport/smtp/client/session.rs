//! Session protocol: greeting, hello, STARTTLS and SASL, without I/O.

use super::{
    TlsParameters,
    core::{Epilogue, Op, OpOutcome, ProtocolMachine, Step},
};
use crate::transport::smtp::{
    Dialect,
    authentication::{
        Credentials, Mechanism, ScramExchange, ScramStep, decode_auth_challenge,
        decode_scram_payload, oauth_mechanism, password_mechanism, resolve_scram_binding,
        scram_hash,
    },
    commands::{Auth, Ehlo, Lhlo, Starttls},
    error::{self, Error, SmtpCommandPhase},
    extension::{ClientId, Extension, ServerInfo},
    response::Response,
};
use bifrost_sasl::Secret;
use std::fmt::{Display, Write as _};
use zeroize::Zeroizing;

type SessionResult = Result<(ServerInfo, Option<Response>), Error>;

enum Entry {
    Greeting,
    StartTls(TlsParameters),
    Auth {
        mechanisms: Vec<Mechanism>,
        credentials: Credentials,
        certificate: Option<Vec<u8>>,
    },
}

enum Stage {
    Start(Entry),
    Writing(Box<Stage>),
    Greeting,
    Hello,
    StartTls(TlsParameters),
    Upgraded,
    Token,
    Sasl,
    ScramFirst,
    ScramChallenge,
    ScramFinal,
    ScramAck,
}

struct Authentication {
    mechanism: Mechanism,
    exchange: Exchange,
}

enum Exchange {
    ResolvingToken { identity: String },
    Sasl(bifrost_sasl::SaslClient),
    Scram(ScramExchange),
}

impl Authentication {
    fn scram(&mut self) -> &mut ScramExchange {
        match &mut self.exchange {
            Exchange::Scram(exchange) => exchange,
            _ => unreachable!("SCRAM stage owns a SCRAM exchange"),
        }
    }
    fn sasl(&mut self) -> &mut bifrost_sasl::SaslClient {
        match &mut self.exchange {
            Exchange::Sasl(client) => client,
            _ => unreachable!("SASL stage owns a resolved client"),
        }
    }
}

/// All session entry points use the same hello and authentication continuations.
/// Token resolution and TLS upgrades are effects; the machine owns their order.
pub(super) struct Session {
    dialect: Dialect,
    hello: ClientId,
    info: Option<ServerInfo>,
    auth: Option<Authentication>,
    response: Option<Response>,
    stage: Stage,
    epilogue: Option<Epilogue<SessionResult>>,
}

impl Session {
    fn new(dialect: Dialect, hello: &ClientId, info: ServerInfo, entry: Entry) -> Self {
        Self {
            dialect,
            hello: hello.clone(),
            info: Some(info),
            auth: None,
            response: None,
            stage: Stage::Start(entry),
            epilogue: None,
        }
    }
    pub(super) fn greeting(dialect: Dialect, hello: &ClientId) -> Self {
        Self::new(dialect, hello, ServerInfo::default(), Entry::Greeting)
    }
    pub(super) fn starttls(
        dialect: Dialect,
        hello: &ClientId,
        info: ServerInfo,
        tls: TlsParameters,
    ) -> Self {
        Self::new(dialect, hello, info, Entry::StartTls(tls))
    }
    pub(super) fn auth(
        dialect: Dialect,
        hello: &ClientId,
        info: ServerInfo,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
        certificate: Option<Vec<u8>>,
    ) -> Self {
        Self::new(
            dialect,
            hello,
            info,
            Entry::Auth {
                mechanisms: mechanisms.to_vec(),
                credentials: credentials.clone(),
                certificate,
            },
        )
    }
    fn phase(&self) -> SmtpCommandPhase {
        SmtpCommandPhase::Auth {
            mechanism: self.auth.as_ref().map(|auth| auth.mechanism),
        }
    }
    fn abort(&mut self, error: Error, phase: SmtpCommandPhase) -> Step<SessionResult> {
        let (epilogue, op) = Epilogue::abort(Err(error.or_phase(phase)));
        self.epilogue = Some(epilogue);
        Step::Run(op)
    }
    fn finish(&mut self) -> Step<SessionResult> {
        Step::Finish(Ok((
            self.info.take().expect("session finishes once"),
            self.response.take(),
        )))
    }
    fn write_then(
        &mut self,
        command: impl Display,
        next: Stage,
    ) -> Result<Step<SessionResult>, Error> {
        // AUTH bytes never enter a non-zeroizing command String or a Debug log.
        let mut bytes = Zeroizing::new(String::new());
        write!(&mut bytes, "{command}")
            .map_err(|_| error::internal("failed to serialize SMTP command"))?;
        self.stage = Stage::Writing(Box::new(next));
        Ok(Step::Run(Op::WriteSecret(bytes.into())))
    }
    fn continuation(&mut self, line: &str, next: Stage) -> Result<Step<SessionResult>, Error> {
        self.write_then(format_args!("{line}\r\n"), next)
    }
    fn send_hello(&mut self) -> Result<Step<SessionResult>, Error> {
        match self.dialect {
            Dialect::Smtp => self.write_then(Ehlo::new(self.hello.clone())?, Stage::Hello),
            Dialect::Lmtp => self.write_then(Lhlo::new(self.hello.clone())?, Stage::Hello),
        }
    }
    fn start_auth(
        &mut self,
        mechanisms: Vec<Mechanism>,
        credentials: Credentials,
        certificate: Option<Vec<u8>>,
    ) -> Result<Step<SessionResult>, Error> {
        let info = self.info.as_ref().expect("server info lives until finish");
        if let Some(mechanism) = oauth_mechanism(&mechanisms, info, &credentials) {
            let Credentials::OAuth2 { identity, .. } = &credentials else {
                unreachable!("OAuth selection requires OAuth credentials")
            };
            self.auth = Some(Authentication {
                mechanism,
                exchange: Exchange::ResolvingToken {
                    identity: identity.clone(),
                },
            });
            self.stage = Stage::Token;
            return Ok(Step::Run(Op::ResolveToken(credentials)));
        }
        if matches!(credentials, Credentials::OAuth2 { .. }) {
            return Err(error::policy("no compatible authentication mechanism"));
        }
        let candidate = mechanisms.iter().any(|mechanism| {
            scram_hash(*mechanism).is_some() && info.supports_auth_mechanism(*mechanism)
        });
        let binding = if candidate {
            resolve_scram_binding(certificate.as_deref())?
        } else {
            None
        };
        let selection = password_mechanism(&mechanisms, info, binding.as_ref())?;
        let mechanism = selection.mechanism;
        let (exchange, command, stage) = if let Some(rung) = selection.scram {
            let (username, password) = credentials.password_parts()?;
            let exchange = ScramExchange::new(
                scram_hash(mechanism).expect("SCRAM rung"),
                rung.binding,
                username,
                password.into(),
            )?;
            (
                Exchange::Scram(exchange),
                Auth::Start(mechanism),
                Stage::ScramFirst,
            )
        } else {
            let mut client = mechanism.password_client(&credentials)?;
            let command = Auth::new(mechanism, &mut client)?;
            (Exchange::Sasl(client), command, Stage::Sasl)
        };
        self.auth = Some(Authentication {
            mechanism,
            exchange,
        });
        self.write_then(command, stage)
    }
    fn verify_final(&mut self, response: &Response, continuation: bool) -> Result<(), Error> {
        let server_final = if continuation {
            decode_auth_challenge(response)?
        } else {
            decode_scram_payload(response)?
        };
        let exchange = self
            .auth
            .as_mut()
            .map(Authentication::scram)
            .expect("SCRAM exchange");
        match exchange.step(&server_final)? {
            ScramStep::Complete => Ok(()),
            ScramStep::Reply(_) => Err(error::parse("unexpected SCRAM reply after server-final")),
        }
    }
    fn advance(&mut self, stage: Stage, outcome: OpOutcome) -> Result<Step<SessionResult>, Error> {
        if let OpOutcome::Failed(error) = outcome {
            return Err(error);
        }
        match stage {
            Stage::Start(entry) => match entry {
                Entry::Greeting => {
                    self.stage = Stage::Greeting;
                    Ok(Step::Run(Op::ReadSingle))
                }
                Entry::StartTls(tls) => {
                    if !self
                        .info
                        .as_ref()
                        .expect("server info")
                        .supports_feature(Extension::StartTls)
                    {
                        return Err(error::policy("STARTTLS is not supported on this server"));
                    }
                    self.write_then(Starttls, Stage::StartTls(tls))
                }
                Entry::Auth {
                    mechanisms,
                    credentials,
                    certificate,
                } => self.start_auth(mechanisms, credentials, certificate),
            },
            Stage::Writing(next) => {
                self.stage = *next;
                Ok(Step::Run(Op::ReadSingle))
            }
            Stage::Token => {
                let OpOutcome::Token(token) = outcome else {
                    unreachable!("token effect returns a token")
                };
                let auth = self.auth.as_mut().expect("selected auth");
                let Exchange::ResolvingToken { identity } = &auth.exchange else {
                    unreachable!("token stage owns a pending OAuth identity")
                };
                let mut client = auth.mechanism.oauth_client(identity, token.as_str())?;
                let command = Auth::new(auth.mechanism, &mut client)?;
                auth.exchange = Exchange::Sasl(client);
                self.write_then(command, Stage::Sasl)
            }
            Stage::Upgraded => self.send_hello(),
            stage => {
                let OpOutcome::Reply(response) = outcome else {
                    unreachable!("read effect returns a reply")
                };
                // AUTH allows 334 challenges; session completion requires the
                // actual terminal code so an extra challenge cannot become Ready.
                if !response.is_positive() {
                    return Err(error::status(response));
                }
                match stage {
                    Stage::Greeting => {
                        if !response.has_code(220) {
                            return Err(error::status(response));
                        }
                        self.send_hello()
                    }
                    Stage::Hello => {
                        if !response.is_completion() {
                            return Err(error::status(response));
                        }
                        self.info = Some(ServerInfo::from_response(&response)?);
                        Ok(self.finish())
                    }
                    Stage::StartTls(tls) => {
                        if !response.has_code(220) {
                            return Err(error::status(response));
                        }
                        self.stage = Stage::Upgraded;
                        Ok(Step::Run(Op::UpgradeTls(tls)))
                    }
                    Stage::Sasl => {
                        let auth = self.auth.as_mut().expect("selected auth");
                        if response.has_code(334) {
                            let command = Auth::new_from_response(auth.sasl(), &response)?;
                            self.write_then(command, Stage::Sasl)
                        } else {
                            if !response.has_code(235) {
                                return Err(error::status(response));
                            }
                            // An enterprise user needs a completed mechanism,
                            // even when the server claims authentication early.
                            auth.sasl().finish()?;
                            self.response = Some(response);
                            self.send_hello()
                        }
                    }
                    Stage::ScramFirst => {
                        if !response.has_code(334) {
                            return Err(error::status(response));
                        }
                        let first: Secret = self
                            .auth
                            .as_mut()
                            .map(Authentication::scram)
                            .expect("SCRAM exchange")
                            .client_first();
                        self.continuation(first.as_str(), Stage::ScramChallenge)
                    }
                    Stage::ScramChallenge => {
                        let first = decode_auth_challenge(&response)?;
                        match self
                            .auth
                            .as_mut()
                            .map(Authentication::scram)
                            .expect("SCRAM exchange")
                            .step(&first)?
                        {
                            ScramStep::Reply(final_line) => {
                                self.continuation(final_line.as_str(), Stage::ScramFinal)
                            }
                            ScramStep::Complete => {
                                Err(error::parse("SCRAM completed before server-final"))
                            }
                        }
                    }
                    Stage::ScramFinal => {
                        if response.has_code(334) {
                            self.verify_final(&response, true)?;
                            self.continuation("", Stage::ScramAck)
                        } else if response.has_code(235) {
                            self.verify_final(&response, false)?;
                            self.response = Some(response);
                            self.send_hello()
                        } else {
                            Err(error::status(response))
                        }
                    }
                    Stage::ScramAck => {
                        if !response.has_code(235) {
                            return Err(error::status(response));
                        }
                        self.response = Some(response);
                        self.send_hello()
                    }
                    _ => unreachable!("non-reply stages handled above"),
                }
            }
        }
    }
}

impl ProtocolMachine for Session {
    type Output = SessionResult;
    fn step(&mut self, outcome: OpOutcome) -> Step<Self::Output> {
        if let Some(epilogue) = self.epilogue.take() {
            return match epilogue.advance(outcome) {
                Ok((epilogue, op)) => {
                    self.epilogue = Some(epilogue);
                    Step::Run(op)
                }
                Err(result) => Step::Finish(result),
            };
        }
        let stage = std::mem::replace(&mut self.stage, Stage::Hello);
        let phase = match &stage {
            Stage::Start(Entry::Auth { .. }) => SmtpCommandPhase::Auth { mechanism: None },
            Stage::Start(Entry::StartTls(_)) | Stage::StartTls(_) | Stage::Upgraded => {
                SmtpCommandPhase::StartTls
            }
            Stage::Start(Entry::Greeting) | Stage::Greeting => SmtpCommandPhase::Greeting,
            Stage::Hello => SmtpCommandPhase::Hello,
            Stage::Writing(next) => match next.as_ref() {
                Stage::Hello => SmtpCommandPhase::Hello,
                Stage::StartTls(_) => SmtpCommandPhase::StartTls,
                _ => self.phase(),
            },
            _ => self.phase(),
        };
        // Unsupported STARTTLS and incompatible credentials are local refusals;
        // they have not changed the stream and must not close a usable connection.
        // Token resolution and credential preparation still precede AUTH.
        // A local refusal here leaves the connection usable for repair/retry.
        let local_start = matches!(stage, Stage::Start(_) | Stage::Token);
        match self.advance(stage, outcome) {
            Ok(step) => step,
            Err(error) if local_start => {
                let phase = if matches!(phase, SmtpCommandPhase::Auth { .. }) {
                    self.phase()
                } else {
                    phase
                };
                Step::Finish(Err(error.or_phase(phase)))
            }
            Err(error) => self.abort(error, phase),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reply(line: &str) -> OpOutcome {
        OpOutcome::Reply(line.parse().unwrap())
    }

    #[test]
    fn tls_upgrade_precedes_hello_and_replaces_pre_tls_capabilities() {
        for dialect in [Dialect::Smtp, Dialect::Lmtp] {
            let hello = ClientId::Domain("client.example".to_owned());
            let info = ServerInfo::from_response(
                &"250-old\r\n250-AUTH PLAIN\r\n250 STARTTLS\r\n"
                    .parse()
                    .unwrap(),
            )
            .unwrap();
            let mut session = Session::starttls(
                dialect,
                &hello,
                info,
                TlsParameters::new("smtp.example".to_owned()).unwrap(),
            );
            let Step::Run(Op::WriteSecret(command)) = session.step(OpOutcome::Done) else {
                panic!("STARTTLS must be the first op")
            };
            assert_eq!(command.as_str(), "STARTTLS\r\n");
            assert!(matches!(
                session.step(OpOutcome::Done),
                Step::Run(Op::ReadSingle)
            ));
            assert!(matches!(
                session.step(reply("220 upgrade\r\n")),
                Step::Run(Op::UpgradeTls(_))
            ));
            let Step::Run(Op::WriteSecret(command)) = session.step(OpOutcome::Done) else {
                panic!("upgrade must precede hello")
            };
            assert_eq!(
                command.as_str(),
                if dialect == Dialect::Smtp {
                    "EHLO client.example\r\n"
                } else {
                    "LHLO client.example\r\n"
                }
            );
            assert!(matches!(
                session.step(OpOutcome::Done),
                Step::Run(Op::ReadSingle)
            ));
            let Step::Finish(Ok((info, response))) =
                session.step(reply("250-new\r\n250 SIZE 123\r\n"))
            else {
                panic!("session should be ready")
            };
            assert!(response.is_none());
            assert!(!info.supports_auth_mechanism(Mechanism::Plain));
            assert!(!info.supports_feature(Extension::StartTls));
        }
    }

    #[test]
    fn secret_ops_redact_both_command_and_token_outcomes() {
        let command = Op::WriteSecret(Secret::from("AUTH token-is-secret\r\n"));
        let token = OpOutcome::Token(Secret::from("token-is-secret"));
        assert!(!format!("{command:?} {token:?}").contains("token-is-secret"));
    }
}
