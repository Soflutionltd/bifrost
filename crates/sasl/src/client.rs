//! Protocol-neutral SASL exchanges. Challenges are raw bytes and replies are
//! raw payloads; adapters own base64, command syntax and terminal replies.

use crate::{Mechanism, OAuthMechanism, SaslError, ScramChannelBinding, ScramHash, Secret};

/// The next action after a server challenge.
#[derive(Debug)]
#[non_exhaustive]
pub enum Step {
    Reply(Secret),
    /// The server's SCRAM proof has been verified. Any protocol acknowledgement
    /// and terminal success reply still belong to the adapter.
    Done,
}

enum State {
    Initial(Secret),
    LoginUser { user: Secret, password: Secret },
    LoginPassword(Secret),
    Cram { user: String, password: Secret },
    ScramFirst(Box<Scram>),
    ScramFinal(Vec<u8>),
    OAuthError(OAuthMechanism),
    Complete,
    Failed,
}

struct Scram {
    hash: ScramHash,
    binding: ScramChannelBinding,
    password: crate::ScramPassword,
    nonce: String,
    first_bare: Secret,
}

/// A single SASL exchange, including credential preparation and verification.
/// OAuth construction requires a resolved token; token-source I/O stays outside.
pub struct SaslClient {
    mechanism: Mechanism,
    state: State,
    after_initial: Option<State>,
}

impl std::fmt::Debug for SaslClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslClient")
            .field("mechanism", &self.mechanism)
            .finish_non_exhaustive()
    }
}

impl SaslClient {
    pub fn plain(authzid: &str, user: &str, password: &str) -> Result<Self, SaslError> {
        Ok(Self::initial(
            Mechanism::Plain,
            crate::plain_payload(authzid, user, password)?,
            State::Complete,
        ))
    }

    pub fn login(user: &str, password: Secret) -> Self {
        Self {
            mechanism: Mechanism::Login,
            state: State::LoginUser {
                user: user.into(),
                password,
            },
            after_initial: None,
        }
    }

    pub fn cram_md5(user: &str, password: Secret) -> Self {
        Self {
            mechanism: Mechanism::CramMd5,
            state: State::Cram {
                user: user.to_owned(),
                password,
            },
            after_initial: None,
        }
    }

    pub fn oauth(
        mechanism: OAuthMechanism,
        identity: &str,
        token: &str,
    ) -> Result<Self, SaslError> {
        let (name, payload) = match mechanism {
            OAuthMechanism::Xoauth2 => {
                (Mechanism::Xoauth2, crate::xoauth2_payload(identity, token)?)
            }
            OAuthMechanism::OAuthBearer => (
                Mechanism::OAuthBearer,
                crate::oauthbearer_payload(identity, token)?,
            ),
        };
        Ok(Self::initial(name, payload, State::OAuthError(mechanism)))
    }

    /// The nonce is supplied by the adapter's entropy source (or a deterministic
    /// test). This client owns it and all subsequent SCRAM state.
    pub fn scram(
        hash: ScramHash,
        binding: ScramChannelBinding,
        user: &str,
        password: &str,
        nonce: String,
    ) -> Result<Self, SaslError> {
        let user = crate::prepare_scram_username(user)?;
        let password = crate::prepare_scram_password(password)?;
        if nonce.is_empty()
            || !nonce
                .bytes()
                .all(|b| (0x21..=0x7e).contains(&b) && b != b',')
        {
            return Err(SaslError::InvalidCredential(
                "invalid SCRAM client nonce".into(),
            ));
        }
        let first_bare: Secret = format!("n={user},r={nonce}").into();
        let first = format!("{}{}", binding.gs2_header(), first_bare.as_str()).into();
        let mechanism = Mechanism::from_scram(hash, binding.binding());
        Ok(Self::initial(
            mechanism,
            first,
            State::ScramFirst(Box::new(Scram {
                hash,
                binding,
                password,
                nonce,
                first_bare,
            })),
        ))
    }

    fn initial(mechanism: Mechanism, payload: Secret, next: State) -> Self {
        Self {
            mechanism,
            state: State::Initial(payload),
            after_initial: Some(next),
        }
    }

    pub fn mechanism(&self) -> Mechanism {
        self.mechanism
    }

    /// Take the optional initial response once, whether sent inline or after a
    /// protocol prompt. Repeated calls never retransmit credentials.
    pub fn initial_response(&mut self) -> Option<Secret> {
        if !matches!(self.state, State::Initial(_)) {
            return None;
        }
        let next = self.after_initial.take().unwrap_or(State::Failed);
        match std::mem::replace(&mut self.state, next) {
            State::Initial(payload) => Some(payload),
            _ => None,
        }
    }

    /// Advance on a decoded server challenge. Any failure consumes the state,
    /// so neither credentials nor a failed SCRAM proof can be retried in place.
    pub fn step(&mut self, challenge: &[u8]) -> Result<Step, SaslError> {
        let state = std::mem::replace(&mut self.state, State::Failed);
        let (next, step) = match state {
            State::LoginUser { user, password } => {
                (State::LoginPassword(password), Step::Reply(user))
            }
            State::LoginPassword(password) => (State::Complete, Step::Reply(password)),
            State::Cram { user, password } => (
                State::Complete,
                Step::Reply(crate::cram::cram_md5_payload(&user, &password, challenge)?),
            ),
            State::ScramFirst(scram) => {
                let (encoded, signature) = crate::scram_client_final(
                    scram.hash,
                    &scram.password,
                    &scram.nonce,
                    &scram.first_bare,
                    challenge_text(challenge)?,
                    &scram.binding,
                )?;
                (
                    State::ScramFinal(signature),
                    Step::Reply(decode_reply(&encoded)?),
                )
            }
            State::ScramFinal(signature) => {
                crate::verify_server_final(challenge_text(challenge)?, &signature)?;
                (State::Complete, Step::Done)
            }
            State::OAuthError(mechanism) => {
                (State::Failed, Step::Reply(mechanism.error_ack().into()))
            }
            _ => return Err(SaslError::Protocol("unexpected SASL continuation".into())),
        };
        self.state = next;
        Ok(step)
    }

    /// Validate protocol success. SCRAM requires a verified server proof;
    /// OAuth cannot succeed after an error continuation has been acknowledged.
    pub fn finish(&self) -> Result<(), SaslError> {
        if matches!(self.state, State::Complete | State::OAuthError(_)) {
            Ok(())
        } else {
            Err(SaslError::Protocol(
                "SASL exchange ended before verification".into(),
            ))
        }
    }
}

fn decode_reply(encoded: &Secret) -> Result<Secret, SaslError> {
    crate::decode_continuation(encoded).map(Secret::from)
}

fn challenge_text(challenge: &[u8]) -> Result<&str, SaslError> {
    std::str::from_utf8(challenge)
        .map_err(|_| SaslError::Protocol("SCRAM challenge is not UTF-8".into()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    fn reply(step: Step) -> Secret {
        match step {
            Step::Reply(reply) => reply,
            Step::Done => panic!("expected reply"),
        }
    }

    #[test]
    fn login_owns_position_independently_of_prompt_text() {
        let mut client = SaslClient::login("alice", "secret".into());
        assert!(client.initial_response().is_none());
        assert!(client.finish().is_err());
        assert_eq!(reply(client.step(b"Password:").unwrap()).as_str(), "alice");
        assert!(client.finish().is_err());
        assert_eq!(reply(client.step(b"Username:").unwrap()).as_str(), "secret");
        client.finish().unwrap();
        assert!(client.step(b"again").is_err());
    }

    #[test]
    fn plain_initial_response_is_single_use_and_refuses_nul() {
        let mut client = SaslClient::plain("", "user", "pw").unwrap();
        assert!(client.finish().is_err());
        assert_eq!(client.initial_response().unwrap().as_bytes(), b"\0user\0pw");
        assert!(client.initial_response().is_none());
        client.finish().unwrap();
        assert!(client.step(b"challenge").is_err());
        for (authzid, user, password) in [
            ("a\0b", "user", "pw"),
            ("", "u\0ser", "pw"),
            ("", "user", "p\0w"),
        ] {
            assert!(matches!(
                SaslClient::plain(authzid, user, password),
                Err(SaslError::InvalidCredential(_))
            ));
        }
    }

    #[test]
    fn oauth_acknowledges_once_and_cannot_succeed_after_error() {
        for (mechanism, ack) in [
            (OAuthMechanism::Xoauth2, b"".as_slice()),
            (OAuthMechanism::OAuthBearer, b"\x01".as_slice()),
        ] {
            let mut client = SaslClient::oauth(mechanism, "user", "token").unwrap();
            assert!(client.finish().is_err());
            assert!(
                client
                    .initial_response()
                    .unwrap()
                    .contains("auth=Bearer token")
            );
            client.finish().unwrap();
            assert_eq!(
                reply(client.step(b"{\"status\":\"401\"}").unwrap()).as_bytes(),
                ack
            );
            assert!(client.finish().is_err());
            assert!(client.step(b"repeat").is_err());
        }
    }

    #[test]
    fn oauth_refuses_frame_delimiters_instead_of_rewriting_credentials() {
        for mechanism in [OAuthMechanism::Xoauth2, OAuthMechanism::OAuthBearer] {
            for (identity, token) in [
                ("u\x01ser", "token"),
                ("user", "tok\x01en"),
                ("user\r\n", "token"),
                ("user", "tok\0en"),
            ] {
                assert!(matches!(
                    SaslClient::oauth(mechanism, identity, token),
                    Err(SaslError::InvalidCredential(_))
                ));
            }
        }
    }

    #[test]
    fn cram_accepts_binary_challenges_and_owns_single_response() {
        let mut client = SaslClient::cram_md5("tim", "tanstaaftanstaaf".into());
        assert!(client.initial_response().is_none());
        let challenge = STANDARD
            .decode("PDE4OTYuNjk3MTcwOTUyQHBvc3RvZmZpY2UucmVzdG9uLm1jaS5uZXQ+")
            .unwrap();
        assert_eq!(
            reply(client.step(&challenge).unwrap()).as_str(),
            "tim b913a602c7eda7a495b4e6e7334d3890"
        );
        client.finish().unwrap();
        assert!(client.step(&challenge).is_err());
        let mut client = SaslClient::cram_md5("user", "pw".into());
        let raw = reply(client.step(&[0xff, 0x00, 0x80]).unwrap());
        assert_eq!(
            STANDARD.encode(raw.as_bytes()),
            crate::cram_md5_response("user", "pw", &STANDARD.encode([0xff, 0x00, 0x80]))
                .unwrap()
                .as_str()
        );
    }

    #[test]
    fn scram_rfc_5802_exchange_requires_verified_server_final() {
        let mut client = SaslClient::scram(
            ScramHash::Sha1,
            ScramChannelBinding::None,
            "user",
            "pencil",
            "fyko+d2lbbFgONRv9qkxdawL".into(),
        )
        .unwrap();
        assert_eq!(
            client.initial_response().unwrap().as_str(),
            "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL"
        );
        let step = client
            .step(b"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096")
            .unwrap();
        assert_eq!(
            reply(step).as_str(),
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
        );
        assert!(client.finish().is_err());
        assert!(matches!(
            client.step(b"v=rmF9pqV8S7suAoZWja4dJRkFsKQ=").unwrap(),
            Step::Done
        ));
        client.finish().unwrap();
        assert!(client.step(b"v=rmF9pqV8S7suAoZWja4dJRkFsKQ=").is_err());
    }

    #[test]
    fn scram_failed_proof_consumes_the_exchange() {
        for final_message in [b"v=AAAA".as_slice(), b"e=invalid-proof".as_slice()] {
            let mut client = SaslClient::scram(
                ScramHash::Sha256,
                ScramChannelBinding::None,
                "user",
                "pencil",
                "nonce".into(),
            )
            .unwrap();
            client.initial_response().unwrap();
            client
                .step(b"r=nonceserver,s=QSXCR+Q6sek8bf92,i=4096")
                .unwrap();
            assert!(client.step(final_message).is_err());
            assert!(client.finish().is_err());
            assert!(client.step(final_message).is_err());
        }
    }

    #[test]
    fn scram_gs2_and_binding_proof_share_one_owner_for_both_hashes() {
        for hash in [ScramHash::Sha1, ScramHash::Sha256] {
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
                let mut client =
                    SaslClient::scram(hash, binding, "us=,er", "pencil", "nonce".into()).unwrap();
                assert_eq!(
                    client.initial_response().unwrap().as_str(),
                    format!("{header}n=us=3D=2Cer,r=nonce")
                );
                let message = reply(
                    client
                        .step(b"r=nonceserver,s=QSXCR+Q6sek8bf92,i=4096")
                        .unwrap(),
                );
                assert!(message.starts_with(&format!("c={c_value},")));
                assert!(!format!("{client:?}").contains("pencil"));
            }
        }
    }
}
