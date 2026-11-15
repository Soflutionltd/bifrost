use bifrost_sasl::{SaslClient, ScramChannelBinding, ScramHash};

use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{ContinuationRequest, StatusKind, TaggedResponse, UntaggedResponse};
use crate::types::{AuthMechanism, SecretString};

use super::{Consumer, ConsumerContext, ContinuationConsumer, ContinuationReply, Finalized};

/// Map a `bifrost-sasl` computation failure back into the IMAP error model.
///
/// Protocol errors remain protocol errors; typed SCRAM refusals retain the
/// shared refusal class until account translation. Invalid credentials are
/// refused locally before AUTHENTICATE and never retire the connection.
impl From<bifrost_sasl::SaslError> for Error {
    fn from(e: bifrost_sasl::SaslError) -> Self {
        match e {
            bifrost_sasl::SaslError::Protocol(m) => {
                Error::Provider(crate::error::ProviderFailure::Protocol(m))
            }
            bifrost_sasl::SaslError::InvalidCredential(m) => {
                Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(m.into()))
            }
            bifrost_sasl::SaslError::LocalRuntime(m) => Error::Internal {
                kind: crate::error::InternalFailure::Runtime(m),
                mid_exchange: None,
            },
            bifrost_sasl::SaslError::ServerRejected(refusal) => {
                Error::ServerStatus(crate::error::ServerStatus::Auth {
                    text: refusal.to_string(),
                    code: None,
                    mechanism: None,
                    scram: Some(refusal),
                })
            }
            // `SaslError` is `#[non_exhaustive]`; any future variant is an
            // mapping gap until it is handled explicitly; it is not evidence
            // that the user must replace credentials.
            other => Error::Internal {
                kind: crate::error::InternalFailure::Invariant(other.to_string()),
                mid_exchange: None,
            },
        }
    }
}

/// Validate a tagged response for an authentication command.
///
/// Returns the response on OK, or an [`crate::error::ServerStatus::Auth`] for NO / [`crate::error::ServerStatus::Bad`]
/// for BAD. AUTH commands need a more specific error than the generic
/// [`crate::error::ServerStatus::No`] that [`TaggedResponse::require_ok`] produces.
///
/// `mechanism` is the wire name of the mechanism this consumer drove. The
/// consumer is the last place that still knows it - by the time the error
/// reaches the account boundary the command is gone - so it is stamped here
/// and carried to the diagnostics tier rather than reconstructed later.
fn require_ok_auth(
    tagged: TaggedResponse,
    mechanism: AuthMechanism,
) -> Result<TaggedResponse, Error> {
    match tagged.status {
        StatusKind::Ok => Ok(tagged),
        StatusKind::No => Err(Error::auth_with_mechanism(
            tagged.text,
            tagged.code,
            mechanism,
        )),
        StatusKind::Bad => Err(Error::bad_with_code(tagged.text, tagged.code)),
    }
}

/// Consumer for LOGIN (RFC 3501 Section6.2.3).
///
/// LOGIN has no solicited untagged responses of its own. Whether the
/// post-authentication CAPABILITY refresh is owed is decided by the driver
/// from the tagged OK alone (`ProtocolState::take_transition`), not here.
#[derive(Default)]
pub(crate) struct LoginConsumer {
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for LoginConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        self.buffered.push(resp);
    }

    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        // `buffered` is `Either`-classified data whose only carrier is the
        // event stream, so a rejected LOGIN surrenders it rather than
        // dropping it with the consumer.
        if let Err(e) = require_ok_auth(tagged, AuthMechanism::ImapLogin) {
            return Finalized::failure(e, self.buffered);
        }
        Finalized::success((), self.buffered)
    }
}

/// IMAP framing for a shared SASL exchange, including deferred SASL-IR.
pub(crate) struct AuthenticateSaslConsumer {
    client: SaslClient,
    initial: Option<SecretString>,
    pending: Option<SecretString>,
    buffered: Vec<UntaggedResponse>,
}

impl AuthenticateSaslConsumer {
    pub(crate) fn new(mut client: SaslClient, sasl_ir_used: bool) -> Self {
        use base64::Engine;
        let initial = client.initial_response().map(|raw| {
            base64::engine::general_purpose::STANDARD
                .encode(raw.as_bytes())
                .into()
        });
        Self {
            client,
            pending: if sasl_ir_used { None } else { initial.clone() },
            initial,
            buffered: Vec::new(),
        }
    }

    pub(crate) fn initial_response(&self) -> Option<SecretString> {
        self.initial.clone()
    }

    fn named_sasl_error(&self, error: bifrost_sasl::SaslError) -> Error {
        let mechanism = self.client.mechanism();
        let mut mapped = Error::from(error);
        match &mut mapped {
            Error::ServerStatus(crate::error::ServerStatus::Auth {
                mechanism: stamp, ..
            }) => *stamp = Some(mechanism),
            Error::Provider(crate::error::ProviderFailure::Protocol(message)) => {
                message.push_str(&format!(" (mechanism {})", mechanism.name()));
            }
            _ => {}
        }
        mapped
    }
}

impl Consumer for AuthenticateSaslConsumer {
    type Output = ();
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        self.buffered.push(resp);
    }
    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        if let Err(error) = require_ok_auth(tagged, self.client.mechanism()) {
            return Finalized::failure(error, self.buffered);
        }
        // An enterprise user needs a verified session even when the server
        // claims success early. The driver retires a failed tagged-OK exchange.
        if self.pending.is_some() {
            return Finalized::failure(
                Error::Provider(crate::error::ProviderFailure::Protocol(
                    "SASL initial response was never sent".into(),
                )),
                self.buffered,
            );
        }
        if let Err(error) = self.client.finish() {
            let error = self.named_sasl_error(error);
            return Finalized::failure(error, self.buffered);
        }
        Finalized::success((), self.buffered)
    }
}

impl ContinuationConsumer for AuthenticateSaslConsumer {
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        use base64::Engine;
        let encoded = if let Some(initial) = self.pending.take() {
            initial
        } else {
            let challenge = base64::engine::general_purpose::STANDARD
                .decode(cont.data.trim())
                .map_err(|e| {
                    Error::Provider(crate::error::ProviderFailure::Protocol(format!(
                        "invalid base64 SASL continuation: {e}"
                    )))
                })?;
            match self
                .client
                .step(&challenge)
                .map_err(|e| self.named_sasl_error(e))?
            {
                bifrost_sasl::Step::Reply(raw) => base64::engine::general_purpose::STANDARD
                    .encode(raw.as_bytes())
                    .into(),
                bifrost_sasl::Step::Done => SecretString::default(),
                _ => {
                    return Err(Error::Internal {
                        kind: crate::error::InternalFailure::Invariant("unmapped SASL step".into()),
                        mid_exchange: None,
                    });
                }
            }
        };
        let mut bytes = Vec::with_capacity(encoded.len() + 2);
        bytes.extend_from_slice(encoded.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        Ok(ContinuationReply::Write(bytes))
    }
}

/// SCRAM construction adapter; all exchange state lives in bifrost-sasl.
pub(crate) struct AuthenticateScramConsumer(AuthenticateSaslConsumer);

impl AuthenticateScramConsumer {
    pub(crate) fn new(
        hash: ScramHash,
        user: String,
        pass: SecretString,
        nonce: String,
        sasl_ir_used: bool,
        binding: ScramChannelBinding,
    ) -> Result<Self, Error> {
        Ok(Self(AuthenticateSaslConsumer::new(
            SaslClient::scram(hash, binding, &user, &pass, nonce)?,
            sasl_ir_used,
        )))
    }
    pub(crate) fn initial_response(&self) -> SecretString {
        self.0.initial_response().unwrap_or_default()
    }
}
impl Consumer for AuthenticateScramConsumer {
    type Output = ();
    fn on_response(&mut self, resp: UntaggedResponse, notify: NotifyFlags, ctx: &ConsumerContext) {
        self.0.on_response(resp, notify, ctx);
    }
    fn finalize(self: Box<Self>, tagged: TaggedResponse, ctx: &ConsumerContext) -> Finalized<()> {
        Box::new(self.0).finalize(tagged, ctx)
    }
}
impl ContinuationConsumer for AuthenticateScramConsumer {
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        self.0.on_continuation(cont, ctx)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use base64::Engine;

    use super::*;

    fn tagged(status: StatusKind) -> TaggedResponse {
        TaggedResponse {
            tag: "A1".to_owned(),
            status,
            code: None,
            text: "authentication rejected".to_owned(),
        }
    }

    fn context() -> ConsumerContext<'static> {
        ConsumerContext {
            session: crate::types::profile::SessionCapabilities::empty(),
            command_target: None,
            command_tag: "A1",
        }
    }

    /// A password that fails SASLprep is refused when the consumer is BUILT,
    /// before AUTHENTICATE is submitted, as `InvalidInput`. It used to be
    /// discovered in `scram_client_final`, after the server-first message,
    /// as a connection-fatal `Protocol` error blaming the server.
    #[test]
    fn scram_consumer_gs2_and_c_attribute_agree() {
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
            let mut consumer = AuthenticateScramConsumer::new(
                ScramHash::Sha256,
                "user".to_owned(),
                "pencil".to_owned().into(),
                "nonce".to_owned(),
                true,
                binding,
            )
            .unwrap();
            assert_eq!(decode_initial(&consumer), format!("{header}n=user,r=nonce"));
            let cont = ContinuationRequest {
                code: None,
                data: base64::engine::general_purpose::STANDARD
                    .encode("r=nonceserver,s=QSXCR+Q6sek8bf92,i=4096"),
            };
            let reply = consumer.on_continuation(cont, &context()).unwrap();
            let ContinuationReply::Write(bytes) = reply;
            let encoded = std::str::from_utf8(&bytes).unwrap().trim();
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap();
            let final_message = String::from_utf8(decoded).unwrap();
            assert_eq!(
                final_message.split(',').next().unwrap(),
                format!("c={c_value}")
            );
        }
    }

    #[test]
    fn a_password_that_fails_saslprep_is_refused_before_authenticate() {
        let Err(err) = AuthenticateScramConsumer::new(
            ScramHash::Sha256,
            "user".to_owned(),
            "bad\u{7}".to_owned().into(),
            "nonce123".to_owned(),
            true,
            ScramChannelBinding::None,
        ) else {
            panic!("a prohibited password must not build a consumer");
        };
        assert!(
            matches!(
                err,
                Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(_))
            ),
            "got {err:?}"
        );
        assert!(!err.is_connection_fatal());
    }

    #[test]
    fn scram_rejection_before_server_final_is_an_auth_error() {
        let consumer = AuthenticateScramConsumer::new(
            ScramHash::Sha256,
            "user".to_owned(),
            "wrong".to_owned().into(),
            "nonce123".to_owned(),
            true,
            ScramChannelBinding::None,
        )
        .unwrap();

        let finalized = Box::new(consumer).finalize(tagged(StatusKind::No), &context());
        let err = match finalized.output {
            Err(err) => err,
            Ok(_) => panic!("tagged NO must reject authentication"),
        };
        assert!(matches!(
            err,
            Error::ServerStatus(crate::error::ServerStatus::Auth { .. })
        ));
    }

    #[test]
    fn success_before_deferred_initial_response_is_refused() {
        for client in [
            SaslClient::plain("", "user", "pw").unwrap(),
            SaslClient::oauth(bifrost_sasl::OAuthMechanism::Xoauth2, "user", "token").unwrap(),
        ] {
            let consumer = AuthenticateSaslConsumer::new(client, false);
            assert!(
                Box::new(consumer)
                    .finalize(tagged(StatusKind::Ok), &context())
                    .output
                    .is_err()
            );
        }
    }

    #[test]
    fn oauth_success_after_an_error_challenge_is_refused() {
        let mut consumer = AuthenticateSaslConsumer::new(
            SaslClient::oauth(bifrost_sasl::OAuthMechanism::OAuthBearer, "user", "token").unwrap(),
            true,
        );
        let cont = ContinuationRequest {
            code: None,
            data: "e30=".into(),
        };
        let ContinuationReply::Write(reply) = consumer.on_continuation(cont, &context()).unwrap();
        assert_eq!(reply, b"AQ==\r\n");
        assert!(
            Box::new(consumer)
                .finalize(tagged(StatusKind::Ok), &context())
                .output
                .is_err()
        );
    }

    #[test]
    fn oauth_error_continuations_use_the_mechanism_answer() {
        use bifrost_sasl::OAuthMechanism;

        for (mechanism, expected) in [
            (OAuthMechanism::Xoauth2, b"\r\n".as_slice()),
            (OAuthMechanism::OAuthBearer, b"AQ==\r\n".as_slice()),
        ] {
            for sasl_ir in [false, true] {
                let mut consumer = AuthenticateSaslConsumer::new(
                    SaslClient::oauth(mechanism, "user", "token").unwrap(),
                    sasl_ir,
                );
                let continuation = || ContinuationRequest {
                    code: None,
                    data: "e30=".to_owned(),
                };
                if !sasl_ir {
                    let ContinuationReply::Write(bytes) = consumer
                        .on_continuation(continuation(), &context())
                        .unwrap();
                    assert_eq!(
                        bytes,
                        format!("{}\r\n", consumer.initial_response().unwrap().as_str()).as_bytes()
                    );
                }
                let ContinuationReply::Write(bytes) = consumer
                    .on_continuation(continuation(), &context())
                    .unwrap();
                assert_eq!(bytes, expected);
            }
        }
    }

    #[test]
    fn a_server_rejection_names_the_mechanism_it_refused() {
        // The consumer is the last place that still knows which rung was on
        // the wire. If it does not stamp the name here, "which mechanism was
        // refused" is unrecoverable downstream - which is why it is asserted
        // on every consumer rather than only on one.
        fn mechanism_of(err: Error) -> Option<AuthMechanism> {
            match err {
                Error::ServerStatus(crate::error::ServerStatus::Auth { mechanism, .. }) => {
                    mechanism
                }
                other => panic!("tagged NO must be an auth failure, got {other:?}"),
            }
        }

        let scram = AuthenticateScramConsumer::new(
            ScramHash::Sha256,
            "user".to_owned(),
            "wrong".to_owned().into(),
            "nonce123".to_owned(),
            true,
            ScramChannelBinding::TlsServerEndPoint(vec![1, 2, 3, 4]),
        )
        .unwrap();
        assert_eq!(
            mechanism_of(
                Box::new(scram)
                    .finalize(tagged(StatusKind::No), &context())
                    .output
                    .err()
                    .unwrap()
            ),
            // The PLUS suffix must survive: an operator debugging a channel
            // binding problem needs to see which of the two rungs was refused.
            Some(AuthMechanism::ScramSha256Plus),
        );

        let plain =
            AuthenticateSaslConsumer::new(SaslClient::plain("", "user", "pw").unwrap(), true);
        assert_eq!(
            mechanism_of(
                Box::new(plain)
                    .finalize(tagged(StatusKind::No), &context())
                    .output
                    .err()
                    .unwrap()
            ),
            Some(AuthMechanism::Plain),
        );

        let cram = AuthenticateSaslConsumer::new(SaslClient::cram_md5("user", "pw".into()), false);
        assert_eq!(
            mechanism_of(
                Box::new(cram)
                    .finalize(tagged(StatusKind::No), &context())
                    .output
                    .err()
                    .unwrap()
            ),
            Some(AuthMechanism::CramMd5),
        );

        let login = LoginConsumer::default();
        assert_eq!(
            mechanism_of(
                Box::new(login)
                    .finalize(tagged(StatusKind::No), &context())
                    .output
                    .err()
                    .unwrap()
            ),
            Some(AuthMechanism::ImapLogin),
        );

        // The XOAUTH2 consumer is shared by two mechanisms, so its token is
        // carried rather than inferred; both spellings must come back out.
        for mechanism in [
            bifrost_sasl::OAuthMechanism::Xoauth2,
            bifrost_sasl::OAuthMechanism::OAuthBearer,
        ] {
            let oauth = AuthenticateSaslConsumer::new(
                SaslClient::oauth(mechanism, "user", "token").unwrap(),
                true,
            );
            assert_eq!(
                mechanism_of(
                    Box::new(oauth)
                        .finalize(tagged(StatusKind::No), &context())
                        .output
                        .err()
                        .unwrap()
                ),
                Some(match mechanism {
                    bifrost_sasl::OAuthMechanism::Xoauth2 => AuthMechanism::Xoauth2,
                    _ => AuthMechanism::OAuthBearer,
                }),
            );
        }
    }

    #[test]
    fn a_sasl_failure_mid_exchange_names_the_rung_and_keeps_its_class() {
        // The `?` on the SASL calls used to go through the context-free
        // `From<SaslError>`, which cannot know the mechanism. Naming it must
        // not move the protocol-class / auth-class split.
        let mut consumer = AuthenticateScramConsumer::new(
            ScramHash::Sha256,
            "user".to_owned(),
            "pw".to_owned().into(),
            "nonce123".to_owned(),
            true,
            ScramChannelBinding::TlsServerEndPoint(vec![1, 2, 3, 4]),
        )
        .unwrap();

        let cont = ContinuationRequest {
            code: None,
            data: base64::engine::general_purpose::STANDARD.encode("garbage-without-r="),
        };
        // Matched rather than `expect_err`: that would require `Debug` on
        // `ContinuationReply`, whose `Write` arm carries SASL client-final
        // proof bytes. Deriving `Debug` there puts credential material one
        // `{:?}` away from a log line, so the test bends instead of the type.
        let Err(err) = consumer.on_continuation(cont, &context()) else {
            panic!("a non-SCRAM server-first is a protocol fault");
        };
        match err {
            Error::Provider(crate::error::ProviderFailure::Protocol(message)) => {
                assert!(message.contains("SCRAM-SHA-256-PLUS"), "got: {message}");
            }
            other => panic!("classification must stay protocol-class, got {other:?}"),
        }
    }

    #[test]
    fn scram_ok_before_server_final_is_still_a_protocol_error() {
        let consumer = AuthenticateScramConsumer::new(
            ScramHash::Sha256,
            "user".to_owned(),
            "pw".to_owned().into(),
            "nonce123".to_owned(),
            true,
            ScramChannelBinding::None,
        )
        .unwrap();

        let finalized = Box::new(consumer).finalize(tagged(StatusKind::Ok), &context());
        let err = match finalized.output {
            Err(err) => err,
            Ok(_) => panic!("tagged OK must not bypass server-final verification"),
        };
        assert!(matches!(
            err,
            Error::Provider(crate::error::ProviderFailure::Protocol(_))
        ));
    }

    fn decode_initial(consumer: &AuthenticateScramConsumer) -> String {
        let ir = consumer.initial_response();
        let raw = base64::engine::general_purpose::STANDARD
            .decode(ir.as_bytes())
            .unwrap();
        String::from_utf8(raw).unwrap()
    }

    #[test]
    fn scram_consumer_gs2_header_plus_prefix() {
        // Username with `=` and `,` exercises saslname escaping inside the
        // PLUS GS2 header.
        let consumer = AuthenticateScramConsumer::new(
            ScramHash::Sha256,
            "us=,er".to_owned(),
            "pw".to_owned().into(),
            "nonce123".to_owned(),
            true,
            ScramChannelBinding::TlsServerEndPoint(vec![1, 2, 3, 4]),
        )
        .unwrap();
        let decoded = decode_initial(&consumer);
        assert!(
            decoded.starts_with("p=tls-server-end-point,,n="),
            "PLUS client-first must carry the tls-server-end-point GS2 header, got: {decoded}"
        );
        // saslname escaping: `=` -> `=3D`, `,` -> `=2C`.
        assert!(
            decoded.contains("n=us=3D=2Cer,r=nonce123"),
            "escaped username + nonce must follow the header, got: {decoded}"
        );
    }

    #[test]
    fn scram_consumer_gs2_header_none_prefix() {
        let consumer = AuthenticateScramConsumer::new(
            ScramHash::Sha256,
            "user".to_owned(),
            "pw".to_owned().into(),
            "nonce123".to_owned(),
            true,
            ScramChannelBinding::None,
        )
        .unwrap();
        let decoded = decode_initial(&consumer);
        assert!(
            decoded.starts_with("n,,n=user,r=nonce123"),
            "non-PLUS client-first must keep the literal n,, header, got: {decoded}"
        );
    }
}
