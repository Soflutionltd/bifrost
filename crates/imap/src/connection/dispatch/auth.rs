use bifrost_sasl::{
    ScramChannelBinding, ScramHash, ScramPassword, cram_md5_response, decode_continuation,
    prepare_scram_password, prepare_scram_username, scram_client_final, verify_server_final,
};

use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{
    ContinuationRequest, ResponseCode, StatusKind, TaggedResponse, UntaggedResponse,
};
use crate::types::{AuthMechanism, SecretString};

use super::{Consumer, ConsumerContext, ContinuationConsumer, ContinuationReply, Finalized};

/// Map a `bifrost-sasl` computation failure back into the IMAP error model.
///
/// Protocol-class messages stay `Error::Protocol`; a SCRAM `e=` server error
/// (the auth-failure lane) stays `Error::auth_with_code(.., None)`. A
/// caller credential that fails SASLprep is `InvalidInput`: it is refused
/// while the consumer is built, before AUTHENTICATE is submitted, so it is a
/// local refusal and must neither blame the server nor retire the connection.
impl From<bifrost_sasl::SaslError> for Error {
    fn from(e: bifrost_sasl::SaslError) -> Self {
        match e {
            bifrost_sasl::SaslError::Protocol(m) => Error::Protocol(m),
            bifrost_sasl::SaslError::InvalidCredential(m) => Error::InvalidInput(m),
            bifrost_sasl::SaslError::AuthFailed(m) => Error::auth_with_code(m, None),
            // `SaslError` is `#[non_exhaustive]`; any future variant is an
            // unclassified auth failure until it is mapped explicitly.
            other => Error::auth_with_code(other.to_string(), None),
        }
    }
}

/// Validate a tagged response for an authentication command.
///
/// Returns the response on OK, or an [`Error::Auth`] for NO / [`Error::Bad`]
/// for BAD. AUTH commands need a more specific error than the generic
/// [`Error::No`] that [`TaggedResponse::require_ok`] produces.
///
/// `mechanism` is the wire name of the mechanism this consumer drove. The
/// consumer is the last place that still knows it - by the time the error
/// reaches the account boundary the command is gone - so it is stamped here
/// and carried to the diagnostics tier rather than reconstructed later.
fn require_ok_auth(
    tagged: TaggedResponse,
    mechanism: &'static str,
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
/// LOGIN has no solicited untagged responses of its own. Tracks whether
/// the server provided inline CAPABILITY data (either as an untagged
/// CAPABILITY response or in the tagged OK response code per
/// RFC 3501 Section6.2.3) so the caller knows whether to issue a follow-up
/// CAPABILITY command.
#[derive(Default)]
pub(crate) struct LoginConsumer {
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for LoginConsumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.2.1: the server SHOULD send updated capabilities
        // after authentication. Track it so the caller can skip a follow-up
        // CAPABILITY command.
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<bool> {
        // `buffered` is `Either`-classified data whose only carrier is the
        // event stream, so a rejected LOGIN surrenders it rather than
        // dropping it with the consumer.
        let tagged = match require_ok_auth(tagged, AuthMechanism::Login.name()) {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, self.buffered),
        };
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Finalized::success(self.caps_seen || caps_in_tagged, self.buffered)
    }
}

/// Consumer for AUTHENTICATE PLAIN (RFC 4616 / RFC 3501 Section6.2.2).
///
/// Implements [`ContinuationConsumer`] when the server sends `+`,
/// the consumer replies with the base64-encoded SASL PLAIN credentials.
/// When SASL-IR (RFC 4959) was used, the payload was already part of
/// the AUTHENTICATE command and no continuation is expected.
pub(crate) struct AuthenticatePlainConsumer {
    /// Base64-encoded SASL PLAIN payload (RFC 4616 Section2).
    encoded: SecretString,
    /// Whether the initial response has already been sent (via SASL-IR
    /// in the command, or via a previous continuation reply).
    initial_sent: bool,
    /// Whether capability data arrived during the exchange.
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl AuthenticatePlainConsumer {
    pub(crate) fn new(encoded: SecretString, sasl_ir_used: bool) -> Self {
        Self {
            encoded,
            initial_sent: sasl_ir_used,
            caps_seen: false,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for AuthenticatePlainConsumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.2.1: capabilities may arrive as untagged response
        // after authentication state changes.
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<bool> {
        // Surrender the `Either` buffer on both arms.
        let tagged = match require_ok_auth(tagged, AuthMechanism::Plain.name()) {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, self.buffered),
        };
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Finalized::success(self.caps_seen || caps_in_tagged, self.buffered)
    }
}

impl ContinuationConsumer for AuthenticatePlainConsumer {
    fn on_continuation(
        &mut self,
        _cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        if self.initial_sent {
            // PLAIN is a single-round mechanism (RFC 4616 Section2). A second
            // continuation is a protocol error.
            return Err(Error::Protocol(
                "unexpected continuation after PLAIN initial response \
                 (RFC 4616 Section 2)"
                    .into(),
            ));
        }
        self.initial_sent = true;
        let mut bytes = Vec::with_capacity(self.encoded.len() + 2);
        bytes.extend_from_slice(self.encoded.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        Ok(ContinuationReply::Write(bytes))
    }
}

/// Consumer for AUTHENTICATE XOAUTH2 (Google SASL mechanism).
///
/// Implements [`ContinuationConsumer`]. The first continuation triggers
/// the base64 credential payload. Subsequent continuations are XOAUTH2
/// error challenges; the client MUST reply with an empty `\r\n` to let
/// the server send the final tagged NO/BAD.
pub(crate) struct AuthenticateXoauth2Consumer {
    /// Base64-encoded XOAUTH2 payload.
    encoded: SecretString,
    /// Whether the initial credential payload has been sent.
    initial_sent: bool,
    /// Whether capability data arrived during the exchange.
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
    /// Wire mechanism token this consumer is driving. The consumer is shared
    /// by XOAUTH2 and OAUTHBEARER (identical framing), so the token cannot be
    /// inferred from the type and has to be carried.
    mechanism: &'static str,
}

impl AuthenticateXoauth2Consumer {
    pub(crate) fn new(encoded: SecretString, sasl_ir_used: bool, mechanism: &'static str) -> Self {
        Self {
            encoded,
            initial_sent: sasl_ir_used,
            caps_seen: false,
            buffered: Vec::new(),
            mechanism,
        }
    }
}

impl Consumer for AuthenticateXoauth2Consumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<bool> {
        // Surrender the `Either` buffer on both arms.
        let tagged = match require_ok_auth(tagged, self.mechanism) {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, self.buffered),
        };
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Finalized::success(self.caps_seen || caps_in_tagged, self.buffered)
    }
}

impl ContinuationConsumer for AuthenticateXoauth2Consumer {
    fn on_continuation(
        &mut self,
        _cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        if self.initial_sent {
            // XOAUTH2 error continuation: the server sent a base64-encoded
            // error as `+ <error>`. The client MUST respond with an empty
            // `\r\n` to let the server finish the exchange with a tagged
            // NO/BAD (Google XOAUTH2 spec, non-IETF).
            Ok(ContinuationReply::Write(b"\r\n".to_vec()))
        } else {
            // First continuation: send the XOAUTH2 credential payload.
            self.initial_sent = true;
            let mut bytes = Vec::with_capacity(self.encoded.len() + 2);
            bytes.extend_from_slice(self.encoded.as_bytes());
            bytes.extend_from_slice(b"\r\n");
            Ok(ContinuationReply::Write(bytes))
        }
    }
}

/// Consumer for AUTHENTICATE CRAM-MD5.
pub(crate) struct AuthenticateCramMd5Consumer {
    user: String,
    pass: SecretString,
    response_sent: bool,
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl AuthenticateCramMd5Consumer {
    pub(crate) fn new(user: String, pass: SecretString) -> Self {
        Self {
            user,
            pass,
            response_sent: false,
            caps_seen: false,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for AuthenticateCramMd5Consumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<bool> {
        // Surrender the `Either` buffer on both arms.
        let tagged = match require_ok_auth(tagged, AuthMechanism::CramMd5.name()) {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, self.buffered),
        };
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Finalized::success(self.caps_seen || caps_in_tagged, self.buffered)
    }
}

impl ContinuationConsumer for AuthenticateCramMd5Consumer {
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        if self.response_sent {
            return Err(Error::Protocol(
                "unexpected continuation after CRAM-MD5 response".into(),
            ));
        }
        self.response_sent = true;
        let encoded = cram_md5_response(&self.user, &self.pass, &cont.data)?;
        let mut bytes = Vec::with_capacity(encoded.as_str().len() + 2);
        bytes.extend_from_slice(encoded.as_bytes());
        bytes.extend_from_slice(b"\r\n");
        Ok(ContinuationReply::Write(bytes))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScramState {
    SendClientFirst,
    AwaitServerFirst,
    AwaitServerFinal,
    Done,
}

/// Consumer for SCRAM-SHA-1 and SCRAM-SHA-256.
pub(crate) struct AuthenticateScramConsumer {
    mechanism: ScramHash,
    /// SASLprep'd in `new`, before AUTHENTICATE is submitted, so a password
    /// the caller supplied that cannot be prepared is refused locally rather
    /// than discovered mid-exchange.
    pass: ScramPassword,
    client_nonce: String,
    client_first_bare: String,
    /// GS2 channel binding for this exchange. `None` reproduces the old
    /// `n,,` / `c=biws` behavior byte-for-byte; `TlsServerEndPoint` drives
    /// the `-PLUS` GS2 header and `c=` value.
    binding: ScramChannelBinding,
    state: ScramState,
    expected_server_signature: Option<Vec<u8>>,
    caps_seen: bool,
    buffered: Vec<UntaggedResponse>,
}

impl AuthenticateScramConsumer {
    pub(crate) fn new(
        mechanism: ScramHash,
        user: String,
        pass: SecretString,
        nonce: String,
        sasl_ir_used: bool,
        binding: ScramChannelBinding,
    ) -> Result<Self, Error> {
        let client_first_bare = format!("n={},r={nonce}", prepare_scram_username(&user)?);
        let pass = prepare_scram_password(pass.as_str())?;
        Ok(Self {
            mechanism,
            pass,
            client_nonce: nonce,
            client_first_bare,
            binding,
            state: if sasl_ir_used {
                ScramState::AwaitServerFirst
            } else {
                ScramState::SendClientFirst
            },
            expected_server_signature: None,
            caps_seen: false,
            buffered: Vec::new(),
        })
    }

    /// Wire mechanism token for this exchange, taken from the same
    /// `bifrost_sasl` authority that built the AUTHENTICATE command line, so
    /// the diagnostic can never name a different rung than the one sent.
    fn mechanism_name(&self) -> &'static str {
        self.mechanism.mechanism_name(self.binding.binding())
    }

    /// Re-message a `bifrost-sasl` failure so it names this rung, then route
    /// it through the crate's `From<SaslError>` split. The classification is
    /// untouched: `Protocol` stays protocol-class, `AuthFailed` stays on the
    /// auth-failure lane. Only the message grows, and the account boundary
    /// turns that message into SUPPORT-ONLY `DiagnosticText`. Mirrors
    /// `bifrost_smtp`'s `named_sasl_error`; the two crates must not differ in
    /// what a SCRAM refusal can tell an operator.
    fn named_sasl_error(&self, error: bifrost_sasl::SaslError) -> Error {
        let mechanism = self.mechanism_name();
        match error {
            bifrost_sasl::SaslError::Protocol(m) => {
                Error::Protocol(format!("{m} (mechanism {mechanism})"))
            }
            bifrost_sasl::SaslError::InvalidCredential(m) => {
                Error::InvalidInput(format!("{m} (mechanism {mechanism})"))
            }
            bifrost_sasl::SaslError::AuthFailed(m) => {
                Error::auth_with_mechanism(m, None, mechanism)
            }
            // `SaslError` is `#[non_exhaustive]`; any future variant is an
            // unclassified auth failure, matching the `From` impl above.
            other => Error::auth_with_mechanism(other.to_string(), None, mechanism),
        }
    }

    pub(crate) fn initial_response(&self) -> SecretString {
        use base64::Engine;

        // RFC 5802: the GS2 header MUST come from the binding, never a
        // literal, so the header and the `c=` value can never desync.
        base64::engine::general_purpose::STANDARD
            .encode(format!("{}{}", self.binding.gs2_header(), self.client_first_bare).as_bytes())
            .into()
    }
}

impl Consumer for AuthenticateScramConsumer {
    type Output = bool;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        if matches!(&resp, UntaggedResponse::Capability(_)) {
            self.caps_seen = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<bool> {
        // Surrender the `Either` buffer on every arm.
        let tagged = match require_ok_auth(tagged, self.mechanism_name()) {
            Ok(t) => t,
            Err(e) => return Finalized::failure(e, self.buffered),
        };
        if self.state != ScramState::Done {
            // Stays `Error::Protocol`, not a command failure: the driver reads
            // this class to decide the failure is connection-fatal before the
            // result is published. A tagged OK here means the server claimed
            // success without proving it knew the password.
            return Finalized::failure(
                Error::Protocol("SCRAM exchange ended before server-final verification".into()),
                self.buffered,
            );
        }
        let caps_in_tagged = matches!(&tagged.code, Some(ResponseCode::Capability(_)));
        Finalized::success(self.caps_seen || caps_in_tagged, self.buffered)
    }
}

impl ContinuationConsumer for AuthenticateScramConsumer {
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        _ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        match self.state {
            ScramState::SendClientFirst => {
                self.state = ScramState::AwaitServerFirst;
                let encoded = self.initial_response();
                let mut bytes = Vec::with_capacity(encoded.len() + 2);
                bytes.extend_from_slice(encoded.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                Ok(ContinuationReply::Write(bytes))
            }
            ScramState::AwaitServerFirst => {
                let server_first = decode_continuation(&cont.data)?;
                let (client_final, server_signature) = scram_client_final(
                    self.mechanism,
                    &self.pass,
                    &self.client_nonce,
                    &self.client_first_bare,
                    &server_first,
                    &self.binding,
                )
                .map_err(|e| self.named_sasl_error(e))?;
                self.expected_server_signature = Some(server_signature);
                self.state = ScramState::AwaitServerFinal;
                let mut bytes = Vec::with_capacity(client_final.as_str().len() + 2);
                bytes.extend_from_slice(client_final.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                Ok(ContinuationReply::Write(bytes))
            }
            ScramState::AwaitServerFinal => {
                let server_final = decode_continuation(&cont.data)?;
                // A local invariant, raised mid-exchange: the server is
                // waiting for our reply to its server-final, so this must
                // retire the connection. The exchange has no tagged
                // completion yet, hence `InFlight`.
                let expected = self.expected_server_signature.take().ok_or_else(|| {
                    Error::internal_mid_exchange(
                        "SCRAM server signature missing from client state",
                        bifrost_types::TransmissionState::InFlight,
                    )
                })?;
                verify_server_final(&server_final, &expected)
                    .map_err(|e| self.named_sasl_error(e))?;
                self.state = ScramState::Done;
                Ok(ContinuationReply::Write(b"\r\n".to_vec()))
            }
            ScramState::Done => Err(Error::Protocol(
                "unexpected continuation after SCRAM server-final message".into(),
            )),
        }
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
            capabilities: &[],
            enabled: &[],
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
        assert!(matches!(err, Error::InvalidInput(_)), "got {err:?}");
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
        assert!(matches!(err, Error::Auth { .. }));
    }

    #[test]
    fn a_server_rejection_names_the_mechanism_it_refused() {
        // The consumer is the last place that still knows which rung was on
        // the wire. If it does not stamp the name here, "which mechanism was
        // refused" is unrecoverable downstream - which is why it is asserted
        // on every consumer rather than only on one.
        fn mechanism_of(err: Error) -> Option<&'static str> {
            match err {
                Error::Auth { mechanism, .. } => mechanism,
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
            Some("SCRAM-SHA-256-PLUS"),
        );

        let plain = AuthenticatePlainConsumer::new("payload".to_owned().into(), true);
        assert_eq!(
            mechanism_of(
                Box::new(plain)
                    .finalize(tagged(StatusKind::No), &context())
                    .output
                    .err()
                    .unwrap()
            ),
            Some("PLAIN"),
        );

        let cram = AuthenticateCramMd5Consumer::new("user".to_owned(), "pw".to_owned().into());
        assert_eq!(
            mechanism_of(
                Box::new(cram)
                    .finalize(tagged(StatusKind::No), &context())
                    .output
                    .err()
                    .unwrap()
            ),
            Some("CRAM-MD5"),
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
            Some("LOGIN"),
        );

        // The XOAUTH2 consumer is shared by two mechanisms, so its token is
        // carried rather than inferred; both spellings must come back out.
        for name in ["XOAUTH2", "OAUTHBEARER"] {
            let oauth = AuthenticateXoauth2Consumer::new("payload".to_owned().into(), true, name);
            assert_eq!(
                mechanism_of(
                    Box::new(oauth)
                        .finalize(tagged(StatusKind::No), &context())
                        .output
                        .err()
                        .unwrap()
                ),
                Some(name),
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
            Error::Protocol(message) => {
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
        assert!(matches!(err, Error::Protocol(_)));
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
