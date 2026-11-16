//! Every session and send transcript is defined once and driven through both adapters.

#[cfg(feature = "tokio")]
use super::async_connection::AsyncSmtpConnection;
use super::{TlsParameters, connection::SmtpConnection};
use crate::address::{Address, Envelope};
use crate::transport::smtp::{
    Dialect,
    account_error::{SmtpErrorContext, into_account_error},
    authentication::{Credentials, Mechanism},
    batch::{SendProgress, SmtpBatchRecipient},
    error::{Error, SmtpCommandPhase, SmtpTransmissionState},
    extension::{
        ClientId, DeliverByMode, DsnNotify, DsnReturn, Extension, MailBodyParameter, MailParameter,
        SendOptions, ServerInfo,
    },
    response::Response,
    test_support::{Transcript, scram_enhanced_success_reply, scram_server_first_reply},
};
use bifrost_types::error::BatchItemId;
use std::fmt::Display;
const HELLO: &str = "EHLO client.example\r\n";

#[derive(Clone, Copy)]
enum Adapter {
    Blocking,
    #[cfg(feature = "tokio")]
    Async,
}
const ADAPTERS: &[Adapter] = &[
    Adapter::Blocking,
    #[cfg(feature = "tokio")]
    Adapter::Async,
];
enum Connection {
    Blocking(Box<SmtpConnection>),
    #[cfg(feature = "tokio")]
    Async(Box<AsyncSmtpConnection>),
}

impl Connection {
    fn should_retire(&self) -> bool {
        match self {
            Self::Blocking(connection) => connection.should_retire(),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.should_retire(),
        }
    }

    async fn test_connected(&mut self) -> bool {
        match self {
            Self::Blocking(connection) => connection.test_connected(),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.test_connected().await,
        }
    }

    async fn message(&mut self, message: &[u8]) -> Result<Response, Error> {
        match self {
            Self::Blocking(connection) => connection.message(message),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.message(message).await,
        }
    }

    async fn message_iter<I, B>(&mut self, message: I) -> Result<Response, Error>
    where
        I: Iterator<Item = B>,
        B: AsRef<[u8]>,
    {
        match self {
            Self::Blocking(connection) => connection.message_iter(message),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.message_iter(message).await,
        }
    }

    async fn send(&mut self, envelope: &Envelope, body: &[u8]) -> Result<Response, Error> {
        match self {
            Self::Blocking(connection) => connection.send(envelope, body),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.send(envelope, body).await,
        }
    }

    async fn send_with_options(
        &mut self,
        envelope: &Envelope,
        body: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        match self {
            Self::Blocking(connection) => connection.send_with_options(envelope, body, options),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.send_with_options(envelope, body, options).await,
        }
    }

    async fn send_bdat_with_options(
        &mut self,
        envelope: &Envelope,
        body: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        match self {
            Self::Blocking(connection) => {
                connection.send_bdat_with_options(envelope, body, options)
            }
            #[cfg(feature = "tokio")]
            Self::Async(connection) => {
                connection
                    .send_bdat_with_options(envelope, body, options)
                    .await
            }
        }
    }

    async fn send_lmtp(
        &mut self,
        envelope: &Envelope,
        body: &[u8],
    ) -> Result<Vec<Response>, Error> {
        match self {
            Self::Blocking(connection) => connection.send_lmtp(envelope, body),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.send_lmtp(envelope, body).await,
        }
    }

    async fn send_lmtp_bdat_with_options(
        &mut self,
        envelope: &Envelope,
        body: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        match self {
            Self::Blocking(connection) => {
                connection.send_lmtp_bdat_with_options(envelope, body, options)
            }
            #[cfg(feature = "tokio")]
            Self::Async(connection) => {
                connection
                    .send_lmtp_bdat_with_options(envelope, body, options)
                    .await
            }
        }
    }

    async fn send_smtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        body: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        match self {
            Self::Blocking(connection) => {
                connection.send_smtp_batch(from, recipients, body, options)
            }
            #[cfg(feature = "tokio")]
            Self::Async(connection) => {
                connection
                    .send_smtp_batch(from, recipients, body, options)
                    .await
            }
        }
    }

    async fn send_lmtp_batch(
        &mut self,
        from: Option<Address>,
        recipients: Vec<SmtpBatchRecipient>,
        body: &[u8],
        options: &SendOptions,
    ) -> Result<SendProgress, (Error, SendProgress)> {
        match self {
            Self::Blocking(connection) => {
                connection.send_lmtp_batch(from, recipients, body, options)
            }
            #[cfg(feature = "tokio")]
            Self::Async(connection) => {
                connection
                    .send_lmtp_batch(from, recipients, body, options)
                    .await
            }
        }
    }

    async fn from_transcript(
        adapter: Adapter,
        transcript: Transcript,
        hello: &ClientId,
        dialect: Dialect,
    ) -> Result<Self, Error> {
        match adapter {
            Adapter::Blocking => SmtpConnection::from_transcript(transcript, hello, dialect)
                .map(|connection| Self::Blocking(Box::new(connection))),
            #[cfg(feature = "tokio")]
            Adapter::Async => AsyncSmtpConnection::from_transcript(transcript, hello, dialect)
                .await
                .map(|connection| Self::Async(Box::new(connection))),
        }
    }
    fn server_info(&self) -> &ServerInfo {
        match self {
            Self::Blocking(connection) => connection.server_info(),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.server_info(),
        }
    }
    fn has_broken(&self) -> bool {
        match self {
            Self::Blocking(connection) => connection.has_broken(),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.has_broken(),
        }
    }
    fn can_starttls(&self) -> bool {
        match self {
            Self::Blocking(connection) => connection.can_starttls(),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.can_starttls(),
        }
    }
    fn is_encrypted(&self) -> bool {
        match self {
            Self::Blocking(connection) => connection.is_encrypted(),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.is_encrypted(),
        }
    }
    async fn auth(
        &mut self,
        mechanisms: &[Mechanism],
        credentials: &Credentials,
    ) -> Result<Response, Error> {
        match self {
            Self::Blocking(connection) => connection.auth(mechanisms, credentials),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.auth(mechanisms, credentials).await,
        }
    }
    async fn starttls(&mut self, tls: &TlsParameters, hello: &ClientId) -> Result<(), Error> {
        match self {
            Self::Blocking(connection) => connection.starttls(tls, hello),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.starttls(tls.clone(), hello).await,
        }
    }
    async fn command(&mut self, command: impl Display) -> Result<Response, Error> {
        match self {
            Self::Blocking(connection) => connection.command(command),
            #[cfg(feature = "tokio")]
            Self::Async(connection) => connection.command(command).await,
        }
    }
}

#[tokio::test]
async fn an_oversized_greeting_line_is_a_parse_error_not_a_hang() {
    for &adapter in ADAPTERS {
        // A WELL-FORMED greeting that is merely too long. A line of garbage
        // would fail to parse whether or not the cap exists, so it pins
        // nothing about the cap; this one parses fine once the cap is removed.
        let mut banner = format!("220 {}", "x".repeat(4096));
        banner.push_str("\r\n");
        let transcript = Transcript::new(&banner);

        let error = Connection::from_transcript(
            adapter,
            transcript,
            &ClientId::Domain("client.example".to_owned()),
            Dialect::Smtp,
        )
        .await
        .err()
        .expect("an oversized banner line must surface as an error");

        assert!(error.is_parse(), "expected a parse error, got {error:?}");
    }
}

#[tokio::test]
async fn whitespace_only_ehlo_extension_is_a_parse_error() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250- \r\n250 AUTH PLAIN\r\n");

        let error = Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
            .await
            .err()
            .expect("whitespace-only EHLO text must be rejected");

        assert!(error.is_parse(), "expected parse error, got {error:?}");
        transcript.assert_exhausted();
    }
}

fn scram_continuation_success_reply(client: &[u8]) -> Vec<u8> {
    String::from_utf8(scram_enhanced_success_reply(client))
        .unwrap()
        .replacen("235 2.7.0 ", "334 ", 1)
        .into_bytes()
}

#[tokio::test]
async fn scram_server_final_continuation_and_enhanced_235_verify() {
    for &adapter in ADAPTERS {
        for continuation in [false, true] {
            let hello = ClientId::Domain("client.example".to_owned());
            let mut transcript = Transcript::new("220 smtp.example\r\n")
                .expect(HELLO, "250-smtp.example\r\n250 AUTH SCRAM-SHA-256\r\n")
                .expect("AUTH SCRAM-SHA-256\r\n", "334 \r\n")
                .expect_generated(scram_server_first_reply)
                .expect_generated(if continuation {
                    scram_continuation_success_reply
                } else {
                    scram_enhanced_success_reply
                });
            if continuation {
                transcript = transcript.expect("\r\n", "235 authenticated\r\n");
            }
            let transcript = transcript.expect(HELLO, "250 smtp.example\r\n");
            let mut connection =
                Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
    }
}

#[tokio::test]
async fn sasl_oauth_rejects_repeated_error_challenges_and_false_success() {
    for &adapter in ADAPTERS {
        for (mechanism, initial, ack) in [
            (
                Mechanism::Xoauth2,
                "user=user\x01auth=Bearer token\x01\x01",
                "\r\n",
            ),
            (
                Mechanism::OAuthBearer,
                "n,a=user,\x01auth=Bearer token\x01\x01",
                "AQ==\r\n",
            ),
        ] {
            for final_reply in ["334 e30=\r\n", "235 authenticated\r\n"] {
                let hello = ClientId::Domain("client.example".to_owned());
                let transcript = Transcript::new("220 smtp.example\r\n")
                    .expect(
                        HELLO,
                        format!("250-smtp.example\r\n250 AUTH {mechanism}\r\n"),
                    )
                    .expect(
                        format!("AUTH {mechanism} {}\r\n", crate::base64::encode(initial)),
                        "334 e30=\r\n",
                    )
                    .expect(ack, final_reply);
                let mut connection =
                    Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                        .await
                        .unwrap();
                let error = connection
                    .auth(&[mechanism], &Credentials::oauth2("user", "token"))
                    .await
                    .unwrap_err();
                assert!(error.is_parse(), "{error:?}");
                assert_eq!(
                    error.phase(),
                    Some(SmtpCommandPhase::Auth {
                        mechanism: Some(mechanism)
                    })
                );
                assert!(connection.has_broken());
                transcript.assert_exhausted();
            }
        }
    }
}

#[tokio::test]
async fn sasl_login_owns_position_for_empty_and_binary_prompts() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH LOGIN\r\n")
            .expect("AUTH LOGIN\r\n", "334 /wCA\r\n")
            .expect("YWxpY2U=\r\n", "334 \r\n")
            .expect("d29uZGVybGFuZA==\r\n", "235 authenticated\r\n")
            .expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let response = connection
            .auth(
                &[Mechanism::Login],
                &Credentials::password("alice", "wonderland"),
            )
            .await
            .unwrap();
        assert!(response.has_code(235));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn sasl_login_refuses_early_success_and_extra_challenges() {
    for &adapter in ADAPTERS {
        for stage in 0..3 {
            let hello = ClientId::Domain("client.example".to_owned());
            let mut transcript = Transcript::new("220 smtp.example\r\n")
                .expect(HELLO, "250-smtp.example\r\n250 AUTH LOGIN\r\n");
            transcript = match stage {
                0 => transcript.expect("AUTH LOGIN\r\n", "235 authenticated\r\n"),
                1 => transcript
                    .expect("AUTH LOGIN\r\n", "334 \r\n")
                    .expect("YWxpY2U=\r\n", "235 authenticated\r\n"),
                _ => transcript
                    .expect("AUTH LOGIN\r\n", "334 \r\n")
                    .expect("YWxpY2U=\r\n", "334 \r\n")
                    .expect("d29uZGVybGFuZA==\r\n", "334 \r\n"),
            };
            let mut connection =
                Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                    .await
                    .unwrap();
            let error = connection
                .auth(
                    &[Mechanism::Login],
                    &Credentials::password("alice", "wonderland"),
                )
                .await
                .unwrap_err();
            assert!(error.is_parse(), "{error:?}");
            assert!(connection.has_broken());
            transcript.assert_exhausted();
        }
    }
}

#[tokio::test]
async fn sasl_oauth_malformed_credentials_write_no_auth_and_keep_connection_usable() {
    for &adapter in ADAPTERS {
        for mechanism in [Mechanism::Xoauth2, Mechanism::OAuthBearer] {
            for (identity, token) in [("u\x01ser", "token"), ("user", "tok\x01en")] {
                let hello = ClientId::Domain("client.example".to_owned());
                let transcript = Transcript::new("220 smtp.example\r\n")
                    .expect(
                        HELLO,
                        format!("250-smtp.example\r\n250 AUTH {mechanism}\r\n"),
                    )
                    .expect("NOOP\r\n", "250 usable\r\n");
                let mut connection =
                    Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                        .await
                        .unwrap();
                let error = connection
                    .auth(&[mechanism], &Credentials::oauth2(identity, token))
                    .await
                    .unwrap_err();
                assert_eq!(
                    error.kind(),
                    &crate::transport::smtp::error::ErrorKind::InvalidInput
                );
                assert_eq!(
                    error.phase(),
                    Some(SmtpCommandPhase::Auth {
                        mechanism: Some(mechanism)
                    })
                );
                assert!(!connection.has_broken());
                connection
                    .command(crate::transport::smtp::commands::Noop)
                    .await
                    .unwrap();
                transcript.assert_exhausted();
            }
        }
    }
}

#[tokio::test]
async fn auth_refreshes_server_info_with_ehlo() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn oauthbearer_auth_sends_initial_response_and_refreshes_ehlo() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn a_server_credential_rejection_names_the_mechanism_in_support_diagnostics() {
    for &adapter in ADAPTERS {
        // Server-rejection naming is identical through both adapters. A 535 says only
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();

        let error = connection
            .auth(
                &[Mechanism::Plain],
                &Credentials::password("user".to_owned(), "pass".to_owned()),
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.phase(),
            Some(SmtpCommandPhase::Auth {
                mechanism: Some(crate::transport::smtp::authentication::Mechanism::Plain)
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
                .any(|text| text.contains("mechanism PLAIN")),
            "support text did not name the refused mechanism: {:?}",
            consented.support_text
        );
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn oauthbearer_immediate_rejection_marks_connection_broken() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!("AUTH OAUTHBEARER {initial}\r\n"),
                "535 rejected\r\n",
            );
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn oauthbearer_failed_challenge_sends_cancel_response() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let initial = crate::base64::encode("n,a=user,\u{1}auth=Bearer token\u{1}\u{1}");
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(format!("AUTH OAUTHBEARER {initial}\r\n"), "334 e30=\r\n")
            // RFC 7628 requires the client to answer a failure challenge with
            // the single `0x01` cancel byte before reading the final reply.
            .expect("AQ==\r\n", "535 rejected\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn starttls_downgrade_is_refused_without_a_wire_command() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();

        assert!(!connection.can_starttls());
        let tls = TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .await
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
            .await
            .unwrap();
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn starttls_refuses_plaintext_bytes_buffered_after_the_reply() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect_coalesced(
                "STARTTLS\r\n",
                "220 go ahead\r\n250 attacker-controlled capabilities\r\n",
            );
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let tls = TlsParameters::new("smtp.example".to_owned()).unwrap();

        let error = connection
            .starttls(&tls, &hello)
            .await
            .expect_err("pre-TLS injected bytes must refuse the upgrade");

        assert!(error.to_string().contains("unsolicited reply"));
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn advertised_starttls_writes_the_command_before_the_handshake() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "220 ready to start tls\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();

        assert!(connection.can_starttls());
        let tls = TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .await
            .expect_err("the in-process transcript stream cannot complete a TLS handshake");
        assert!(
            error.to_string().contains("only supported on TCP"),
            "expected the upgrade to fail at the handshake boundary, got: {error}"
        );
        // The STARTTLS step was consumed, and no post-upgrade EHLO was sent.
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn refused_starttls_reply_does_not_upgrade_the_stream() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 STARTTLS\r\n")
            .expect("STARTTLS\r\n", "454 TLS temporarily unavailable\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();

        let tls = TlsParameters::new("smtp.example".to_owned()).unwrap();
        let error = connection
            .starttls(&tls, &hello)
            .await
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
}

#[tokio::test]
async fn peer_closing_mid_reply_line_is_a_transport_failure() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect_then_close(HELLO, "250-smtp.example\r\n250 PIPELI");

        let Err(error) =
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp).await
        else {
            panic!("a truncated EHLO reply cannot yield a usable connection");
        };
        assert!(
            error.to_string().contains("incomplete SMTP response"),
            "expected an incomplete-response transport error, got: {error}"
        );
        assert!(error.is_network(), "{error:?}");
        assert_eq!(error.phase(), Some(SmtpCommandPhase::Hello));
    }
}

#[tokio::test]
async fn unsolicited_reply_coalesced_with_an_answer_breaks_the_connection() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect_coalesced("NOOP\r\n", "250 noop ok\r\n421 service closing\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
                .await
                .unwrap();

        let error = connection
            .command(crate::transport::smtp::commands::Noop)
            .await
            .expect_err("the surplus reply must break the connection immediately");
        assert!(
            error.to_string().contains("unsolicited reply"),
            "expected a desynchronization error, got: {error}"
        );
        assert!(connection.has_broken());
    }
}

impl Connection {
    async fn send_body(
        &mut self,
        dialect: Dialect,
        bdat: bool,
        envelope: &crate::Envelope,
        body: &[u8],
    ) -> Result<Vec<Response>, Error> {
        let options = crate::transport::smtp::extension::SendOptions::default();
        match self {
            Self::Blocking(connection) => match (dialect, bdat) {
                (Dialect::Smtp, false) => connection
                    .send_with_options(envelope, body, &options)
                    .map(|response| vec![response]),
                (Dialect::Smtp, true) => connection
                    .send_bdat_with_options(envelope, body, &options)
                    .map(|response| vec![response]),
                (Dialect::Lmtp, false) => {
                    connection.send_lmtp_with_options(envelope, body, &options)
                }
                (Dialect::Lmtp, true) => {
                    connection.send_lmtp_bdat_with_options(envelope, body, &options)
                }
            },
            #[cfg(feature = "tokio")]
            Self::Async(connection) => match (dialect, bdat) {
                (Dialect::Smtp, false) => connection
                    .send_with_options(envelope, body, &options)
                    .await
                    .map(|response| vec![response]),
                (Dialect::Smtp, true) => connection
                    .send_bdat_with_options(envelope, body, &options)
                    .await
                    .map(|response| vec![response]),
                (Dialect::Lmtp, false) => {
                    connection
                        .send_lmtp_with_options(envelope, body, &options)
                        .await
                }
                (Dialect::Lmtp, true) => {
                    connection
                        .send_lmtp_bdat_with_options(envelope, body, &options)
                        .await
                }
            },
        }
    }
}

#[tokio::test]
async fn advertised_pipelining_applies_to_both_dialects_and_body_framings() {
    for &adapter in ADAPTERS {
        for dialect in [Dialect::Smtp, Dialect::Lmtp] {
            for bdat in [false, true] {
                let hello = ClientId::Domain("client.example".to_owned());
                let body = b"body\r\n";
                let mut transcript = Transcript::new("220 smtp.example\r\n")
                    .expect(if dialect == Dialect::Smtp { HELLO } else { "LHLO client.example\r\n" }, "250-smtp.example\r\n250-CHUNKING\r\n250 PIPELINING\r\n")
                    .expect("MAIL FROM:<sender@example.com>\r\nRCPT TO:<first@example.com>\r\nRCPT TO:<second@example.com>\r\n", "250 sender\r\n250 first\r\n250 second\r\n");
                transcript = if bdat {
                    transcript.expect("BDAT 6 LAST\r\n", "")
                } else {
                    transcript.expect("DATA\r\n", "354 body\r\n")
                };
                let finals = if dialect == Dialect::Smtp {
                    "250 delivered\r\n"
                } else {
                    "250 delivered-first\r\n550 rejected-second\r\n"
                };
                transcript = if bdat {
                    transcript.expect(body, finals)
                } else {
                    transcript.expect(body, "").expect(".\r\n", finals)
                };
                let envelope = crate::Envelope::new(
                    Some("sender@example.com".parse().unwrap()),
                    vec![
                        "first@example.com".parse().unwrap(),
                        "second@example.com".parse().unwrap(),
                    ],
                )
                .unwrap();
                let mut connection =
                    Connection::from_transcript(adapter, transcript.clone(), &hello, dialect)
                        .await
                        .unwrap();
                let responses = connection
                    .send_body(dialect, bdat, &envelope, body)
                    .await
                    .unwrap();
                assert!(responses[0].has_code(250));
                if dialect == Dialect::Lmtp {
                    assert!(responses[1].has_code(550));
                }
                assert!(!connection.has_broken());
                transcript.assert_exhausted();
            }
        }
    }
}

#[tokio::test]
async fn oauth_token_resolution_uses_each_adapter_once_before_auth() {
    use bifrost_net::{AccessToken, AccountFuture, TokenSource, auth::TokenSnapshot};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Source {
        snapshots: AtomicUsize,
        currents: AtomicUsize,
    }
    impl TokenSource for Source {
        fn snapshot(&self) -> Result<TokenSnapshot, bifrost_net::Error> {
            self.snapshots.fetch_add(1, Ordering::SeqCst);
            Ok(TokenSnapshot::Usable(AccessToken::new("snapshot", None)))
        }
        fn current(&self) -> AccountFuture<Result<AccessToken, bifrost_net::Error>> {
            self.currents.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(AccessToken::new("current", None)) })
        }
        fn refresh(&self) -> AccountFuture<Result<AccessToken, bifrost_net::Error>> {
            panic!("the adapter must delegate refresh to current()")
        }
    }
    for &adapter in ADAPTERS {
        let source = Arc::new(Source {
            snapshots: AtomicUsize::new(0),
            currents: AtomicUsize::new(0),
        });
        let (token, snapshots, currents) = match adapter {
            Adapter::Blocking => ("snapshot", 1, 0),
            #[cfg(feature = "tokio")]
            Adapter::Async => ("current", 0, 1),
        };
        let payload = bifrost_sasl::oauthbearer_payload("user", token);
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 AUTH OAUTHBEARER\r\n")
            .expect(
                format!(
                    "AUTH OAUTHBEARER {}\r\n",
                    crate::base64::encode(payload.as_bytes())
                ),
                "235 authenticated\r\n",
            )
            .expect(HELLO, "250 smtp.example\r\n");
        let hello = ClientId::Domain("client.example".to_owned());
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        connection
            .auth(
                &[Mechanism::OAuthBearer],
                &Credentials::oauth2_source("user", Arc::<Source>::clone(&source)),
            )
            .await
            .unwrap();
        assert_eq!(source.snapshots.load(Ordering::SeqCst), snapshots);
        assert_eq!(source.currents.load(Ordering::SeqCst), currents);
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn peer_eof_carries_transport_and_actual_body_evidence() {
    use crate::transport::smtp::error::SmtpTransmissionState;
    for &adapter in ADAPTERS {
        for body_sent in [false, true] {
            let hello = ClientId::Domain("client.example".to_owned());
            let mut transcript = Transcript::new("220 smtp.example\r\n")
                .expect(HELLO, "250 smtp.example\r\n")
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender\r\n")
                .expect("RCPT TO:<recipient@example.com>\r\n", "250 recipient\r\n");
            transcript = if body_sent {
                transcript
                    .expect("DATA\r\n", "354 body\r\n")
                    .expect("body\r\n", "")
                    .expect_then_close(".\r\n", "250 partially")
            } else {
                transcript.expect_then_close("DATA\r\n", "35")
            };
            let envelope = crate::Envelope::new(
                Some("sender@example.com".parse().unwrap()),
                vec!["recipient@example.com".parse().unwrap()],
            )
            .unwrap();
            let mut connection =
                Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                    .await
                    .unwrap();
            let error = connection
                .send_body(Dialect::Smtp, false, &envelope, b"body\r\n")
                .await
                .unwrap_err();
            assert!(error.is_network(), "{error:?}");
            assert_eq!(
                error.phase(),
                Some(if body_sent {
                    SmtpCommandPhase::DataBody
                } else {
                    SmtpCommandPhase::DataCommand
                })
            );
            assert_eq!(
                error.attempt(),
                Some(if body_sent {
                    SmtpTransmissionState::InFlight
                } else {
                    SmtpTransmissionState::Unsent
                })
            );
            assert!(connection.has_broken());
            transcript.assert_exhausted();
        }
    }
}

#[tokio::test]
async fn batch_mail_construction_failure_is_stamped_before_mail_from() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript =
            Transcript::new("220 smtp.example\r\n").expect(HELLO, "250 smtp.example\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let recipients = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: "recipient@example.com".parse().unwrap(),
        }];

        let Err((error, _progress)) = connection
            .send_smtp_batch(
                Some(Address::new_dangerous("sender\r\nRSET", "example.com")),
                recipients,
                b"body",
                &SendOptions::default(),
            )
            .await
        else {
            panic!("hostile sender should fail MAIL construction");
        };

        assert_eq!(error.attempt(), Some(SmtpTransmissionState::Unsent));
        assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
        assert!(!connection.has_broken(), "validation must precede wire I/O");
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn all_recipient_rejection_resets_every_direct_and_batch_transaction() {
    for &adapter in ADAPTERS {
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
                Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Lmtp)
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

        for protocol in [Dialect::Smtp, Dialect::Lmtp] {
            let hello_command = if protocol == Dialect::Smtp {
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
                Connection::from_transcript(adapter, transcript.clone(), &hello, protocol)
                    .await
                    .unwrap();
            let progress = if protocol == Dialect::Smtp {
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
}

#[tokio::test]
async fn pipelined_server_rejections_carry_their_command_phase() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));

        // RCPT TO rejected.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));

        // DATA rejected before the body.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 PIPELINING\r\n")
            .expect(window, "250 sender ok\r\n250 recipient ok\r\n")
            .expect("DATA\r\n", "554 no data\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(SmtpCommandPhase::DataCommand));
    }
}

/// Finding 2: without PIPELINING, a routine negative reply must be handled
/// exactly as the pipelined path handles it - RSET (or nothing, for a
/// rejected MAIL FROM) and a reusable connection - not `abort()`.
#[tokio::test]
async fn unpipelined_server_rejections_keep_the_connection_reusable() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(SmtpCommandPhase::MailFrom));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();

        // RCPT TO rejected: RSET clears the open transaction.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect(mail, "250 sender ok\r\n")
            .expect(rcpt, "550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.phase(), Some(SmtpCommandPhase::DataCommand));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();

        // The BDAT path shares the same envelope runner.
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250-smtp.example\r\n250 CHUNKING\r\n")
            .expect(mail, "250 sender ok\r\n")
            .expect(rcpt, "550 recipient rejected\r\n")
            .expect("RSET\r\n", "250 reset ok\r\n");
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection
            .send_bdat_with_options(&envelope, b"body", &SendOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.phase(), Some(SmtpCommandPhase::RcptTo));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn pipelined_body_failure_carries_data_body_phase() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
                .await
                .unwrap();

        let error = connection.send(&envelope, b"body").await.unwrap_err();

        assert_eq!(error.phase(), Some(SmtpCommandPhase::DataBody));
    }
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
/// final CRLF. Both adapters must reuse an existing one rather than
/// appending an empty line the sender never wrote, and the chunked writer
/// must carry the last two bytes across iterator items so a CRLF
/// straddling a chunk boundary is still recognized.
#[tokio::test]
async fn data_terminator_preserves_exact_message_bytes() {
    for &adapter in ADAPTERS {
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
                Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        connection
            .message_iter([b"body\r".as_slice(), b"\n".as_slice()].into_iter())
            .await
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Lmtp)
                .await
                .unwrap();

        let outcome = connection
            .send_lmtp_batch(
                Some("sender@example.com".parse().unwrap()),
                batch,
                b"body\r\n",
                &Default::default(),
            )
            .await
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Lmtp)
                .await
                .unwrap();

        let statuses = connection.send_lmtp(&envelope, b"body\r\n").await.unwrap();

        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].is_positive());
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn pipelining_drains_each_recipient_window_before_writing_the_next() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        connection.send(&envelope, b"body").await.unwrap();
        transcript.assert_exhausted();
    }
}

/// A 550 followed by a 421 inside one pipelined RCPT window reports the 550
/// (the first refusal) but still retires the connection, and no RSET is
/// scripted or written into the channel the peer said it is closing. The
/// biting assertion on the exact op stream is
/// `a_421_later_in_an_rcpt_window_aborts_and_reports_the_first_refusal`
/// in the core tests; this one pins the same exchange through the driver.
#[tokio::test]
async fn a_421_after_a_550_in_one_rcpt_window_retires_the_connection() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection.send(&envelope, b"body").await.unwrap_err();

        assert!(
            error
                .smtp_response()
                .is_some_and(|response| response.has_code(550)),
            "the first refusal is the reported one: {error:?}"
        );
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn pipelined_batch_keeps_original_indexes_across_a_window_boundary() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let outcome = connection
            .send_smtp_batch(
                Some("sender@example.com".parse().unwrap()),
                recipients,
                b"body",
                &Default::default(),
            )
            .await
            .unwrap()
            .resolve();

        assert_eq!(outcome.failed().len(), 1);
        assert_eq!(outcome.failed()[0].item.0, "item-31");
        assert_eq!(outcome.succeeded().last().unwrap().item.0, "item-32");
        transcript.assert_exhausted();
    }
}

/// A real PIPELINING peer answers the whole window in one TCP segment, so
/// after the MAIL reply is parsed the buffer still holds every RCPT reply.
/// The window drain must defer the surplus-bytes check to
/// `finish_reply_group`, exactly like the envelope pipelined path; a per-reply surplus check misreads a healthy exchange as an
/// unsolicited reply and fails the batch.
#[tokio::test]
async fn pipelined_batch_drains_coalesced_window_replies() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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

        assert_eq!(outcome.succeeded().len(), 3);
        assert!(outcome.failed().is_empty());
        assert!(outcome.uncertain().is_empty());
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }
}

/// A later recipient window fails to write. `DATA` was never issued, so no
/// content can have reached the peer: every still-open recipient must land
/// in the `failed` lane with `Unsent` evidence, and the RCPT rejection the
/// server already gave must survive. `uncertain` here would be a false
/// claim that the message might have been delivered.
#[tokio::test]
async fn later_window_write_failure_reports_unsent_not_uncertain() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
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

        assert!(
            outcome.uncertain().is_empty(),
            "DATA was never issued, so nothing may be reported as uncertain"
        );
        assert_eq!(outcome.succeeded().len(), 0);
        assert_eq!(outcome.failed().len(), 33);
        assert_eq!(outcome.failed()[31].item.0, "item-31");
    }
}

#[tokio::test]
async fn rcpt_reply_read_failure_reports_unsent_not_uncertain() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn lmtp_drains_one_final_status_per_accepted_recipient() {
    for &adapter in ADAPTERS {
        for second_status in [250, 550] {
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
                    format!("250 first delivered\r\n{second_status} second status\r\n"),
                );
            let envelope =
                Envelope::new(Some("sender@example.com".parse().unwrap()), recipients).unwrap();

            let mut connection =
                Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Lmtp)
                    .await
                    .unwrap();
            let statuses = connection.send_lmtp(&envelope, b"body").await.unwrap();

            assert_eq!(statuses.len(), 2);
            assert!(statuses[0].is_positive());
            assert!(statuses[1].has_code(second_status));
            // Surplus bytes below the read buffer are undetectable without a
            // blocking read, so a completed LMTP drain retires the connection.
            assert!(connection.should_retire());
            assert!(!connection.has_broken());
            transcript.assert_exhausted();
        }
    }
}

#[tokio::test]
async fn lmtp_too_few_final_statuses_breaks_the_connection() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Lmtp)
                .await
                .unwrap();
        assert!(connection.send_lmtp(&envelope, b"body").await.is_err());
        // `has_broken()` after an abort is sharp against the adapter
        // dropping the `Op::Abort` arm entirely, and only that: it does NOT
        // bite against removing `abort()`'s own `set_state(Broken)`, because
        // the shutdown `abort()` also performs sets `Closed`, which fails
        // `verify()` just as well. The same holds for every abort assertion
        // through both adapters.
        assert!(connection.has_broken());
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn lmtp_too_many_final_statuses_breaks_the_connection() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Lmtp)
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
}

#[tokio::test]
async fn lmtp_batch_surplus_final_status_retires_the_connection_and_keeps_outcomes() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Lmtp)
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

        // Both deliveries are known, so the batch result stands; the stream is
        // what cannot be reused.
        assert_eq!(outcome.succeeded().len(), 2);
        assert!(outcome.uncertain().is_empty());
        assert!(connection.has_broken());
        assert!(connection.should_retire());
    }
}

/// `Op::Abort` is a request until the adapter performs it. The core's
/// batch `MAIL FROM` rejection asks for one; what has to be true at the
/// wire afterwards is that the stream can never be handed out again.
#[tokio::test]
async fn an_aborting_batch_failure_leaves_the_stream_broken() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

/// `OpenReplyGroup` holds the stream `Broken` for the whole window and
/// `CloseReplyGroup` restores it. A rejected pipelined `MAIL FROM` is the
/// case that proves the restore happens at the close: the window still
/// drains, no RSET is sent, nothing is aborted, and the connection is
/// reusable at the end even though the send failed.
#[tokio::test]
async fn a_rejected_pipelined_mail_from_restores_the_stream_at_the_group_close() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

/// `CloseLmtpDrain { restore_ok: false }` is the batch LMTP drain, and the
/// `false` has to reach the wire: unlike a direct LMTP send, a clean batch
/// drain leaves the stream `Broken` as well as retired.
#[tokio::test]
async fn a_clean_lmtp_batch_drain_leaves_the_stream_broken_and_retired() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Lmtp)
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
}

#[tokio::test]
async fn send_with_options_pipelines_envelope_parameters_in_one_window() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let response = connection
            .send_with_options(&envelope, b"Subject: test\r\n\r\nHello", &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn pipelined_send_rsets_when_a_recipient_is_rejected() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn explicit_mail_parameters_are_not_duplicated() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let response = connection
            .send_with_options(&envelope, "Subject: test\r\n\r\nHéllo".as_bytes(), &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }
}

#[tokio::test]
async fn batch_eai_recipient_requires_smtputf8_before_mail_from() {
    for &adapter in ADAPTERS {
        let hello = ClientId::Domain("client.example".to_owned());
        let transcript = Transcript::new("220 smtp.example\r\n")
            .expect(HELLO, "250 smtp.example\r\n")
            .expect("NOOP\r\n", "250 noop ok\r\n");
        let batch = vec![SmtpBatchRecipient {
            id: BatchItemId("item-0".to_owned()),
            address: crate::address::Address::new_dangerous("üser", "example.com"),
        }];
        let mut connection =
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn hostile_unchecked_sender_never_reaches_the_wire() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn hostile_unchecked_recipient_leaves_no_open_transaction() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn send_bdat_uses_chunking_without_dot_stuffing() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let response = connection
            .send_bdat_with_options(&envelope, message, &options)
            .await
            .unwrap();

        assert!(response.has_code(250));
        transcript.assert_exhausted();
    }
}

/// The peer closes right after accepting `DATA`. The body write fails, so
/// the message may or may not have been seen: every recipient must land in
/// the `uncertain` lane, never `succeeded`.
#[tokio::test]
async fn peer_closing_after_the_data_command_leaves_recipients_uncertain() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
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
            .expect("a mid-body drop is a per-recipient outcome, not a batch-level error")
            .resolve();

        assert!(outcome.succeeded().is_empty());
        assert_eq!(outcome.uncertain().len(), 1);
        assert_eq!(outcome.uncertain()[0].item.0, "item-0");
        assert!(connection.has_broken());
    }
}

/// DSN RCPT parameters are refused before `MAIL FROM` when the server did
/// not advertise DSN. The refusal must be local: no envelope byte may hit
/// the wire, so the transaction can be retried on the same connection.
#[tokio::test]
async fn dsn_rcpt_parameters_are_refused_before_any_envelope_byte() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();
        let error = connection
            .send_with_options(&envelope, b"body", &options)
            .await
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
            .await
            .unwrap();
        transcript.assert_exhausted();
    }
}

/// Recipient-specific parameters are keyed by address. A parameter naming
/// an address that is not in the batch is a caller bug and must be caught
/// before `MAIL FROM`, never silently dropped.
#[tokio::test]
async fn batch_rcpt_parameters_for_an_unknown_recipient_fail_before_mail_from() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
            error
                .to_string()
                .contains("do not match an envelope recipient"),
            "expected an unmatched-recipient refusal, got: {error}"
        );
        let outcome = progress.resolve();
        assert!(outcome.succeeded().is_empty());

        connection
            .command(crate::transport::smtp::commands::Noop)
            .await
            .unwrap();
        transcript.assert_exhausted();
    }
}

/// Per-recipient DSN parameters override the uniform ones on the matching
/// RCPT line only, and the batch path emits the same wire shape as the
/// envelope path. Sequential (non-PIPELINING) peer, so each RCPT line is
/// pinned as its own write.
#[tokio::test]
async fn batch_rcpt_options_are_emitted_per_recipient_in_order() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

/// A negative reply to the end-of-data terminator is an ANSWER, not a
/// transport failure: the server read the whole message and refused it, so
/// the transaction is complete (RFC 5321 4.1.1.4), every accepted
/// recipient is a `failed` lane classified under `DataFinal`, and the
/// connection stays reusable. Routing it through the `Failed` arm put the
/// recipients in `uncertain` (asking the caller to reconcile a delivery the
/// server explicitly refused) and threw away a healthy connection.
#[tokio::test]
async fn a_rejected_data_final_reply_fails_the_recipients_and_keeps_the_connection() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

/// The direct path takes the same rule: a refused message on an answered
/// transaction is a status error on a reusable connection, not an abort.
#[tokio::test]
async fn a_rejected_data_final_reply_keeps_the_direct_connection_reusable() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();

        let error = connection.send(&envelope, b"body").await.unwrap_err();
        assert_eq!(error.status().map(u16::from), Some(554));
        assert_eq!(error.phase(), Some(SmtpCommandPhase::DataFinal));
        assert!(!connection.has_broken());
        transcript.assert_exhausted();
    }
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
#[tokio::test]
async fn a_rejected_bdat_last_resets_the_transaction_before_the_connection_is_reused() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
                .await
                .unwrap();

        let error = connection
            .send_bdat_with_options(&envelope, b"body", &SendOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.status().map(u16::from), Some(452));
        assert_eq!(error.phase(), Some(SmtpCommandPhase::BdatBody));
        assert!(
            !connection.has_broken(),
            "the peer acknowledged the reset, so the connection is reusable"
        );
        transcript.assert_exhausted();
    }
}

/// A rejected end-of-data reply keeps the connection - unless the peer
/// said it is going away. 421 is "closing transmission channel", so
/// parking it would hand the next checkout a dead socket. The status
/// error is still what the caller sees.
#[tokio::test]
async fn a_421_data_final_reply_aborts_instead_of_parking_a_dying_connection() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

/// The batch machine takes the same 421 rule, and it is the path that
/// matters most: `send_smtp_batch` is the account-level send, so a
/// connection parked after the peer announced it is closing goes back
/// into the pool, and the next checkout under `test_on_checkout(false)`
/// writes `MAIL FROM` into a dead socket. The lanes still resolve from
/// the answer - `DataFinal` `failed`, never `uncertain` - only the
/// connection is retired.
#[tokio::test]
async fn a_421_data_final_reply_aborts_the_batch_connection_too() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript.clone(), &hello, Dialect::Smtp)
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
}

#[tokio::test]
async fn pipelined_rcpt_reply_read_failure_reports_unsent_not_uncertain() {
    for &adapter in ADAPTERS {
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
            Connection::from_transcript(adapter, transcript, &hello, Dialect::Smtp)
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
}
