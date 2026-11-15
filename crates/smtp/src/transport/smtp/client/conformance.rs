//! Every session transcript is defined once and driven through both adapters.

#[cfg(feature = "tokio")]
use super::async_connection::AsyncSmtpConnection;
use super::{TlsParameters, connection::SmtpConnection};
use crate::transport::smtp::{
    Dialect,
    account_error::{SmtpErrorContext, into_account_error},
    authentication::{Credentials, Mechanism},
    error::{Error, SmtpCommandPhase},
    extension::{ClientId, Extension, ServerInfo},
    response::Response,
    test_support::{Transcript, scram_enhanced_success_reply, scram_server_first_reply},
};
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
