#[cfg(test)]
use super::Dialect;
#[cfg(all(test, feature = "account-error"))]
use super::pool::async_impl::Pool;
#[cfg(all(test, feature = "account-error"))]
use super::surface::{AsyncLmtpTransport, AsyncSmtpTransport};
use super::{AsyncSmtpConnection, Error, SmtpInfo};
#[cfg(all(test, feature = "account-error"))]
use super::{ClientId, PoolConfig, SendOptions};
use crate::transport::smtp::error::SmtpCommandPhase;
use std::fmt::{self, Debug};

/// Build client
pub(super) struct AsyncSmtpClient {
    pub(super) info: SmtpInfo,
}

impl AsyncSmtpClient {
    #[cfg(test)]
    pub(super) fn for_pool_test() -> Self {
        Self {
            info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
        }
    }

    /// Creates a new connection directly usable to send emails
    ///
    /// Handles encryption and authentication
    pub(super) async fn connection(&self) -> Result<AsyncSmtpConnection, Error> {
        let mut conn = super::dial::connect_async(&self.info).await?;

        self.authenticate_if_configured(&mut conn).await?;
        Ok(conn)
    }

    /// The post-greeting authentication stage.
    ///
    /// Factored out of `connection()` so it can be driven against a scripted
    /// in-memory connection: the refusal it enforces (no AUTH over an
    /// unencrypted link unless the caller opted in) is a decision made after
    /// the greeting and EHLO, and has nothing to do with how the socket was
    /// dialled.
    pub(super) async fn authenticate_if_configured(
        &self,
        conn: &mut AsyncSmtpConnection,
    ) -> Result<(), Error> {
        if let Some(credentials) = &self.info.credentials {
            if let Err(error) = self.info.ensure_can_authenticate(conn.is_encrypted()) {
                conn.abort().await;
                // Refused before any rung was picked, so there is no
                // mechanism to name here.
                return Err(error.with_phase(SmtpCommandPhase::Auth { mechanism: None }));
            }
            conn.auth(self.info.authentication(), credentials).await?;
        }
        Ok(())
    }
}

impl Debug for AsyncSmtpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("AsyncSmtpClient");
        builder.field("info", &self.info);
        builder.finish()
    }
}

#[cfg(all(test, feature = "account-error"))]
mod tests {
    use crate::{
        AsyncLmtpTransport, AsyncSmtpTransport,
        transport::smtp::{
            authentication::{
                Credentials, DEFAULT_MECHANISMS, Mechanism, OAUTH2_MECHANISMS, PASSWORD_MECHANISMS,
            },
            client::Tls,
            test_support::Transcript,
        },
    };

    use super::{AsyncSmtpClient, AsyncSmtpConnection, ClientId, Dialect};

    #[test]
    fn tokio_transport_from_plaintext_url() {
        let builder = AsyncSmtpTransport::from_url("smtp://127.0.0.1:2525").unwrap();

        assert_eq!(builder.info.port, 2525);
        assert_eq!(builder.info.server, "127.0.0.1");
    }

    #[test]
    fn tokio_lmtp_builder_uses_lmtp_defaults() {
        let builder = AsyncLmtpTransport::builder_dangerous("localhost");

        assert_eq!(builder.info.port, super::super::LMTP_PORT);
        assert_eq!(builder.info.protocol, Dialect::Lmtp);

        #[cfg(unix)]
        {
            // Explicit TCP configuration must replace a previous local route
            // with identical behavior in the blocking and Tokio builders.
            let socket = AsyncLmtpTransport::unix_socket("/run/lmtp.sock");
            assert!(socket.clone().tls(Tls::None).info.unix_socket.is_none());
            assert!(socket.port(2524).info.unix_socket.is_none());
        }
    }

    /// URL parsing decides the port, the TLS posture and the credentials, and
    /// percent-decodes both halves of the userinfo. Getting any of those wrong
    /// silently downgrades or misauthenticates the connection.
    #[test]
    fn tokio_transport_from_tls_url() {
        let builder =
            AsyncSmtpTransport::from_url("smtp://127.0.0.1:2525").expect("a plaintext URL parses");
        assert!(matches!(builder.info.tls, Tls::None));

        let builder =
            AsyncSmtpTransport::from_url("smtps://username:password@smtp.example.com:465")
                .expect("an implicit-TLS URL parses");
        assert_eq!(builder.info.port, 465);
        assert_eq!(builder.info.server, "smtp.example.com");
        assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::Password { ref username, ref password })
                if username == "username" && password.as_str() == "password"
        ));

        let builder = AsyncSmtpTransport::from_url(
            "smtps://user%40example.com:pa$$word%3F%22!@smtp.example.com:465",
        )
        .expect("percent-encoded userinfo parses");
        assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::Password { ref username, ref password })
                if username == "user@example.com" && password.as_str() == "pa$$word?\"!"
        ));

        let builder = AsyncSmtpTransport::from_url(
            "smtp://username:password@smtp.example.com:587?tls=required",
        )
        .expect("an explicit STARTTLS URL parses");
        assert_eq!(builder.info.port, 587);
        assert!(matches!(builder.info.tls, Tls::Required(_)));
    }

    /// The `password` helper picks the password mechanism ladder, strongest
    /// first. SCRAM is offered out of the box; LOGIN is opt-in only. A silent
    /// reordering here is a downgrade.
    #[test]
    fn tokio_password_helper_uses_password_mechanisms() {
        let builder = AsyncSmtpTransport::builder_dangerous("smtp.example.com")
            .password("username", "password");

        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::Password { ref username, ref password })
                if username == "username" && password.as_str() == "password"
        ));
        assert_eq!(builder.info.authentication(), PASSWORD_MECHANISMS);
        assert_eq!(
            builder.info.authentication(),
            vec![
                Mechanism::ScramSha256Plus,
                Mechanism::ScramSha1Plus,
                Mechanism::ScramSha256,
                Mechanism::ScramSha1,
                Mechanism::Plain,
            ]
        );
        assert_eq!(DEFAULT_MECHANISMS, PASSWORD_MECHANISMS);
    }

    /// OAUTHBEARER (RFC 7628) outranks the legacy XOAUTH2 draft.
    #[test]
    fn tokio_oauth2_helper_prefers_standard_bearer_mechanism() {
        let builder = AsyncSmtpTransport::builder_dangerous("smtp.example.com")
            .oauth2("user@example.com", "token");

        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::OAuth2 { ref identity, .. })
                if identity == "user@example.com"
        ));
        assert_eq!(builder.info.authentication(), OAUTH2_MECHANISMS);
        assert_eq!(
            builder.info.authentication(),
            [Mechanism::OAuthBearer, Mechanism::Xoauth2]
        );
    }

    /// Setting credentials must not clobber an explicit mechanism list. A
    /// caller who narrowed the ladder deliberately gets to keep it.
    #[test]
    fn tokio_credentials_preserve_explicit_authentication_mechanisms() {
        let builder = AsyncSmtpTransport::builder_dangerous("smtp.example.com")
            .authentication(vec![Mechanism::Xoauth2])
            .oauth2("user@example.com", "token");
        assert_eq!(builder.info.authentication(), [Mechanism::Xoauth2]);

        let builder = AsyncSmtpTransport::builder_dangerous("smtp.example.com")
            .authentication(vec![Mechanism::Plain])
            .password("username", "password");
        assert_eq!(builder.info.authentication(), [Mechanism::Plain]);
    }

    /// The insecure-auth escape hatch is a builder flag, and it is the only
    /// thing that may relax the refusal. Both directions are pinned so the
    /// flag cannot rot into a no-op (silently breaking local relays) or into
    /// a default (silently sending credentials in the clear).
    #[test]
    fn tokio_dangerous_allow_insecure_auth_is_the_only_plaintext_escape_hatch() {
        let guarded =
            AsyncSmtpTransport::builder_dangerous("smtp.example.com").password("user", "pass");
        assert!(!guarded.info.allow_insecure_auth);
        let error = guarded
            .info
            .ensure_can_authenticate(false)
            .expect_err("plaintext AUTH is refused by default");
        assert!(
            error
                .to_string()
                .contains("refusing to authenticate over an unencrypted SMTP connection"),
            "expected a plaintext-auth policy refusal, got: {error}"
        );
        guarded
            .info
            .ensure_can_authenticate(true)
            .expect("an encrypted connection may authenticate");

        let opted_in = AsyncSmtpTransport::builder_dangerous("smtp.example.com")
            .password("user", "pass")
            .dangerous_allow_insecure_auth(true);
        assert!(opted_in.info.allow_insecure_auth);
        opted_in
            .info
            .ensure_can_authenticate(false)
            .expect("the opt-in escape hatch permits plaintext AUTH");
    }

    /// LMTP names itself in the refusal, so an operator reading a log knows
    /// which transport refused.
    #[test]
    fn tokio_plaintext_auth_refusal_names_the_lmtp_protocol() {
        let builder = AsyncLmtpTransport::builder_dangerous("localhost").password("user", "pass");

        let error = builder
            .info
            .ensure_can_authenticate(false)
            .expect_err("plaintext AUTH is refused for LMTP too");
        assert!(
            error
                .to_string()
                .contains("refusing to authenticate over an unencrypted LMTP connection"),
            "expected an LMTP-labelled refusal, got: {error}"
        );
    }

    /// Finding 4c: ported off the loopback listener, the async mirror of the
    /// blocking pin. The transcript scripts the greeting and the `AUTH
    /// PLAIN`-advertising EHLO reply and NOTHING else, so an AUTH command
    /// reaching the wire fails the write outright instead of producing the
    /// policy error - the same observable the old listener's zero-length read
    /// gave, without a socket.
    #[tokio::test(crate = "tokio")]
    async fn tokio_plaintext_auth_is_refused_before_auth_command() {
        let transcript = Transcript::new("220 localhost\r\n").expect(
            "EHLO client.example\r\n",
            "250-localhost\r\n250 AUTH PLAIN\r\n",
        );
        let mut conn = AsyncSmtpConnection::from_transcript(
            transcript.clone(),
            &ClientId::Domain("client.example".to_owned()),
            Dialect::Smtp,
        )
        .await
        .unwrap();

        let info = AsyncSmtpTransport::builder_dangerous("127.0.0.1")
            .password("user", "pass")
            .info;
        let client = AsyncSmtpClient { info };
        let Err(error) = client.authenticate_if_configured(&mut conn).await else {
            panic!("plaintext auth must be refused");
        };

        assert!(error.is_policy(), "expected policy error, got {error:?}");
        transcript.assert_exhausted();
    }

    /// Finding 4c: the async escape-hatch mirror. The transcript DOES script
    /// an `AUTH PLAIN` step here, so the exact base64 credential line the
    /// driver puts on the wire is asserted by the transcript itself.
    #[tokio::test(crate = "tokio")]
    async fn tokio_dangerous_allow_insecure_auth_sends_auth_plain_on_a_plaintext_connection() {
        let transcript = Transcript::new("220 localhost\r\n")
            .expect(
                "EHLO client.example\r\n",
                "250-localhost\r\n250 AUTH PLAIN\r\n",
            )
            .expect("AUTH PLAIN AHVzZXIAcGFzcw==\r\n", "235 authenticated\r\n")
            // A successful AUTH re-issues EHLO: the server's capability list
            // may change once the session is authenticated.
            .expect("EHLO client.example\r\n", "250 localhost\r\n");
        let mut conn = AsyncSmtpConnection::from_transcript(
            transcript.clone(),
            &ClientId::Domain("client.example".to_owned()),
            Dialect::Smtp,
        )
        .await
        .unwrap();

        let info = AsyncSmtpTransport::builder_dangerous("127.0.0.1")
            .password("user", "pass")
            .dangerous_allow_insecure_auth(true)
            .authentication(vec![Mechanism::Plain])
            .info;
        let client = AsyncSmtpClient { info };
        client
            .authenticate_if_configured(&mut conn)
            .await
            .expect("the escape hatch permits plaintext AUTH");

        transcript.assert_exhausted();
    }

    mod pooled_batch {
        use std::sync::Arc;

        use bifrost_types::error::{AccountErrorKind, BatchItem, BatchItemId, RequestErrorKind};

        use crate::transport::smtp::test_support::Transcript;

        use super::super::{
            AsyncLmtpTransport, AsyncSmtpClient, AsyncSmtpConnection, AsyncSmtpTransport, ClientId,
            Dialect, Pool, PoolConfig, SendOptions, SmtpInfo,
        };

        fn hello() -> ClientId {
            ClientId::Domain("client.example".to_owned())
        }

        /// A pool that will never dial: the only connection it can ever hand
        /// out is the scripted one parked here.
        async fn pool_with(conn: AsyncSmtpConnection, protocol: Dialect) -> Arc<Pool> {
            let pool = Pool::new(
                // The checkout probe would need its own scripted NOOP step;
                // the probe itself is pinned at the connection level.
                PoolConfig::new().test_on_checkout(false),
                AsyncSmtpClient {
                    info: SmtpInfo::new("transcript.invalid", protocol),
                },
            );
            pool.park_for_test(conn).await;
            pool
        }

        fn batch(addresses: &[&str]) -> Vec<BatchItem<crate::address::Address>> {
            addresses
                .iter()
                .enumerate()
                .map(|(index, address)| {
                    BatchItem::new(
                        BatchItemId(format!("item-{index}")),
                        address.parse().unwrap(),
                    )
                })
                .collect()
        }

        /// Finding 4b: the async transport-level LMTP delivery pin, ported off
        /// the `TcpListener`/`UnixListener` scripted servers it used to run
        /// against. The transcript asserts the exact command sequence the old
        /// `assert_lmtp_delivery_commands` checked, and the returned statuses
        /// pin that a mid-envelope RCPT rejection is re-inserted in the
        /// original recipient order alongside the two real final statuses.
        #[tokio::test(crate = "tokio")]
        async fn tokio_lmtp_transport_returns_per_recipient_statuses() {
            use crate::AsyncTransport;
            use crate::address::Envelope;

            let transcript = Transcript::new("220 localhost\r\n")
                .expect(
                    "LHLO client.example\r\n",
                    "250-localhost\r\n250 8BITMIME\r\n",
                )
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<first@example.com>\r\n", "250 rcpt ok\r\n")
                .expect("RCPT TO:<second@example.com>\r\n", "550 rcpt rejected\r\n")
                .expect("RCPT TO:<third@example.com>\r\n", "250 rcpt ok\r\n")
                .expect("DATA\r\n", "354 send message\r\n")
                .expect("Subject: test\r\n\r\nHello", "")
                .expect(
                    "\r\n.\r\n",
                    "250 first recipient ok\r\n451 third recipient deferred\r\n",
                );
            let conn =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Lmtp)
                    .await
                    .unwrap();
            let transport = AsyncLmtpTransport {
                marker_: std::marker::PhantomData,
                inner: pool_with(conn, Dialect::Lmtp).await,
            };

            let envelope = Envelope::new(
                Some("sender@example.com".parse().unwrap()),
                vec![
                    "first@example.com".parse().unwrap(),
                    "second@example.com".parse().unwrap(),
                    "third@example.com".parse().unwrap(),
                ],
            )
            .unwrap();

            let responses = transport
                .send_raw(&envelope, b"Subject: test\r\n\r\nHello")
                .await
                .unwrap();

            assert_eq!(responses.len(), 3);
            assert!(responses[0].has_code(250));
            assert!(responses[1].has_code(550));
            assert!(!responses[1].is_positive());
            assert!(responses[2].has_code(451));
            assert!(!responses[2].is_positive());
            transcript.assert_exhausted();
        }

        /// Finding 4b: what the old `UnixListener` test could actually pin
        /// in-process is the routing decision, not the kernel's socket. The
        /// delivery behaviour over a Unix-domain LMTP socket is identical to
        /// the TCP case above (same connection driver), so what is left to pin
        /// is that the builder routes to the Unix funnel at all and that the
        /// documented TLS-over-Unix rejection fires before any dial.
        #[tokio::test(crate = "tokio")]
        #[cfg(unix)]
        async fn tokio_lmtp_unix_socket_builder_routes_to_the_unix_funnel() {
            use crate::transport::smtp::client::{Tls, TlsParametersBuilder};

            let builder = AsyncLmtpTransport::unix_socket("/run/lmtp.sock");
            assert_eq!(
                builder.info.unix_socket.as_deref(),
                Some(std::path::Path::new("/run/lmtp.sock"))
            );
            assert_eq!(builder.info.protocol, Dialect::Lmtp);

            let mut info = SmtpInfo::new("localhost", Dialect::Lmtp);
            info.unix_socket = Some(std::path::PathBuf::from("/run/lmtp.sock"));
            info.tls = Tls::Wrapper(
                TlsParametersBuilder::new("localhost".to_owned())
                    .build()
                    .unwrap(),
            );
            let client = AsyncSmtpClient { info };
            let error = client
                .connection()
                .await
                .err()
                .expect("TLS over a Unix socket is refused");
            assert!(
                error.to_string().contains("Unix-domain LMTP sockets"),
                "got {error}"
            );
        }

        /// End-to-end counterpart to the connection-level `should_retire()`
        /// pin, on the async path: a completed LMTP delivery must leave the
        /// pool empty.
        #[tokio::test(crate = "tokio")]
        async fn tokio_lmtp_batch_send_leaves_no_pooled_connection_behind() {
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
            let conn =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Lmtp)
                    .await
                    .unwrap();
            let pool = pool_with(conn, Dialect::Lmtp).await;
            let transport = AsyncLmtpTransport {
                marker_: std::marker::PhantomData,
                inner: Arc::clone(&pool),
            };

            let outcome = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    batch(&["first@example.com", "second@example.com"]),
                    b"body",
                    &SendOptions::default(),
                )
                .await
                .expect("the delivery completes with per-recipient outcomes");
            assert_eq!(outcome.succeeded().len(), 1);
            assert_eq!(outcome.failed().len(), 1);
            assert_eq!(
                pool.idle_count_for_test(),
                0,
                "a drained LMTP connection must be retired, not parked"
            );
            transcript.assert_exhausted();
        }

        use std::sync::atomic::{AtomicU64, Ordering};

        use crate::transport::smtp::WireMetering;

        /// Records every byte the transport reads and writes, per account.
        #[derive(Default)]
        struct CountingSink {
            bytes_in: AtomicU64,
            bytes_out: AtomicU64,
        }

        impl bifrost_net::MeterSink for CountingSink {
            fn record_bytes_in(&self, _account: &bifrost_net::AccountId, n: u64) {
                self.bytes_in.fetch_add(n, Ordering::Relaxed);
            }
            fn record_bytes_out(&self, _account: &bifrost_net::AccountId, n: u64) {
                self.bytes_out.fetch_add(n, Ordering::Relaxed);
            }
        }

        /// smtp-M1. SMTP was the one protocol whose bytes reached the wire
        /// unmetered, which made an account's bandwidth cap silently
        /// PARTIAL: honoured on its other traffic, ignored on submission -
        /// the upstream-heavy path a cap usually exists to protect.
        ///
        /// Drives a real send through the transport and asserts the sink
        /// saw traffic in both directions, rather than asserting the
        /// adapter was merely installed.
        #[tokio::test(crate = "tokio")]
        async fn a_metered_transport_reports_its_socket_bytes_to_the_sink() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
                .expect("DATA\r\n", "354 send body\r\n")
                .expect("body", "")
                .expect("\r\n.\r\n", "250 queued\r\n");

            let sink = Arc::new(CountingSink::default());
            let erased: Arc<dyn bifrost_net::MeterSink> = Arc::<CountingSink>::clone(&sink);
            let handle = bifrost_net::MeterSinkHandle::new(
                erased,
                bifrost_net::AccountId("metered".to_owned()),
            );
            let conn = AsyncSmtpConnection::from_transcript_metered(
                transcript.clone(),
                &hello(),
                Dialect::Smtp,
                WireMetering::new(Some(handle), None),
            )
            .await
            .unwrap();
            let pool = pool_with(conn, Dialect::Smtp).await;
            let transport = AsyncSmtpTransport {
                marker_: std::marker::PhantomData,
                inner: Arc::clone(&pool),
            };

            let outcome = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    batch(&["first@example.com"]),
                    b"body",
                    &SendOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(outcome.succeeded().len(), 1);

            let sent = sink.bytes_out.load(Ordering::Relaxed);
            let received = sink.bytes_in.load(Ordering::Relaxed);
            assert!(
                sent > 0,
                "the commands and body the transport wrote must be metered"
            );
            assert!(
                received > 0,
                "the server's replies must be metered too - a cap that\
                 counted only one direction would misreport usage"
            );
        }

        #[tokio::test(start_paused = true)]
        async fn async_lmtp_builder_applies_bandwidth_metering() {
            use crate::transport::smtp::client::AsyncNetworkStream;
            use crate::transport::smtp::test_support::SlowSinkPeer;
            use tokio::io::AsyncWriteExt;

            let sink = Arc::new(CountingSink::default());
            let erased: Arc<dyn bifrost_net::MeterSink> = Arc::<CountingSink>::clone(&sink);
            let handle =
                bifrost_net::MeterSinkHandle::new(erased, bifrost_net::AccountId("lmtp".into()));
            let budget = bifrost_net::BandwidthBudget::new(Some(10));
            let builder = AsyncLmtpTransport::builder_dangerous("lmtp.invalid")
                .bandwidth_metering(Some(handle), Some(budget.clone()));
            // Use the exact configuration the builder hands to its connector.
            let mut stream = AsyncNetworkStream::from_raw_stream_for_test(Box::new(
                SlowSinkPeer::new(usize::MAX, std::time::Duration::ZERO),
            ));
            stream.set_metering(builder.info.metering.clone());
            let started = tokio::time::Instant::now();
            assert_eq!(stream.write(&[0; 20]).await.unwrap(), 10);
            assert_eq!(stream.write(&[0; 20]).await.unwrap(), 10);
            assert_eq!(started.elapsed(), std::time::Duration::from_secs(1));
            assert_eq!(sink.bytes_out.load(Ordering::Relaxed), 20);
            budget.set_cap(None);
            assert_eq!(stream.write(&[0; 20]).await.unwrap(), 20);
        }

        /// The default configuration has no sink and an unlimited budget.
        #[tokio::test(crate = "tokio")]
        async fn an_unmetered_transport_is_the_default() {
            let info = SmtpInfo::new("transcript.invalid", Dialect::Smtp);
            assert!(
                !info.metering.is_enabled(),
                "metering is opt-in; a plain transport must not meter"
            );
        }

        /// The SMTP contrast case on the async path: the connection goes back
        /// to the pool and the next batch reuses it without a reconnect.
        #[tokio::test(crate = "tokio")]
        async fn tokio_smtp_batch_send_recycles_and_reuses_its_pooled_connection() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
                .expect("DATA\r\n", "354 send body\r\n")
                .expect("body", "")
                .expect("\r\n.\r\n", "250 queued\r\n")
                // Second transaction: no greeting, no EHLO. A reconnect would
                // have to write EHLO here and fail the transcript.
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
                .expect("DATA\r\n", "354 send body\r\n")
                .expect("body", "")
                .expect("\r\n.\r\n", "250 queued\r\n");
            let conn =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp)
                    .await
                    .unwrap();
            let pool = pool_with(conn, Dialect::Smtp).await;
            let transport = AsyncSmtpTransport {
                marker_: std::marker::PhantomData,
                inner: Arc::clone(&pool),
            };

            let first = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    batch(&["first@example.com"]),
                    b"body",
                    &SendOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(first.succeeded().len(), 1);
            assert_eq!(
                pool.idle_count_for_test(),
                1,
                "a healthy SMTP connection must be recycled"
            );

            let second = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    batch(&["second@example.com"]),
                    b"body",
                    &SendOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(second.succeeded().len(), 1);
            assert_eq!(pool.idle_count_for_test(), 1);
            transcript.assert_exhausted();
        }

        #[tokio::test(crate = "tokio")]
        async fn pool_max_size_bounds_checked_out_connections() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n");
            let conn =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp)
                    .await
                    .unwrap();
            let pool = Pool::new(
                PoolConfig::new().max_size(1).test_on_checkout(false),
                AsyncSmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
                },
            );
            pool.park_for_test(conn).await;

            let first = pool.connection().await.unwrap();
            let mut second = Box::pin(pool.connection());
            std::future::poll_fn(|cx| {
                assert!(
                    std::future::Future::poll(second.as_mut(), cx).is_pending(),
                    "a second checkout must wait while the only slot is checked out"
                );
                std::task::Poll::Ready(())
            })
            .await;

            drop(first);
            let second = second.await.expect("released slot wakes the next checkout");
            drop(second);
            assert_eq!(pool.idle_count_for_test(), 1);
            transcript.assert_exhausted();
        }

        #[tokio::test(crate = "tokio", start_paused = true)]
        async fn cancelled_shutdown_and_concurrent_shutdown_wait_for_idle_abort() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
                .stall_shutdown();
            let conn =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp)
                    .await
                    .unwrap();
            let pool = Pool::new(
                PoolConfig::new().max_size(1).test_on_checkout(false),
                AsyncSmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
                },
            );
            pool.park_for_test(conn).await;
            let mut first = Box::pin(pool.shutdown());
            assert!(futures::poll!(first.as_mut()).is_pending());
            tokio::task::yield_now().await;
            let mut second = Box::pin(pool.shutdown());
            assert!(futures::poll!(second.as_mut()).is_pending());
            drop(first);
            assert!(
                pool.connection()
                    .await
                    .err()
                    .expect("closed pool")
                    .is_transport_shutdown()
            );
            // Virtual time advances to the abort ceiling; the second caller
            // cannot report success merely because the idle list was taken.
            second.await;
            pool.shutdown().await;
            transcript.assert_exhausted();
        }

        /// `max_size` is enforced by a semaphore, so a checkout blocked on
        /// admission is only reachable through that semaphore. `shutdown()`
        /// must therefore close it: otherwise the waiter parks forever, and if
        /// the checked-out connection is ever returned its released permit
        /// hands the waiter a slot to dial a NEW connection through a pool that
        /// is already closed. `transcript.invalid` does not resolve, so a dial
        /// would surface as a connection error rather than a shutdown error -
        /// which is what makes the assertion below discriminating.
        #[tokio::test(crate = "tokio")]
        async fn shutdown_wakes_a_blocked_checkout_instead_of_letting_it_dial() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n");
            let conn =
                AsyncSmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp)
                    .await
                    .unwrap();
            let pool = Pool::new(
                PoolConfig::new().max_size(1).test_on_checkout(false),
                AsyncSmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
                },
            );
            pool.park_for_test(conn).await;

            let first = pool.connection().await.unwrap();
            let mut second = Box::pin(pool.connection());
            std::future::poll_fn(|cx| {
                assert!(
                    std::future::Future::poll(second.as_mut(), cx).is_pending(),
                    "a second checkout must wait while the only slot is checked out"
                );
                std::task::Poll::Ready(())
            })
            .await;

            // `first` is deliberately still held. The only permit is checked
            // out, so nothing will ever release it: if `shutdown()` does not
            // close the admission semaphore, the waiter below stays parked
            // forever. Polling to Ready rather than awaiting is what turns that
            // hang into a deterministic failure.
            pool.shutdown().await;

            let outcome = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(second.as_mut(), cx))
            })
            .await;
            let std::task::Poll::Ready(result) = outcome else {
                panic!("shutdown must wake a checkout parked on admission");
            };
            let error = result
                .err()
                .expect("a blocked checkout must fail once the pool is shut down");
            assert!(
                matches!(
                    error.kind(),
                    crate::transport::smtp::error::ErrorKind::TransportShutdown
                ),
                "expected a shutdown error, got: {error}"
            );

            // Returning the checked-out connection after shutdown releases its
            // permit. That must not resurrect the pool.
            drop(first);
            assert_eq!(pool.idle_count_for_test(), 0);
            transcript.assert_exhausted();
        }

        /// Batch input is validated before a connection is taken out of the
        /// pool. Duplicate ids would make the per-item outcome lanes
        /// ambiguous, and discovering that after checkout would burn a
        /// connection (or, worse, open a transaction) over a caller bug. The
        /// pool here is shut down, so any checkout attempt fails loudly with
        /// a different error than the one asserted.
        #[tokio::test(crate = "tokio")]
        async fn invalid_batch_input_is_rejected_before_pool_checkout() {
            let pool = Pool::new(
                PoolConfig::new(),
                AsyncSmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
                },
            );
            pool.shutdown().await;
            let transport = AsyncSmtpTransport {
                marker_: std::marker::PhantomData,
                inner: Arc::clone(&pool),
            };
            let duplicated = vec![
                BatchItem::new(
                    BatchItemId("dup".to_owned()),
                    "first@example.com".parse().unwrap(),
                ),
                BatchItem::new(
                    BatchItemId("dup".to_owned()),
                    "second@example.com".parse().unwrap(),
                ),
            ];

            let error = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    duplicated,
                    b"body",
                    &SendOptions::default(),
                )
                .await
                .expect_err("duplicate batch ids are refused");

            assert!(
                matches!(
                    error.kind(),
                    AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid)
                ),
                "expected BatchInputInvalid before checkout, got {:?}",
                error.kind()
            );
        }
    }
}
