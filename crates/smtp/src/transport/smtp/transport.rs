#[cfg(feature = "test-support")]
use super::Tls;
#[cfg(all(test, feature = "account-error"))]
use super::pool::sync_impl::Pool;
#[cfg(all(test, feature = "account-error"))]
use super::{
    ClientId, Dialect,
    surface::{LmtpTransport, SmtpTransport},
};
use super::{Error, SmtpConnection, SmtpInfo};
#[cfg(all(test, feature = "account-error"))]
use super::{PoolConfig, SendOptions, account_error::SmtpErrorContext};
use crate::transport::smtp::error::SmtpCommandPhase;

/// Build client
#[derive(Debug, Clone)]
pub(super) struct SmtpClient {
    pub(super) info: SmtpInfo,
}

impl SmtpClient {
    /// Creates a new connection directly usable to send emails
    ///
    /// Handles encryption and authentication
    pub(super) fn connection(&self) -> Result<SmtpConnection, Error> {
        let plan = super::DialPlan::new(&self.info.tls, self.info.unix_socket.as_deref())?;
        #[cfg(feature = "test-support")]
        if let Some(connector) = &self.info.connector {
            // Scripted peers cannot prove TLS. Refuse before creating a peer,
            // just as the async dialer does, rather than bypassing policy.
            if !matches!(self.info.tls, Tls::None) {
                return Err(super::error::invalid_input(
                    "scripted connectors require plaintext TLS mode",
                ));
            }
            let mut conn = SmtpConnection::from_scripted_peer(
                connector.connect(),
                &self.info.hello_name,
                self.info.protocol,
                self.info.timeout,
                self.info.metering.clone(),
            )?;
            self.authenticate_if_configured(&mut conn)?;
            return Ok(conn);
        }
        if let Some(path) = &self.info.unix_socket {
            #[cfg(unix)]
            {
                let mut conn = SmtpConnection::connect_unix_with_protocol(
                    path,
                    self.info.timeout,
                    &self.info.hello_name,
                    self.info.protocol,
                    self.info.metering.clone(),
                )?;

                self.authenticate_if_configured(&mut conn)?;
                return Ok(conn);
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                return Err(super::error::invalid_input(
                    "Unix-domain LMTP sockets are only supported on Unix platforms",
                ));
            }
        }

        // Owned, because resolution may move to a helper thread that outlives
        // this call when it exceeds the timeout.
        let server: &str = self.info.server.as_ref();
        let mut conn = SmtpConnection::connect_with_protocol::<(String, u16)>(
            (server.to_owned(), self.info.port),
            self.info.timeout,
            &self.info.hello_name,
            plan.implicit_tls,
            None,
            self.info.protocol,
            self.info.metering.clone(),
        )?;

        if let Some(parameters) = plan.starttls(conn.can_starttls()) {
            conn.starttls(parameters, &self.info.hello_name)?;
        }

        self.authenticate_if_configured(&mut conn)?;
        Ok(conn)
    }

    /// The post-greeting authentication stage, shared by the TCP and the
    /// Unix-socket funnels.
    ///
    /// Factored out of `connection()` so it can be driven against a scripted
    /// in-memory connection: the refusal it enforces (no AUTH over an
    /// unencrypted link unless the caller opted in) is a decision made after
    /// the greeting and EHLO, and has nothing to do with how the socket was
    /// dialled.
    fn authenticate_if_configured(&self, conn: &mut SmtpConnection) -> Result<(), Error> {
        if let Some(credentials) = &self.info.credentials {
            if let Err(error) = self.info.ensure_can_authenticate(conn.is_encrypted()) {
                conn.abort();
                // Refused before any rung was picked, so there is no
                // mechanism to name here.
                return Err(error.with_phase(SmtpCommandPhase::Auth { mechanism: None }));
            }
            conn.auth(self.info.authentication(), credentials)?;
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "account-error"))]
mod tests {
    use crate::transport::smtp::Tls;
    use crate::transport::smtp::test_support::Transcript;
    use crate::{
        LmtpTransport, SmtpTransport,
        transport::smtp::authentication::{
            Credentials, DEFAULT_MECHANISMS, Mechanism, OAUTH2_MECHANISMS, PASSWORD_MECHANISMS,
        },
    };

    use super::{ClientId, Dialect, SmtpClient, SmtpConnection, SmtpErrorContext};

    fn hello() -> ClientId {
        ClientId::Domain("client.example".to_owned())
    }

    #[test]
    fn transport_from_plaintext_url() {
        let builder = SmtpTransport::from_url("smtp://127.0.0.1:2525").unwrap();

        assert_eq!(builder.info.port, 2525);
        assert_eq!(builder.info.server, "127.0.0.1");
    }

    #[test]
    fn lmtp_builder_uses_lmtp_defaults() {
        let builder = LmtpTransport::builder_dangerous("localhost");

        assert_eq!(builder.info.port, super::super::LMTP_PORT);
        assert_eq!(builder.info.protocol, Dialect::Lmtp);

        #[cfg(unix)]
        {
            // Explicit TCP configuration must replace a previous local route
            // with identical behavior in the blocking and Tokio builders.
            let socket = LmtpTransport::unix_socket("/run/lmtp.sock");
            assert!(socket.clone().tls(Tls::None).info.unix_socket.is_none());
            assert!(socket.port(2524).info.unix_socket.is_none());
        }
    }

    #[test]
    fn transport_from_tls_url() {
        let builder = SmtpTransport::from_url("smtp://127.0.0.1:2525").unwrap();

        assert!(matches!(builder.info.tls, Tls::None));

        let builder =
            SmtpTransport::from_url("smtps://username:password@smtp.example.com:465").unwrap();

        assert_eq!(builder.info.port, 465);
        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::Password { ref username, ref password })
                if username == "username" && password.as_str() == "password"
        ));
        assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
        assert_eq!(builder.info.server, "smtp.example.com");

        let builder = SmtpTransport::from_url(
            "smtps://user%40example.com:pa$$word%3F%22!@smtp.example.com:465",
        )
        .unwrap();

        assert_eq!(builder.info.port, 465);
        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::Password { ref username, ref password })
                if username == "user@example.com" && password.as_str() == "pa$$word?\"!"
        ));
        assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
        assert_eq!(builder.info.server, "smtp.example.com");

        let builder =
            SmtpTransport::from_url("smtp://username:password@smtp.example.com:587?tls=required")
                .unwrap();

        assert_eq!(builder.info.port, 587);
        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::Password { ref username, ref password })
                if username == "username" && password.as_str() == "password"
        ));
        assert!(matches!(builder.info.tls, Tls::Required(_)));

        let builder = SmtpTransport::from_url(
            "smtp://username:password@smtp.example.com:587?tls=opportunistic",
        )
        .unwrap();

        assert_eq!(builder.info.port, 587);
        assert!(matches!(builder.info.tls, Tls::Opportunistic(_)));

        let builder = SmtpTransport::from_url("smtps://smtp.example.com").unwrap();

        assert_eq!(builder.info.port, 465);
        assert!(builder.info.credentials.is_none());
        assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
    }

    #[test]
    fn plaintext_url_and_builder_share_the_default_port() {
        assert_eq!(
            SmtpTransport::from_url("smtp://relay.example")
                .unwrap()
                .info
                .port,
            SmtpTransport::builder_dangerous("relay.example").info.port
        );
        assert_eq!(
            SmtpTransport::from_url("smtp://relay.example:587")
                .unwrap()
                .info
                .port,
            587
        );
    }

    #[test]
    fn url_configuration_errors_are_invalid_input() {
        for url in [
            "not a URL",
            "smtp://",
            "unknown://relay.example",
            "smtp://relay.example?tls=wrong",
            "smtp://user@relay.example",
            "smtp://user:%FF@relay.example",
            "smtp://user:%GG@relay.example",
        ] {
            let error = SmtpTransport::from_url(url).unwrap_err();
            assert!(error.is_invalid_input(), "{url}: {error}");
            let account = crate::transport::smtp::account_error::into_account_error(
                error,
                SmtpErrorContext::send(Dialect::Smtp),
            );
            assert!(matches!(
                account.kind(),
                bifrost_types::error::AccountErrorKind::Request(
                    bifrost_types::error::RequestErrorKind::Malformed
                )
            ));
        }
    }

    #[test]
    fn password_helper_uses_password_mechanisms() {
        let builder =
            SmtpTransport::builder_dangerous("smtp.example.com").password("username", "password");

        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::Password { ref username, ref password })
                if username == "username" && password.as_str() == "password"
        ));
        assert_eq!(builder.info.authentication(), PASSWORD_MECHANISMS);
        // Pin the grown default-construction surface explicitly: SCRAM is now
        // offered out of the box (strongest first), LOGIN is opt-in only.
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

    #[test]
    fn oauth2_helper_prefers_standard_bearer_mechanism() {
        let builder = SmtpTransport::builder_dangerous("smtp.example.com")
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

    #[test]
    fn credentials_preserve_explicit_authentication_mechanisms() {
        let builder = SmtpTransport::builder_dangerous("smtp.example.com")
            .authentication(vec![Mechanism::Xoauth2])
            .oauth2("user@example.com", "token");

        assert_eq!(builder.info.authentication(), [Mechanism::Xoauth2]);

        let builder = SmtpTransport::builder_dangerous("smtp.example.com")
            .authentication(vec![Mechanism::Plain])
            .password("username", "password");

        assert_eq!(builder.info.authentication(), [Mechanism::Plain]);
    }

    /// Finding 4c: ported off the loopback listener. The transcript scripts
    /// the greeting and the `AUTH PLAIN`-advertising EHLO reply and NOTHING
    /// else, so an AUTH command reaching the wire fails the write outright
    /// ("transcript exhausted by client write") instead of producing the
    /// policy error - which is the same observable the old listener's
    /// zero-length read gave, without a socket.
    #[test]
    fn plaintext_auth_is_refused_before_auth_command() {
        let transcript = Transcript::new("220 localhost\r\n").expect(
            "EHLO client.example\r\n",
            "250-localhost\r\n250 AUTH PLAIN\r\n",
        );
        let mut conn =
            SmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp).unwrap();

        let info = SmtpTransport::builder_dangerous("127.0.0.1")
            .password("user", "pass")
            .info;
        let Err(error) = (SmtpClient { info }).authenticate_if_configured(&mut conn) else {
            panic!("plaintext auth must be refused");
        };

        assert!(error.is_policy(), "expected policy error, got {error:?}");
        transcript.assert_exhausted();
    }

    /// Finding 4c: the escape-hatch mirror, also ported off the listener. Here
    /// the transcript DOES script an `AUTH PLAIN` step, so the exact base64
    /// credential line the driver puts on the wire is asserted by the
    /// transcript itself.
    #[test]
    fn dangerous_allow_insecure_auth_sends_auth_plain_on_a_plaintext_connection() {
        let transcript = Transcript::new("220 localhost\r\n")
            .expect(
                "EHLO client.example\r\n",
                "250-localhost\r\n250 AUTH PLAIN\r\n",
            )
            .expect("AUTH PLAIN AHVzZXIAcGFzcw==\r\n", "235 authenticated\r\n")
            // A successful AUTH re-issues EHLO: the server's capability list
            // may change once the session is authenticated.
            .expect("EHLO client.example\r\n", "250 localhost\r\n");
        let mut conn =
            SmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp).unwrap();

        let info = SmtpTransport::builder_dangerous("127.0.0.1")
            .password("user", "pass")
            .dangerous_allow_insecure_auth(true)
            .authentication(vec![Mechanism::Plain])
            .info;
        (SmtpClient { info })
            .authenticate_if_configured(&mut conn)
            .expect("the escape hatch permits plaintext AUTH");

        transcript.assert_exhausted();
    }

    mod pooled_batch {
        use std::sync::Arc;

        use bifrost_types::error::{AccountErrorKind, BatchItem, BatchItemId, RequestErrorKind};

        use crate::transport::smtp::test_support::Transcript;

        use super::super::{
            ClientId, Dialect, LmtpTransport, Pool, PoolConfig, SendOptions, SmtpClient,
            SmtpConnection, SmtpInfo, SmtpTransport,
        };

        fn hello() -> ClientId {
            ClientId::Domain("client.example".to_owned())
        }

        /// A pool that will never dial: the only connection it can ever hand
        /// out is the scripted one parked here.
        fn pool_with(conn: SmtpConnection, protocol: Dialect) -> Arc<Pool> {
            let pool = Pool::new(
                // The checkout probe would need its own scripted NOOP step;
                // the probe itself is pinned at the connection level.
                PoolConfig::new().test_on_checkout(false),
                SmtpClient {
                    info: SmtpInfo::new("transcript.invalid", protocol),
                },
            );
            pool.park_for_test(conn);
            pool
        }

        /// The blocking mirror of `pool_max_size_bounds_checked_out_connections`.
        /// With one slot and one connection checked out, a second checkout must
        /// WAIT for the slot rather than dial past the bound. `transcript.invalid`
        /// does not resolve, so a pool that dials past `max_size` fails this with
        /// a connection error instead of handing back the recycled connection.
        #[test]
        fn pool_max_size_bounds_checked_out_connections() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n");
            let conn = SmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp)
                .unwrap();
            let pool = Pool::new(
                PoolConfig::new().max_size(1).test_on_checkout(false),
                SmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
                },
            );
            pool.park_for_test(conn);

            let first = pool.connection().expect("the parked connection");
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::scope(|scope| {
                let waiter = {
                    let pool = Arc::clone(&pool);
                    scope.spawn(move || {
                        tx.send(()).unwrap();
                        pool.connection().map(|conn| {
                            drop(conn);
                        })
                    })
                };

                rx.recv().unwrap();
                drop(first);

                waiter
                    .join()
                    .unwrap()
                    .expect("the released slot must hand back the pooled connection, not a dial");
            });
            assert_eq!(pool.idle_count_for_test(), 1);
            transcript.assert_exhausted();
        }

        /// The blocking mirror of the async shutdown wake-up. A checkout parked
        /// on the condvar must not survive `shutdown()` as a live checkout, and
        /// must not dial once woken: `transcript.invalid` does not resolve, so
        /// a dial surfaces as a connection error rather than
        /// `TransportShutdown`.
        ///
        /// What this does NOT pin is the pure hang: the blocking `recycle` also
        /// notifies, so dropping the checked-out guard wakes the waiter even
        /// with `shutdown()`'s `notify_all` ablated. Pinning the case where the
        /// guard is never returned needs a bounded join, which means a
        /// wall-clock wait, which is out of scope here. The `notify_all` in
        /// `shutdown()` carries its own comment for that reason.
        #[test]
        fn shutdown_wakes_a_blocked_checkout_instead_of_letting_it_dial() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n");
            let conn = SmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp)
                .unwrap();
            let pool = Pool::new(
                PoolConfig::new().max_size(1).test_on_checkout(false),
                SmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
                },
            );
            pool.park_for_test(conn);

            let first = pool.connection().expect("the parked connection");
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::scope(|scope| {
                let waiter = {
                    let pool = Arc::clone(&pool);
                    scope.spawn(move || {
                        tx.send(()).unwrap();
                        pool.connection().err().map(|error| {
                            (
                                matches!(
                                    error.kind(),
                                    crate::transport::smtp::error::ErrorKind::TransportShutdown
                                ),
                                error.to_string(),
                            )
                        })
                    })
                };

                rx.recv().unwrap();
                pool.shutdown();
                drop(first);

                let (is_shutdown, message) = waiter
                    .join()
                    .unwrap()
                    .expect("a checkout must not succeed against a shut-down pool");
                assert!(is_shutdown, "expected a shutdown error, got: {message}");
            });
            assert_eq!(pool.idle_count_for_test(), 0);
            transcript.assert_exhausted();
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

        /// Finding 4b: the transport-level LMTP delivery pin, ported off the
        /// `TcpListener`/`UnixListener` scripted servers it used to run
        /// against. The transcript asserts the exact command sequence the old
        /// `assert_lmtp_delivery_commands` checked, and the returned statuses
        /// pin that a mid-envelope RCPT rejection is re-inserted in the
        /// original recipient order alongside the two real final statuses.
        #[test]
        fn lmtp_transport_returns_per_recipient_statuses() {
            use crate::Transport;
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
            let conn = SmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Lmtp)
                .unwrap();
            let transport = LmtpTransport {
                marker_: std::marker::PhantomData,
                inner: pool_with(conn, Dialect::Lmtp),
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
        /// the TCP case above (same `SmtpConnection`, same driver), so what is
        /// left to pin is that the builder routes to the Unix funnel at all and
        /// that the documented TLS-over-Unix rejection fires before any dial.
        #[test]
        #[cfg(unix)]
        fn lmtp_unix_socket_builder_routes_to_the_unix_funnel() {
            use crate::transport::smtp::Tls;

            let builder = LmtpTransport::unix_socket("/run/lmtp.sock");
            assert_eq!(
                builder.info.unix_socket.as_deref(),
                Some(std::path::Path::new("/run/lmtp.sock"))
            );
            assert_eq!(builder.info.protocol, Dialect::Lmtp);

            // TLS over a Unix socket is refused inside `connection()`, before
            // any connect syscall, so this branch is reachable hermetically -
            // and proves the Unix funnel, not the TCP one, was entered.
            let mut info = SmtpInfo::new("localhost", Dialect::Lmtp);
            info.unix_socket = Some(std::path::PathBuf::from("/run/lmtp.sock"));
            info.tls = Tls::Wrapper(
                crate::transport::smtp::client::TlsParametersBuilder::new("localhost".to_owned())
                    .build()
                    .unwrap(),
            );
            let error = SmtpClient { info }
                .connection()
                .err()
                .expect("TLS over a Unix socket is refused");
            assert!(
                error.to_string().contains("Unix-domain LMTP sockets"),
                "got {error}"
            );
        }

        /// End-to-end counterpart to the connection-level `should_retire()`
        /// pin: a completed LMTP delivery must leave the pool empty, so the
        /// next transaction cannot inherit a stream that may still hold an
        /// unread final status.
        #[test]
        fn lmtp_batch_send_leaves_no_pooled_connection_behind() {
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
            let conn = SmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Lmtp)
                .unwrap();
            let pool = pool_with(conn, Dialect::Lmtp);
            let transport = LmtpTransport {
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

        /// SMTP is the contrast case: the same entry point on a healthy SMTP
        /// connection returns it to the pool, and the next batch reuses it
        /// without a reconnect.
        #[test]
        fn smtp_batch_send_recycles_and_reuses_its_pooled_connection() {
            let transcript = Transcript::new("220 smtp.example\r\n")
                .expect("EHLO client.example\r\n", "250 smtp.example\r\n")
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<first@example.com>\r\n", "250 first ok\r\n")
                .expect("DATA\r\n", "354 send body\r\n")
                .expect("body", "")
                .expect("\r\n.\r\n", "250 queued\r\n")
                // Second transaction: no greeting, no EHLO. Any reconnect
                // would have to write EHLO here and fail the transcript.
                .expect("MAIL FROM:<sender@example.com>\r\n", "250 sender ok\r\n")
                .expect("RCPT TO:<second@example.com>\r\n", "250 second ok\r\n")
                .expect("DATA\r\n", "354 send body\r\n")
                .expect("body", "")
                .expect("\r\n.\r\n", "250 queued\r\n");
            let conn = SmtpConnection::from_transcript(transcript.clone(), &hello(), Dialect::Smtp)
                .unwrap();
            let pool = pool_with(conn, Dialect::Smtp);
            let transport = SmtpTransport {
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
                .unwrap();
            assert_eq!(second.succeeded().len(), 1);
            assert_eq!(pool.idle_count_for_test(), 1);
            transcript.assert_exhausted();
        }

        /// A batch whose recipient ids are unusable must be rejected before
        /// checkout. The pool is shut down first, so reaching checkout would
        /// yield a transport-shutdown error instead - and dial nothing either
        /// way.
        #[test]
        fn invalid_batch_input_is_rejected_before_pool_checkout() {
            let pool = Pool::new(
                PoolConfig::new(),
                SmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Dialect::Smtp),
                },
            );
            pool.shutdown();
            let transport = SmtpTransport {
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
                .expect_err("duplicate batch ids are refused");
            assert!(
                matches!(
                    error.kind(),
                    AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid)
                ),
                "expected BatchInputInvalid before checkout, got {:?}",
                error.kind()
            );

            let empty: Vec<BatchItem<crate::address::Address>> = Vec::new();
            let error = transport
                .send_raw_batch_with_options(
                    Some("sender@example.com".parse().unwrap()),
                    empty,
                    b"body",
                    &SendOptions::default(),
                )
                .expect_err("an empty batch is refused");
            assert!(matches!(
                error.kind(),
                AccountErrorKind::Request(RequestErrorKind::BatchInputInvalid)
            ));
        }
    }
}
