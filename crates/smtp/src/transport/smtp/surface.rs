//! One configuration and transport shape for the dialect/runtime product.
use super::authentication::IntoSecretString;
use super::{
    ClientId, Credentials, Dialect, Error, Mechanism, PoolConfig, PortTls, Response, SendOptions,
    SmtpInfo, Tls, TlsParameters, WireMetering, default_port,
};
#[cfg(feature = "account-error")]
use super::{
    account_error::SmtpErrorContext,
    batch::{SmtpBatchRecipient, batch_level_error},
};
use crate::Envelope;
#[cfg(feature = "account-error")]
use crate::address::Address;
use bifrost_net::MeterSinkHandle;
#[cfg(feature = "account-error")]
use bifrost_types::error::{AccountError, BatchItem, BatchOutcome};
#[cfg(unix)]
use std::path::Path;
use std::{fmt, marker::PhantomData, sync::Arc, time::Duration};

/// SMTP wire dialect marker.
#[derive(Clone, Copy, Debug)]
pub struct Smtp;
/// LMTP wire dialect marker.
#[derive(Clone, Copy, Debug)]
pub struct Lmtp;
/// Blocking socket runtime marker.
#[derive(Clone, Copy, Debug)]
pub struct Blocking;
/// Tokio socket runtime marker.
#[cfg(feature = "tokio")]
#[derive(Clone, Copy, Debug)]
pub struct Tokio;

pub(super) trait WireDialect {
    const DIALECT: Dialect;
}
impl WireDialect for Smtp {
    const DIALECT: Dialect = Dialect::Smtp;
}
impl WireDialect for Lmtp {
    const DIALECT: Dialect = Dialect::Lmtp;
}

// These are implementation choices, not downstream runtime extension points.
// The concrete pools keep their native blocking and cancellation semantics.
pub(super) trait Runtime {
    type Pool: fmt::Debug;
    fn pool(info: SmtpInfo, config: PoolConfig) -> Arc<Self::Pool>;
}
impl Runtime for Blocking {
    type Pool = super::pool::sync_impl::Pool;
    fn pool(info: SmtpInfo, config: PoolConfig) -> Arc<Self::Pool> {
        Self::Pool::new(config, super::transport::SmtpClient { info })
    }
}
#[cfg(feature = "tokio")]
impl Runtime for Tokio {
    type Pool = super::pool::async_impl::Pool;
    fn pool(info: SmtpInfo, config: PoolConfig) -> Arc<Self::Pool> {
        Self::Pool::new(config, super::async_transport::AsyncSmtpClient { info })
    }
}

/// Pooled transport selected by wire dialect and socket runtime.
///
/// Holds the connection configuration and creates connections as needed.
/// SMTP sends recycle healthy connections for later messages, avoiding repeated
/// connection setup and authentication. Concurrent sends can grow the pool up
/// to its configured limit: SMTP cannot multiplex messages on one connection.
/// Broken connections are discarded. LMTP deliveries retire their connections
/// after the per-recipient final replies rather than reusing them.
///
/// Reuse the transport across sends to benefit from SMTP connection reuse.
/// Cloning a transport shares its pool; constructing a new transport for every
/// message creates a separate pool. Configure pool size, idle expiration and
/// checkout probes with [`Builder::pool_config`] and [`PoolConfig`].
#[allow(private_bounds)]
pub struct Transport<D, R: Runtime> {
    pub(super) inner: Arc<R::Pool>,
    pub(super) marker_: PhantomData<D>,
}
impl<D, R: Runtime> Clone for Transport<D, R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            marker_: PhantomData,
        }
    }
}
impl<D, R: Runtime> fmt::Debug for Transport<D, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transport")
            .field("inner", &self.inner)
            .finish()
    }
}

/// Shared SMTP/LMTP configuration builder.
#[derive(Clone, Debug)]
pub struct Builder<D, R> {
    pub(super) info: SmtpInfo,
    pool_config: PoolConfig,
    marker_: PhantomData<(D, R)>,
}

/// Blocking SMTP transport.
///
/// Sends messages through an SMTP relay server. Use [`SmtpTransport::relay`]
/// for implicit TLS on port 465, or [`SmtpTransport::starttls_relay`] for
/// required STARTTLS on port 587. Both validate the server certificate against
/// the supplied relay hostname. Required STARTTLS refuses to send credentials
/// or messages if the server cannot upgrade the connection.
///
/// Retain this transport or a clone across sends to reuse its connection pool.
/// See [`Transport`] for pooling behavior and [`SmtpTransportBuilder::pool_config`]
/// for pool settings. `builder_dangerous` starts with plaintext configuration;
/// use it only when configuring the connection security explicitly.
pub type SmtpTransport = Transport<Smtp, Blocking>;
/// Blocking LMTP transport, returning one reply per recipient.
pub type LmtpTransport = Transport<Lmtp, Blocking>;
/// Blocking SMTP builder.
pub type SmtpTransportBuilder = Builder<Smtp, Blocking>;
/// Blocking LMTP builder.
pub type LmtpTransportBuilder = Builder<Lmtp, Blocking>;
/// Tokio SMTP transport.
///
/// Asynchronously sends messages through an SMTP relay server using Tokio.
/// Use [`AsyncSmtpTransport::relay`] for implicit TLS on port 465, or
/// [`AsyncSmtpTransport::starttls_relay`] for required STARTTLS on port 587.
/// Both validate the server certificate against the supplied relay hostname.
/// Required STARTTLS refuses to send credentials or messages if the server
/// cannot upgrade the connection.
///
/// Retain this transport or a clone across sends to reuse its connection pool.
/// See [`Transport`] for pooling behavior and
/// [`AsyncSmtpTransportBuilder::pool_config`] for pool settings.
/// `builder_dangerous` starts with plaintext configuration; use it only when
/// configuring the connection security explicitly.
#[cfg(feature = "tokio")]
pub type AsyncSmtpTransport = Transport<Smtp, Tokio>;
/// Tokio LMTP transport, returning one reply per recipient.
#[cfg(feature = "tokio")]
pub type AsyncLmtpTransport = Transport<Lmtp, Tokio>;
/// Tokio SMTP builder.
#[cfg(feature = "tokio")]
pub type AsyncSmtpTransportBuilder = Builder<Smtp, Tokio>;
/// Tokio LMTP builder.
#[cfg(feature = "tokio")]
pub type AsyncLmtpTransportBuilder = Builder<Lmtp, Tokio>;

#[allow(private_bounds)]
impl<D: WireDialect, R> Builder<D, R> {
    pub(crate) fn new(server: impl Into<String>) -> Self {
        Self {
            info: SmtpInfo::new(server, D::DIALECT),
            pool_config: PoolConfig::default(),
            marker_: PhantomData,
        }
    }

    /// Substitute a fresh in-memory peer at stream creation. Plaintext TCP only.
    ///
    /// TLS modes other than `Tls::None` and Unix socket routes are refused
    /// before the connector is invoked, using the same policy in both runtimes.
    #[cfg(feature = "test-support")]
    pub fn scripted_connector(mut self, connector: super::test_support::ScriptedConnector) -> Self {
        self.info.connector = Some(connector);
        self
    }

    /// Set the name used during EHLO or LHLO
    pub fn hello_name(mut self, name: ClientId) -> Self {
        self.info.hello_name = name;
        self
    }

    /// Set the authentication credentials to use
    ///
    /// Unless [`Self::authentication`] was called explicitly, this also selects
    /// the default mechanisms for the credential kind.
    pub fn credentials(mut self, credentials: Credentials) -> Self {
        self.info.set_credentials(credentials);
        self
    }

    /// Set username and password authentication credentials.
    pub fn password<U, P>(self, username: U, password: P) -> Self
    where
        U: Into<String>,
        P: IntoSecretString,
    {
        self.credentials(Credentials::password(username, password))
    }

    /// Set OAuth 2.0 bearer-token authentication credentials.
    ///
    /// This configures `OAUTHBEARER` and `XOAUTH2`, in that preference order.
    pub fn oauth2<I, T>(self, identity: I, access_token: T) -> Self
    where
        I: Into<String>,
        T: IntoSecretString,
    {
        self.credentials(Credentials::oauth2(identity, access_token))
    }

    /// Set OAuth 2.0 bearer-token credentials from a shared token source.
    ///
    /// The blocking transport reads the token from the source snapshot;
    /// a `StaticTokenSource` (or an already-fresh `OAuthRefresher`)
    /// resolves immediately. A source needing a network refresh requires
    /// the async transport.
    pub fn oauth2_source<I>(
        self,
        identity: I,
        token_source: Arc<dyn bifrost_net::TokenSource>,
    ) -> Self
    where
        I: Into<String>,
    {
        self.credentials(Credentials::oauth2_source(identity, token_source))
    }

    /// Set the authentication mechanism to use
    pub fn authentication(mut self, mechanisms: Vec<Mechanism>) -> Self {
        self.info.set_authentication(mechanisms);
        self
    }

    /// Allow credentials to be sent over an unencrypted SMTP or LMTP connection.
    ///
    /// By default, Bifrost SMTP refuses to send passwords or bearer tokens
    /// unless the connection is already encrypted by TLS or has been upgraded
    /// with STARTTLS. Set this only for trusted local relays or test servers.
    pub fn dangerous_allow_insecure_auth(mut self, allow: bool) -> Self {
        self.info.allow_insecure_auth = allow;
        self
    }

    /// Set the operation timeout. `None` leaves protocol operations unbounded.
    /// Tokio setup shares one deadline; blocking setup budgets each phase
    /// separately. Blocking STARTTLS retains its handshake fallback timeout.
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.info.timeout = timeout;
        self
    }

    /// Meter socket bytes and optionally share a bandwidth cap across connections.
    /// Blocking admission waits on the calling thread; Tokio admission awaits.
    #[must_use]
    pub fn bandwidth_metering(
        mut self,
        sink: Option<MeterSinkHandle>,
        budget: Option<bifrost_net::BandwidthBudget>,
    ) -> Self {
        self.info.metering = WireMetering::new(sink, budget);
        self
    }

    /// Select the TCP port, clearing a previously configured Unix socket.
    pub fn port(mut self, port: u16) -> Self {
        self.info.port = port;
        self.info.unix_socket = None;
        self
    }

    /// Select TLS over TCP, clearing a previously configured Unix socket.
    /// This routing rule is shared by blocking and Tokio LMTP: an explicit
    /// TCP setting must not silently keep delivering to the old local socket.
    pub fn tls(mut self, tls: Tls) -> Self {
        self.info.tls = tls;
        self.info.unix_socket = None;
        self
    }

    /// Use a custom configuration for the connection pool
    ///
    /// Defaults can be found at [`PoolConfig`]
    pub fn pool_config(mut self, pool_config: PoolConfig) -> Self {
        self.pool_config = pool_config;
        self
    }
}
impl<R> Builder<Lmtp, R> {
    /// Connect over a Unix-domain socket instead of TCP.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub fn unix_socket(mut self, path: impl AsRef<Path>) -> Self {
        self.info.unix_socket = Some(path.as_ref().to_path_buf());
        self.info.tls = super::Tls::None;
        self
    }
}
#[allow(private_bounds)]
impl<D, R: Runtime> Builder<D, R> {
    /// Build a pooled transport with this configuration.
    pub fn build(self) -> Transport<D, R> {
        Transport {
            inner: R::pool(self.info, self.pool_config),
            marker_: PhantomData,
        }
    }
}
#[allow(private_bounds)]
impl<D: WireDialect, R: Runtime> Transport<D, R> {
    /// Configure an unencrypted connection with no authentication.
    pub fn builder_dangerous(server: impl Into<String>) -> Builder<D, R> {
        Builder::new(server)
    }
    /// Connect to the dialect's default plaintext port on localhost.
    pub fn unencrypted_localhost() -> Self {
        Self::builder_dangerous("localhost").build()
    }
}
#[allow(private_bounds)]
impl<R: Runtime> Transport<Smtp, R> {
    /// Configure SMTP over implicit TLS, validating the relay certificate.
    pub fn relay(relay: &str) -> Result<Builder<Smtp, R>, Error> {
        Ok(Self::builder_dangerous(relay)
            .port(default_port(Dialect::Smtp, PortTls::Implicit))
            .tls(Tls::Wrapper(TlsParameters::new(relay.into())?)))
    }
    /// Require STARTTLS before authentication or message submission.
    pub fn starttls_relay(relay: &str) -> Result<Builder<Smtp, R>, Error> {
        Ok(Self::builder_dangerous(relay)
            .port(default_port(Dialect::Smtp, PortTls::StartTls))
            .tls(Tls::Required(TlsParameters::new(relay.into())?)))
    }
    /// Parse SMTP URL credentials, host, port, hello name and TLS policy.
    pub fn from_url(url: &str) -> Result<Builder<Smtp, R>, Error> {
        super::connection_url::from_connection_url(url)
    }
}
#[allow(private_bounds)]
impl<R: Runtime> Transport<Lmtp, R> {
    /// Configure local delivery over a Unix-domain socket.
    #[cfg(unix)]
    pub fn unix_socket(path: impl AsRef<Path>) -> Builder<Lmtp, R> {
        Self::builder_dangerous("localhost").unix_socket(path)
    }
}

// Runtime substitution is syntactic because a blocking result and a future
// have different calling conventions. Keep checkout, validation and resolution
// in this one body; the pools and connections own runtime-specific I/O.
macro_rules! transport_methods {
    ($d:ty, $r:ty, $dialect:expr, $output:ty, [$($async:tt)*], [$($wait:tt)*],
        $trait:path, $direct:ident, $send:ident, $bdat:ident, $batch:ident) => {
        impl Transport<$d, $r> {
    /// Test the connection with NOOP.
    ///
    /// `test_connection()` tests the connection by using the SMTP NOOP command.
    pub $($async)* fn test_connection(&self) -> Result<bool, Error> {
        let mut conn = self.inner.connection() $($wait)* ?;

        let is_connected = conn.test_connected() $($wait)*;

        Ok(is_connected)
    }

    /// Sends `VRFY` and returns the server response.
    ///
    /// Many servers disable `VRFY` for privacy. Negative SMTP replies are
    /// returned as [`Response`] values so callers can inspect the exact status.
    pub $($async)* fn verify(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection() $($wait)* ?;
        conn.verify(argument) $($wait)*
    }

    /// Sends `EXPN` and returns the server response.
    ///
    /// Many servers disable `EXPN` for privacy. Negative SMTP replies are
    /// returned as [`Response`] values so callers can inspect the exact status.
    pub $($async)* fn expand(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection() $($wait)* ?;
        conn.expand(argument) $($wait)*
    }

    /// Sends an email with per-message SMTP options.
    ///
    /// This is the advanced counterpart to [`crate::Transport::send_raw`]. It keeps
    /// the core transport trait simple while still allowing message-specific
    /// ESMTP parameters such as `REQUIRETLS`, `DELIVERBY`, `FUTURERELEASE`,
    /// `MT-PRIORITY`, and DSN options.
    pub $($async)* fn send_raw_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<$output, Error> {
        let mut conn = self.inner.connection() $($wait)* ?;

        let result = conn.$send(envelope, email, options) $($wait)* ?;

        Ok(result)
    }

    /// Sends an email with `BDAT ... LAST`.
    ///
    /// The server must advertise `CHUNKING`. This path avoids DATA
    /// dot-stuffing and is the required path for `BODY=BINARYMIME`.
    pub $($async)* fn send_raw_bdat(&self, envelope: &Envelope, email: &[u8]) -> Result<$output, Error> {
        self.send_raw_bdat_with_options(envelope, email, &SendOptions::default()) $($wait)*
    }

    /// Sends an email with `BDAT ... LAST` and per-message SMTP options.
    pub $($async)* fn send_raw_bdat_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<$output, Error> {
        let mut conn = self.inner.connection() $($wait)* ?;

        let result = conn.$bdat(envelope, email, options) $($wait)* ?;

        Ok(result)
    }

    /// Account-oriented multi-recipient send over the configured dialect.
    ///
    /// Validates batch input, runs MAIL FROM / RCPT TO / DATA body, and returns
    /// a `BatchOutcome<()>` with every submitted recipient accounted for in
    /// exactly one lane (`succeeded`, `failed`, or `uncertain`).
    ///
    /// Returns `Err(AccountError)` only for batch-level failures where no
    /// per-recipient outcome can be attributed: pool checkout failure, connection
    /// setup failure, MAIL FROM rejection, or a pre-MAIL transport drop.
    #[cfg(feature = "account-error")]
    pub $($async)* fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError> {
        let ctx = SmtpErrorContext::send($dialect);

        bifrost_types::error::validate_batch_input(
            &recipients,
            $dialect.into(),
            bifrost_types::error::AccountOperation::Send,
        )?;

        let batch_recipients: Vec<SmtpBatchRecipient> = recipients
            .into_iter()
            .map(SmtpBatchRecipient::from)
            .collect();

        let mut conn = self
            .inner
            .connection() $($wait)*
            .map_err(|e| batch_level_error(e, ctx.clone()))?;

        match conn.$batch(from, batch_recipients, email, options) $($wait)* {
            Ok(progress) => Ok(progress.resolve()),
            Err((e, _progress)) => Err(batch_level_error(e, ctx)),
        }
    }
        }
        impl $trait for Transport<$d, $r> {
            type Ok = $output;
            type Error = Error;
            $($async)* fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Error> {
                let mut conn = self.inner.connection() $($wait)* ?;
                conn.$direct(envelope, email) $($wait)*
            }
            $($async)* fn shutdown(&self) {
                self.inner.shutdown() $($wait)*;
            }
        }
    };
}
transport_methods!(
    Smtp,
    Blocking,
    Dialect::Smtp,
    Response,
    [],
    [],
    crate::Transport,
    send,
    send_with_options,
    send_bdat_with_options,
    send_smtp_batch
);
transport_methods!(
    Lmtp,
    Blocking,
    Dialect::Lmtp,
    Vec<Response>,
    [],
    [],
    crate::Transport,
    send_lmtp,
    send_lmtp_with_options,
    send_lmtp_bdat_with_options,
    send_lmtp_batch
);
#[cfg(feature = "tokio")]
transport_methods!(Smtp, Tokio, Dialect::Smtp, Response, [async], [.await],
    crate::AsyncTransport, send, send_with_options, send_bdat_with_options, send_smtp_batch);
#[cfg(feature = "tokio")]
transport_methods!(Lmtp, Tokio, Dialect::Lmtp, Vec<Response>, [async], [.await],
    crate::AsyncTransport, send_lmtp, send_lmtp_with_options, send_lmtp_bdat_with_options, send_lmtp_batch);
