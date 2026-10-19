#[cfg(unix)]
use std::path::Path;
use std::sync::Arc;
use std::{fmt::Debug, time::Duration};

use bifrost_types::error::{AccountError, BatchItem, BatchOutcome};

use bifrost_net::MeterSinkHandle;

use super::PoolConfig;
use super::WireMetering;
use super::batch::{SmtpBatchRecipient, batch_level_error};
use super::pool::sync_impl::Pool;
use super::{
    ClientId, Credentials, Error, Mechanism, Protocol, Response, SendOptions, SmtpConnection,
    SmtpInfo, error,
};
use super::{SUBMISSION_PORT, SUBMISSIONS_PORT, Tls, TlsParameters};
use crate::address::Address;
use crate::transport::smtp::account_error::SmtpErrorContext;
use crate::transport::smtp::authentication::IntoSecretString;
use crate::transport::smtp::error::SmtpCommandPhase;
use crate::{Transport, address::Envelope};

/// Synchronously send emails using the SMTP protocol
///
/// `SmtpTransport` is the primary way for communicating
/// with SMTP relay servers to send email messages. It holds the
/// client connect configuration and creates new connections
/// as necessary.
///
/// # Connection pool
///
/// `SmtpTransport` maintains a connection pool to manage SMTP connections. The
/// pool:
///
/// - Establishes a new connection when sending a message.
/// - Recycles connections internally after a message is sent.
/// - Reuses connections for subsequent messages, reducing connection setup overhead.
///
/// The connection pool can grow to hold multiple SMTP connections if multiple
/// emails are sent concurrently, as SMTP does not support multiplexing within a
/// single connection.
///
/// However, **connection reuse is not possible** if the `SmtpTransport` instance
/// is dropped after every email send operation. You must reuse the instance
/// of this struct for the connection pool to be of any use.
///
/// To customize connection pool settings, use [`SmtpTransportBuilder::pool_config`].
#[derive(Clone)]
pub struct SmtpTransport {
    inner: Arc<Pool>,
}

/// Synchronously send emails using the LMTP protocol
///
/// `LmtpTransport` is the local-delivery counterpart to [`SmtpTransport`].
/// LMTP uses `LHLO` for capability discovery and returns one status per
/// envelope recipient. Rejected recipients carry their `RCPT` response;
/// accepted recipients carry their post-DATA delivery response.
///
/// Direct sends return only the ordered statuses. Use
/// [`LmtpTransport::send_raw_batch_with_options`] when callers need the
/// RCPT-versus-final-status phase and per-recipient recovery classification.
#[derive(Clone)]
pub struct LmtpTransport {
    inner: Arc<Pool>,
}

impl Transport for SmtpTransport {
    type Ok = Response;
    type Error = Error;

    /// Sends an email
    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send(envelope, email)?;

        Ok(result)
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl Transport for LmtpTransport {
    type Ok = Vec<Response>;
    type Error = Error;

    /// Sends an email and returns one LMTP status per recipient.
    ///
    /// For per-recipient command-phase and recovery details, use
    /// [`LmtpTransport::send_raw_batch_with_options`].
    fn send_raw(&self, envelope: &Envelope, email: &[u8]) -> Result<Self::Ok, Self::Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_lmtp(envelope, email)?;

        Ok(result)
    }

    fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl Debug for SmtpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_struct("SmtpTransport");
        builder.field("inner", &self.inner);
        builder.finish()
    }
}

impl Debug for LmtpTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_struct("LmtpTransport");
        builder.field("inner", &self.inner);
        builder.finish()
    }
}

impl SmtpTransport {
    /// Simple and secure transport, using TLS connections to communicate with the SMTP server
    ///
    /// The right option for most SMTP servers.
    ///
    /// Creates an encrypted transport over submissions port, using the provided domain
    /// to validate TLS certificates.
    pub fn relay(relay: &str) -> Result<SmtpTransportBuilder, Error> {
        let tls_parameters = TlsParameters::new(relay.into())?;

        Ok(Self::builder_dangerous(relay)
            .port(SUBMISSIONS_PORT)
            .tls(Tls::Wrapper(tls_parameters)))
    }

    /// Simple and secure transport, using STARTTLS to obtain encrypted connections
    ///
    /// Alternative to [`SmtpTransport::relay`](#method.relay), for SMTP servers
    /// that don't take SMTPS connections.
    ///
    /// Creates an encrypted transport over submissions port, by first connecting using
    /// an unencrypted connection and then upgrading it with STARTTLS. The provided
    /// domain is used to validate TLS certificates.
    ///
    /// An error is returned if the connection can't be upgraded. No credentials
    /// or emails will be sent to the server, protecting from downgrade attacks.
    pub fn starttls_relay(relay: &str) -> Result<SmtpTransportBuilder, Error> {
        let tls_parameters = TlsParameters::new(relay.into())?;

        Ok(Self::builder_dangerous(relay)
            .port(SUBMISSION_PORT)
            .tls(Tls::Required(tls_parameters)))
    }

    /// Creates a new local SMTP client to port 25
    ///
    /// Shortcut for local unencrypted relay (typical local email daemon that will handle relaying)
    pub fn unencrypted_localhost() -> SmtpTransport {
        Self::builder_dangerous("localhost").build()
    }

    /// Creates a new SMTP client
    ///
    /// Defaults are:
    ///
    /// * No authentication
    /// * No TLS
    /// * A 10-second timeout for SMTP commands
    /// * Port 25
    ///
    /// Consider using [`SmtpTransport::relay`](#method.relay) or
    /// [`SmtpTransport::starttls_relay`](#method.starttls_relay) instead,
    /// if possible.
    pub fn builder_dangerous<T: Into<String>>(server: T) -> SmtpTransportBuilder {
        SmtpTransportBuilder::new(server)
    }

    /// Creates a `SmtpTransportBuilder` from a connection URL
    ///
    /// The protocol, credentials, host, port and EHLO name can be provided
    /// in a single URL. This may be simpler than having to configure SMTP
    /// through multiple configuration parameters and then having to pass
    /// those options to Bifrost SMTP.
    ///
    /// The URL is created in the following way:
    /// `scheme://user:pass@hostname:port/ehlo-name?tls=TLS`.
    ///
    /// `user` (Username) and `pass` (Password) are optional in case the
    /// SMTP relay doesn't require authentication. When `port` is not
    /// configured it is automatically determined based on the `scheme`.
    /// `ehlo-name` optionally overwrites the hostname sent for the EHLO
    /// command. `TLS` controls whether STARTTLS is simply enabled
    /// (`opportunistic` - not enough to prevent man-in-the-middle attacks)
    /// or `required` (require the server to upgrade the connection to
    /// STARTTLS, otherwise fail on suspicion of main-in-the-middle attempt).
    ///
    /// Use the following table to construct your SMTP url:
    ///
    /// | scheme  | `tls` query parameter | example                                            | default port | remarks                                                                                                                               |
    /// | ------- | --------------------- | -------------------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
    /// | `smtps` | unset                 | `smtps://user:pass@hostname:port`                  | 465          | SMTP over TLS, recommended method                                                                                                     |
    /// | `smtp`  | `required`            | `smtp://user:pass@hostname:port?tls=required`      | 587          | SMTP with STARTTLS required, when SMTP over TLS is not available                                                                      |
    /// | `smtp`  | `opportunistic`       | `smtp://user:pass@hostname:port?tls=opportunistic` | 587          | SMTP with optionally STARTTLS when supported by the server. Not suitable for production use: vulnerable to a man-in-the-middle attack |
    /// | `smtp`  | unset                 | `smtp://user:pass@hostname:port`                   | 587          | Always unencrypted SMTP. Credentials are refused by default; message data is still unencrypted                                        |
    ///
    /// IMPORTANT: some parameters like `user` and `pass` cannot simply
    /// be concatenated to construct the final URL because special characters
    /// contained within the parameter may confuse the URL decoder.
    /// Manually URL encode the parameters before concatenating them or use
    /// a proper URL encoder, like the following cargo script:
    ///
    /// ```rust
    /// # const TOML: &str = r#"
    /// #!/usr/bin/env cargo
    ///
    /// //! ```cargo
    /// //! [dependencies]
    /// //! url = "2"
    /// //! ```
    /// # "#;
    ///
    /// use url::Url;
    ///
    /// fn main() {
    ///     // don't touch this line
    ///     let mut url = Url::parse("foo://bar").unwrap();
    ///
    ///     // configure the scheme (`smtp` or `smtps`) here.
    ///     url.set_scheme("smtps").unwrap();
    ///     // configure the username and password.
    ///     // remove the following two lines if unauthenticated.
    ///     url.set_username("username").unwrap();
    ///     url.set_password(Some("password")).unwrap();
    ///     // configure the hostname
    ///     url.set_host(Some("smtp.example.com")).unwrap();
    ///     // configure the port - only necessary if using a non-default port
    ///     url.set_port(Some(465)).unwrap();
    ///     // configure the EHLO name
    ///     url.set_path("ehlo-name");
    ///
    ///     println!("{url}");
    /// }
    /// ```
    ///
    /// The connection URL can then be used in the following way:
    /// If a plaintext URL contains credentials, authentication is refused at
    /// connection time unless
    /// [`SmtpTransportBuilder::dangerous_allow_insecure_auth`] is enabled.
    ///
    /// ```rust,no_run
    /// use bifrost_smtp::{
    ///     Message, SmtpTransport, Transport, message::header::ContentType,
    /// };
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let email = Message::builder()
    ///     .from("NoBody <nobody@domain.tld>".parse().unwrap())
    ///     .reply_to("Yuin <yuin@domain.tld>".parse().unwrap())
    ///     .to("Hei <hei@domain.tld>".parse().unwrap())
    ///     .subject("Happy new year")
    ///     .header(ContentType::TEXT_PLAIN)
    ///     .body(String::from("Be happy!"))
    ///     .unwrap();
    ///
    /// // Open a remote connection to example
    /// let mailer = SmtpTransport::from_url("smtps://username:password@smtp.example.com")?.build();
    ///
    /// // Send the email
    /// mailer.send(&email)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_url(connection_url: &str) -> Result<SmtpTransportBuilder, Error> {
        super::connection_url::from_connection_url(connection_url)
    }

    /// Tests the SMTP connection
    ///
    /// `test_connection()` tests the connection by using the SMTP NOOP command.
    pub fn test_connection(&self) -> Result<bool, Error> {
        let mut conn = self.inner.connection()?;

        let is_connected = conn.test_connected();

        Ok(is_connected)
    }

    /// Sends `VRFY` and returns the server response.
    ///
    /// Many servers disable `VRFY` for privacy. Negative SMTP replies are
    /// returned as [`Response`] values so callers can inspect the exact status.
    pub fn verify(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;
        conn.verify(argument)
    }

    /// Sends `EXPN` and returns the server response.
    ///
    /// Many servers disable `EXPN` for privacy. Negative SMTP replies are
    /// returned as [`Response`] values so callers can inspect the exact status.
    pub fn expand(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;
        conn.expand(argument)
    }

    /// Sends an email with per-message SMTP options.
    ///
    /// This is the advanced counterpart to [`Transport::send_raw`]. It keeps
    /// the core transport trait simple while still allowing message-specific
    /// ESMTP parameters such as `REQUIRETLS`, `DELIVERBY`, `FUTURERELEASE`,
    /// `MT-PRIORITY`, and DSN options.
    pub fn send_raw_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_with_options(envelope, email, options)?;

        Ok(result)
    }

    /// Sends an email with `BDAT ... LAST`.
    ///
    /// The server must advertise `CHUNKING`. This path avoids DATA
    /// dot-stuffing and is the required path for `BODY=BINARYMIME`.
    pub fn send_raw_bdat(&self, envelope: &Envelope, email: &[u8]) -> Result<Response, Error> {
        self.send_raw_bdat_with_options(envelope, email, &SendOptions::default())
    }

    /// Sends an email with `BDAT ... LAST` and per-message SMTP options.
    pub fn send_raw_bdat_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_bdat_with_options(envelope, email, options)?;

        Ok(result)
    }

    /// Account-oriented multi-recipient send over SMTP.
    ///
    /// Validates batch input, runs MAIL FROM / RCPT TO / DATA body, and returns
    /// a `BatchOutcome<()>` with every submitted recipient accounted for in
    /// exactly one lane (`succeeded`, `failed`, or `uncertain`).
    ///
    /// Returns `Err(AccountError)` only for batch-level failures where no
    /// per-recipient outcome can be attributed: pool checkout failure, connection
    /// setup failure, MAIL FROM rejection, or a pre-MAIL transport drop.
    pub fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError> {
        let ctx = SmtpErrorContext::send(Protocol::Smtp);

        bifrost_types::error::validate_batch_input(
            &recipients,
            Protocol::Smtp,
            bifrost_types::error::AccountOperation::Send,
        )?;

        let batch_recipients: Vec<SmtpBatchRecipient> = recipients
            .into_iter()
            .map(SmtpBatchRecipient::from)
            .collect();

        let mut conn = self
            .inner
            .connection()
            .map_err(|e| batch_level_error(e, ctx.clone()))?;

        match conn.send_smtp_batch(from, batch_recipients, email, options) {
            Ok(progress) => Ok(progress.resolve()),
            Err((e, _progress)) => Err(batch_level_error(e, ctx)),
        }
    }
}

impl LmtpTransport {
    /// Creates a new local LMTP client to port 24.
    ///
    /// RFC 2033 does not assign an LMTP TCP port. Port 24 is the common TCP
    /// convention. Use [`Self::unix_socket`] for the more common local socket
    /// deployment shape.
    pub fn unencrypted_localhost() -> LmtpTransport {
        Self::builder_dangerous("localhost").build()
    }

    /// Creates a new local LMTP client over a Unix-domain socket.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub fn unix_socket(path: impl AsRef<Path>) -> LmtpTransportBuilder {
        Self::builder_dangerous("localhost").unix_socket(path)
    }

    /// Creates a new LMTP client.
    ///
    /// Defaults are:
    ///
    /// * No authentication
    /// * No TLS
    /// * A 10-second timeout for SMTP commands
    /// * Port 24, the common TCP LMTP convention
    pub fn builder_dangerous<T: Into<String>>(server: T) -> LmtpTransportBuilder {
        LmtpTransportBuilder::new(server)
    }

    /// Tests the LMTP connection.
    ///
    /// `test_connection()` tests the connection by using the SMTP NOOP command.
    pub fn test_connection(&self) -> Result<bool, Error> {
        let mut conn = self.inner.connection()?;

        let is_connected = conn.test_connected();

        Ok(is_connected)
    }

    /// Sends `VRFY` over LMTP and returns the server response.
    ///
    /// Negative replies are returned as [`Response`] values so callers can
    /// inspect the exact status.
    pub fn verify(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;
        conn.verify(argument)
    }

    /// Sends `EXPN` over LMTP and returns the server response.
    ///
    /// Negative replies are returned as [`Response`] values so callers can
    /// inspect the exact status.
    pub fn expand(&self, argument: impl Into<String>) -> Result<Response, Error> {
        let mut conn = self.inner.connection()?;
        conn.expand(argument)
    }

    /// Sends an email over LMTP with per-message SMTP options.
    ///
    /// For per-recipient command-phase and recovery details, use
    /// [`Self::send_raw_batch_with_options`].
    pub fn send_raw_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_lmtp_with_options(envelope, email, options)?;

        Ok(result)
    }

    /// Sends an email over LMTP with `BDAT ... LAST`.
    ///
    /// The server must advertise `CHUNKING`. Returned responses still preserve
    /// one status per input recipient.
    pub fn send_raw_bdat(&self, envelope: &Envelope, email: &[u8]) -> Result<Vec<Response>, Error> {
        self.send_raw_bdat_with_options(envelope, email, &SendOptions::default())
    }

    /// Sends an email over LMTP with `BDAT ... LAST` and per-message options.
    pub fn send_raw_bdat_with_options(
        &self,
        envelope: &Envelope,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<Vec<Response>, Error> {
        let mut conn = self.inner.connection()?;

        let result = conn.send_lmtp_bdat_with_options(envelope, email, options)?;

        Ok(result)
    }

    /// Account-oriented multi-recipient send over LMTP.
    ///
    /// Validates batch input, runs MAIL FROM / RCPT TO / DATA body, reads one
    /// final status per accepted recipient, and returns a `BatchOutcome<()>`
    /// with every submitted recipient accounted for exactly once.
    ///
    /// Returns `Err(AccountError)` only for batch-level failures where no
    /// per-recipient outcome can be attributed: pool checkout failure, connection
    /// setup failure, MAIL FROM rejection, or a pre-MAIL transport drop.
    pub fn send_raw_batch_with_options(
        &self,
        from: Option<Address>,
        recipients: Vec<BatchItem<Address>>,
        email: &[u8],
        options: &SendOptions,
    ) -> Result<BatchOutcome<()>, AccountError> {
        let ctx = SmtpErrorContext::send(Protocol::Lmtp);

        bifrost_types::error::validate_batch_input(
            &recipients,
            Protocol::Lmtp,
            bifrost_types::error::AccountOperation::Send,
        )?;

        let batch_recipients: Vec<SmtpBatchRecipient> = recipients
            .into_iter()
            .map(SmtpBatchRecipient::from)
            .collect();

        let mut conn = self
            .inner
            .connection()
            .map_err(|e| batch_level_error(e, ctx.clone()))?;

        match conn.send_lmtp_batch(from, batch_recipients, email, options) {
            Ok(progress) => Ok(progress.resolve()),
            Err((e, _progress)) => Err(batch_level_error(e, ctx)),
        }
    }
}

/// Contains client configuration.
/// Instances of this struct can be created using functions of [`SmtpTransport`].
#[derive(Debug, Clone)]
pub struct SmtpTransportBuilder {
    info: SmtpInfo,
    pool_config: PoolConfig,
}

/// Contains LMTP client configuration.
/// Instances of this struct can be created using functions of [`LmtpTransport`].
#[derive(Debug, Clone)]
pub struct LmtpTransportBuilder {
    info: SmtpInfo,
    pool_config: PoolConfig,
}

/// Builder for the SMTP `SmtpTransport`
impl SmtpTransportBuilder {
    // Create new builder with default parameters
    pub(crate) fn new<T: Into<String>>(server: T) -> Self {
        Self {
            info: SmtpInfo::new(server, Protocol::Smtp),
            pool_config: PoolConfig::default(),
        }
    }

    /// Set the name used during EHLO
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
    /// The blocking transport reads the token by polling the source once;
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

    /// Allow credentials to be sent over an unencrypted SMTP connection.
    ///
    /// By default, Bifrost LMTP refuses to send passwords or bearer tokens
    /// unless the connection is already encrypted by TLS or has been upgraded
    /// with STARTTLS. Set this only for trusted local relays or test servers.
    pub fn dangerous_allow_insecure_auth(mut self, allow: bool) -> Self {
        self.info.allow_insecure_auth = allow;
        self
    }

    /// Set the timeout duration
    ///
    /// `None` means "do not limit my SMTP conversation": protocol operations
    /// are then unbounded. It does NOT make an explicit `STARTTLS` handshake
    /// unbounded: with no timeout set, the socket is armed with a fixed 30
    /// second default for the handshake alone (a per-read bound, like every
    /// blocking timeout here) and then returned to unbounded. A configured
    /// timeout is used as given. The implicit-TLS handshake at connect time
    /// runs before any socket timeout is armed and stays unbounded on the
    /// blocking transport, as does address resolution.
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.info.timeout = timeout;
        self
    }

    /// Meter this transport's socket bytes, and optionally cap its
    /// throughput.
    ///
    /// See `AsyncSmtpTransportBuilder::bandwidth_metering`. This blocking
    /// transport honours the cap by sleeping the calling thread, which is
    /// the semantic its caller accepted by choosing the blocking API.
    #[must_use]
    pub fn bandwidth_metering(
        mut self,
        sink: Option<MeterSinkHandle>,
        budget: Option<bifrost_net::BandwidthBudget>,
    ) -> Self {
        self.info.metering = WireMetering::new(sink, budget);
        self
    }

    /// Set the port to use
    ///
    /// # Warning
    ///
    /// You probably do not need to call this method.
    ///
    /// Bifrost SMTP usually picks the correct `port` when building
    /// [`SmtpTransport`] using [`SmtpTransport::relay`] or
    /// [`SmtpTransport::starttls_relay`].
    ///
    /// # Errors
    ///
    /// Using the incorrect `port` and [`Self::tls`] combination may
    /// lead to hard to debug IO errors coming from the TLS library.
    pub fn port(mut self, port: u16) -> Self {
        self.info.port = port;
        self
    }

    /// Set the TLS settings to use.
    ///
    /// LMTP-over-TLS and STARTTLS are supported through the same TLS modes as
    /// SMTP. After STARTTLS succeeds, the client refreshes capabilities with
    /// `LHLO`.
    ///
    /// # Warning
    ///
    /// You probably do not need to call this method.
    ///
    /// By default Bifrost SMTP chooses the correct `tls` configuration when
    /// building [`SmtpTransport`] using [`SmtpTransport::relay`] or
    /// [`SmtpTransport::starttls_relay`].
    ///
    /// # Errors
    ///
    /// Using the wrong [`Tls`] and [`Self::port`] combination may
    /// lead to hard to debug IO errors coming from the TLS library.
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

    /// Build the transport
    ///
    /// Defaults can be found at [`PoolConfig`]
    pub fn build(self) -> SmtpTransport {
        let client = SmtpClient { info: self.info };

        let client = Pool::new(self.pool_config, client);

        SmtpTransport { inner: client }
    }
}

/// Builder for the LMTP `LmtpTransport`
impl LmtpTransportBuilder {
    // Create new builder with default parameters
    pub(crate) fn new<T: Into<String>>(server: T) -> Self {
        Self {
            info: SmtpInfo::new(server, Protocol::Lmtp),
            pool_config: PoolConfig::default(),
        }
    }

    /// Set the name used during LHLO
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
    /// The blocking transport reads the token by polling the source once;
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

    /// Allow credentials to be sent over an unencrypted LMTP connection.
    ///
    /// By default, Bifrost SMTP refuses to send passwords or bearer tokens
    /// unless the connection is already encrypted by TLS or has been upgraded
    /// with STARTTLS. Set this only for trusted local relays or test servers.
    pub fn dangerous_allow_insecure_auth(mut self, allow: bool) -> Self {
        self.info.allow_insecure_auth = allow;
        self
    }

    /// Set the timeout duration
    ///
    /// `None` means "do not limit my SMTP conversation": protocol operations
    /// are then unbounded. It does NOT make an explicit `STARTTLS` handshake
    /// unbounded: with no timeout set, the socket is armed with a fixed 30
    /// second default for the handshake alone (a per-read bound, like every
    /// blocking timeout here) and then returned to unbounded. A configured
    /// timeout is used as given. The implicit-TLS handshake at connect time
    /// runs before any socket timeout is armed and stays unbounded on the
    /// blocking transport, as does address resolution.
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.info.timeout = timeout;
        self
    }

    /// Meter this transport's socket bytes, and optionally cap its
    /// throughput.
    ///
    /// See `AsyncSmtpTransportBuilder::bandwidth_metering`. This blocking
    /// transport honours the cap by sleeping the calling thread, which is
    /// the semantic its caller accepted by choosing the blocking API.
    #[must_use]
    pub fn bandwidth_metering(
        mut self,
        sink: Option<MeterSinkHandle>,
        budget: Option<bifrost_net::BandwidthBudget>,
    ) -> Self {
        self.info.metering = WireMetering::new(sink, budget);
        self
    }

    /// Set the port to use
    pub fn port(mut self, port: u16) -> Self {
        self.info.port = port;
        self.info.unix_socket = None;
        self
    }

    /// Connect over a Unix-domain socket instead of TCP.
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub fn unix_socket(mut self, path: impl AsRef<Path>) -> Self {
        self.info.unix_socket = Some(path.as_ref().to_path_buf());
        self.info.tls = super::Tls::None;
        self
    }

    /// Set the TLS settings to use
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

    /// Build the transport
    ///
    /// Defaults can be found at [`PoolConfig`]
    pub fn build(self) -> LmtpTransport {
        let client = SmtpClient { info: self.info };

        let client = Pool::new(self.pool_config, client);

        LmtpTransport { inner: client }
    }
}

/// Build client
#[derive(Debug, Clone)]
pub(super) struct SmtpClient {
    info: SmtpInfo,
}

impl SmtpClient {
    /// Creates a new connection directly usable to send emails
    ///
    /// Handles encryption and authentication
    pub(super) fn connection(&self) -> Result<SmtpConnection, Error> {
        if let Some(path) = &self.info.unix_socket {
            #[cfg(unix)]
            {
                if self.info.uses_tls() {
                    return Err(error::invalid_input(
                        "TLS is not supported over Unix-domain LMTP sockets",
                    ));
                }
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
                return Err(error::invalid_input(
                    "Unix-domain LMTP sockets are only supported on Unix platforms",
                ));
            }
        }

        let tls_parameters = match &self.info.tls {
            Tls::Wrapper(tls_parameters) => Some(tls_parameters),
            _ => None,
        };

        // Owned, because resolution may move to a helper thread that outlives
        // this call when it exceeds the timeout.
        let server: &str = self.info.server.as_ref();
        let mut conn = SmtpConnection::connect_with_protocol::<(String, u16)>(
            (server.to_owned(), self.info.port),
            self.info.timeout,
            &self.info.hello_name,
            tls_parameters,
            None,
            self.info.protocol,
            self.info.metering.clone(),
        )?;

        match &self.info.tls {
            Tls::Opportunistic(tls_parameters) if conn.can_starttls() => {
                conn.starttls(tls_parameters, &self.info.hello_name)?;
            }
            Tls::Required(tls_parameters) => {
                conn.starttls(tls_parameters, &self.info.hello_name)?;
            }
            _ => (),
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
            conn.auth(&self.info.authentication, credentials)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::transport::smtp::Tls;
    use crate::transport::smtp::test_support::Transcript;
    use crate::{
        LmtpTransport, SmtpTransport,
        transport::smtp::authentication::{
            Credentials, DEFAULT_MECHANISMS, Mechanism, OAUTH2_MECHANISMS, PASSWORD_MECHANISMS,
        },
    };

    use super::{ClientId, Protocol, SmtpClient, SmtpConnection};

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
        assert_eq!(builder.info.protocol, Protocol::Lmtp);
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
    fn password_helper_uses_password_mechanisms() {
        let builder =
            SmtpTransport::builder_dangerous("smtp.example.com").password("username", "password");

        assert!(matches!(
            builder.info.credentials,
            Some(Credentials::Password { ref username, ref password })
                if username == "username" && password.as_str() == "password"
        ));
        assert_eq!(builder.info.authentication, PASSWORD_MECHANISMS);
        // Pin the grown default-construction surface explicitly: SCRAM is now
        // offered out of the box (strongest first), LOGIN is opt-in only.
        assert_eq!(
            builder.info.authentication,
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
        assert_eq!(builder.info.authentication, OAUTH2_MECHANISMS);
        assert_eq!(
            builder.info.authentication,
            [Mechanism::OAuthBearer, Mechanism::Xoauth2]
        );
    }

    #[test]
    fn credentials_preserve_explicit_authentication_mechanisms() {
        let builder = SmtpTransport::builder_dangerous("smtp.example.com")
            .authentication(vec![Mechanism::Xoauth2])
            .oauth2("user@example.com", "token");

        assert_eq!(builder.info.authentication, [Mechanism::Xoauth2]);

        let builder = SmtpTransport::builder_dangerous("smtp.example.com")
            .authentication(vec![Mechanism::Plain])
            .password("username", "password");

        assert_eq!(builder.info.authentication, [Mechanism::Plain]);
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
            SmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Smtp).unwrap();

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
            SmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Smtp).unwrap();

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
            ClientId, LmtpTransport, Pool, PoolConfig, Protocol, SendOptions, SmtpClient,
            SmtpConnection, SmtpInfo, SmtpTransport,
        };

        fn hello() -> ClientId {
            ClientId::Domain("client.example".to_owned())
        }

        /// A pool that will never dial: the only connection it can ever hand
        /// out is the scripted one parked here.
        fn pool_with(conn: SmtpConnection, protocol: Protocol) -> Arc<Pool> {
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
            let conn =
                SmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Smtp)
                    .unwrap();
            let pool = Pool::new(
                PoolConfig::new().max_size(1).test_on_checkout(false),
                SmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Protocol::Smtp),
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
            let conn =
                SmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Smtp)
                    .unwrap();
            let pool = Pool::new(
                PoolConfig::new().max_size(1).test_on_checkout(false),
                SmtpClient {
                    info: SmtpInfo::new("transcript.invalid", Protocol::Smtp),
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
            let conn =
                SmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Lmtp)
                    .unwrap();
            let transport = LmtpTransport {
                inner: pool_with(conn, Protocol::Lmtp),
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
            assert_eq!(builder.info.protocol, Protocol::Lmtp);

            // TLS over a Unix socket is refused inside `connection()`, before
            // any connect syscall, so this branch is reachable hermetically -
            // and proves the Unix funnel, not the TCP one, was entered.
            let mut info = SmtpInfo::new("localhost", Protocol::Lmtp);
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
            let conn =
                SmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Lmtp)
                    .unwrap();
            let pool = pool_with(conn, Protocol::Lmtp);
            let transport = LmtpTransport {
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
            let conn =
                SmtpConnection::from_transcript(transcript.clone(), &hello(), Protocol::Smtp)
                    .unwrap();
            let pool = pool_with(conn, Protocol::Smtp);
            let transport = SmtpTransport {
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
                    info: SmtpInfo::new("transcript.invalid", Protocol::Smtp),
                },
            );
            pool.shutdown();
            let transport = SmtpTransport {
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
