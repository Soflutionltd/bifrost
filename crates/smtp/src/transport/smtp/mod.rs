//! The SMTP transport sends emails using the SMTP protocol.
//!
//! This SMTP client follows [RFC
//! 5321](https://tools.ietf.org/html/rfc5321), and is designed to efficiently send emails from an
//! application to a relay email server, as it relies as much as possible on the relay server
//! for sanity and RFC compliance checks.
//!
//! It implements the following extensions:
//!
//! * 8BITMIME ([RFC 6152](https://tools.ietf.org/html/rfc6152))
//! * AUTH ([RFC 4954](https://tools.ietf.org/html/rfc4954)) with PLAIN, LOGIN, OAUTHBEARER and XOAUTH2 mechanisms
//! * BDAT/CHUNKING ([RFC 3030](https://www.rfc-editor.org/rfc/rfc3030)) through explicit BDAT send methods
//! * DELIVERBY ([RFC 2852](https://www.rfc-editor.org/rfc/rfc2852)) through per-message send options
//! * DSN ([RFC 3461](https://www.rfc-editor.org/rfc/rfc3461)) through per-message send options
//! * ENHANCEDSTATUSCODES ([RFC 2034](https://www.rfc-editor.org/rfc/rfc2034)) response parsing
//! * FUTURERELEASE ([RFC 4865](https://www.rfc-editor.org/rfc/rfc4865)) through per-message send options
//! * MT-PRIORITY ([RFC 6710](https://www.rfc-editor.org/rfc/rfc6710)) through per-message send options
//! * PIPELINING ([RFC 2920](https://www.rfc-editor.org/rfc/rfc2920)) for MAIL and RCPT commands
//! * REQUIRETLS ([RFC 8689](https://www.rfc-editor.org/rfc/rfc8689)) through per-message send options
//! * STARTTLS ([RFC 2487](https://tools.ietf.org/html/rfc2487))
//!
//! #### SMTP Transport
//!
//! This transport uses the SMTP protocol to send emails over the network (locally or remotely).
//!
//! It is designed to be:
//!
//! * Secured: connections are encrypted by default
//! * Modern: unicode support for email contents and sender/recipient addresses when compatible
//! * Fast: supports connection reuse and pooling
//!
//! [`LmtpTransport`] and [`AsyncLmtpTransport`] provide the same transport
//! shape for local delivery over LMTP, returning one status per recipient.
//! They support both TCP LMTP and Unix-domain LMTP sockets on Unix platforms.
//!
//! This client is designed to send emails to a relay server, and should *not* be used to send
//! emails directly to the destination server.
//!
//! The relay server can be the local email server, a specific host or a third-party service.
//!
//! #### Simple example with authentication
//!
//! A good starting point for sending emails via SMTP relay is to
//! do the following:
//!
//! ```rust,no_run
//! # fn test() -> Result<(), Box<dyn std::error::Error>> {
//! use bifrost_smtp::{
//!     Message, SmtpTransport, Transport,
//!     message::header::ContentType,
//!     transport::smtp::authentication::{Credentials, Mechanism},
//! };
//!
//! let email = Message::builder()
//!     .from("NoBody <nobody@domain.tld>".parse()?)
//!     .reply_to("Yuin <yuin@domain.tld>".parse()?)
//!     .to("Hei <hei@domain.tld>".parse()?)
//!     .subject("Happy new year")
//!     .header(ContentType::TEXT_PLAIN)
//!     .body(String::from("Be happy!"))?;
//!
//! // Create the SMTPS transport
//! let sender = SmtpTransport::relay("smtp.example.com")?
//!     // Add credentials for authentication
//!     .password("username", "password")
//!     // Optionally configure expected authentication mechanism
//!     .authentication(vec![Mechanism::Plain])
//!     .build();
//!
//! // Send the email via remote relay
//! sender.send(&email)?;
//! # Ok(())
//! # }
//! ```
//!
//! #### Shortening configuration
//!
//! It can be very repetitive to ask the user for every SMTP connection parameter.
//! In some cases this can be simplified by using a connection URI instead.
//!
//! For more information take a look at [`SmtpTransport::from_url`] or [`AsyncSmtpTransport::from_url`].
//!
//! ```rust,no_run
//! # fn test() -> Result<(), Box<dyn std::error::Error>> {
//! use bifrost_smtp::{
//!     Message, SmtpTransport, Transport,
//!     message::header::ContentType,
//!     transport::smtp::authentication::Mechanism,
//! };
//!
//! let email = Message::builder()
//!     .from("NoBody <nobody@domain.tld>".parse()?)
//!     .reply_to("Yuin <yuin@domain.tld>".parse()?)
//!     .to("Hei <hei@domain.tld>".parse()?)
//!     .subject("Happy new year")
//!     .header(ContentType::TEXT_PLAIN)
//!     .body(String::from("Be happy!"))?;
//!
//! // Create the SMTPS transport
//! let sender = SmtpTransport::from_url("smtps://username:password@smtp.example.com")?.build();
//!
//! // Send the email via remote relay
//! sender.send(&email)?;
//! # Ok(())
//! # }
//! ```
//!
//! #### Advanced configuration with custom TLS settings
//!
//! ```rust,no_run
//! # fn test() -> Result<(), Box<dyn std::error::Error>> {
//! use std::fs;
//!
//! use bifrost_smtp::{
//!     Message, SmtpTransport, Transport,
//!     message::header::ContentType,
//!     transport::smtp::{Certificate, Tls, TlsParameters},
//! };
//!
//! let email = Message::builder()
//!     .from("NoBody <nobody@domain.tld>".parse()?)
//!     .reply_to("Yuin <yuin@domain.tld>".parse()?)
//!     .to("Hei <hei@domain.tld>".parse()?)
//!     .subject("Happy new year")
//!     .header(ContentType::TEXT_PLAIN)
//!     .body(String::from("Be happy!"))?;
//!
//! // Custom TLS configuration - Use a self signed certificate
//! let cert = fs::read("self-signed.crt")?;
//! let cert = Certificate::from_pem(&cert)?;
//! let tls = TlsParameters::builder(/* TLS SNI value */ "smtp.example.com".to_owned())
//!     .add_root_certificate(cert)
//!     .build()?;
//!
//! // Create the SMTPS transport
//! let sender = SmtpTransport::relay("smtp.example.com")?
//!     .tls(Tls::Wrapper(tls))
//!     .build();
//!
//! // Send the email via remote relay
//! sender.send(&email)?;
//! # Ok(())
//! # }
//! ```
//!
//! #### Connection pooling
//!
//! [`SmtpTransport`] and [`AsyncSmtpTransport`] store connections in
//! a connection pool by default. This avoids connecting and disconnecting
//! from the relay server for every message the application tries to send. For the connection pool
//! to work the instance of the transport **must** be reused.
//! In a webserver context it may go about this:
//!
//! ```rust,no_run
//! # fn test() {
//! use bifrost_smtp::{
//!     Message, SmtpTransport, Transport,
//!     message::header::ContentType,
//! };
//! #
//! # type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
//!
//! /// The global application state
//! #[derive(Debug)]
//! struct AppState {
//!     smtp: SmtpTransport,
//!     // ... other global application parameters
//! }
//!
//! impl AppState {
//!     pub fn new(smtp_url: &str) -> Result<Self> {
//!         let smtp = SmtpTransport::from_url(smtp_url)?.build();
//!         Ok(Self { smtp })
//!     }
//! }
//!
//! fn handle_request(app_state: &AppState) -> Result<String> {
//!     let email = Message::builder()
//!         .from("NoBody <nobody@domain.tld>".parse()?)
//!         .reply_to("Yuin <yuin@domain.tld>".parse()?)
//!         .to("Hei <hei@domain.tld>".parse()?)
//!         .subject("Happy new year")
//!         .header(ContentType::TEXT_PLAIN)
//!         .body(String::from("Be happy!"))?;
//!
//!     // Send the email via remote relay
//!     app_state.smtp.send(&email)?;
//!
//!     Ok("The email has successfully been sent!".to_owned())
//! }
//! # }
//! ```

use std::{path::PathBuf, time::Duration};

pub(crate) use self::client::WireMetering;
// pub: the "no cap" sentinel for `bandwidth_metering`.
pub use self::client::UNLIMITED_BANDWIDTH;

pub(crate) use bifrost_types::error::Protocol;

#[cfg(feature = "tokio")]
// pub: async SMTP and LMTP transports are the tokio user-facing API.
pub use self::async_transport::{
    AsyncLmtpTransport, AsyncLmtpTransportBuilder, AsyncSmtpTransport, AsyncSmtpTransportBuilder,
};
#[cfg(feature = "tokio")]
pub(crate) use self::client::AsyncSmtpConnection;
pub(crate) use self::client::SmtpConnection;
// pub: callers configure SMTP TLS roots and client identities explicitly.
pub use self::client::{Certificate, Identity};
// pub: callers choose SMTP TLS mode, SNI, and native-tls parameters.
pub use self::client::{CertificateStore, Tls, TlsParameters, TlsParametersBuilder, TlsVersion};
// pub: callers tune connection pooling on transport builders.
pub use self::pool::PoolConfig;
// pub: SMTP transports return rich protocol errors, send options, and builders.
pub use self::{
    error::{Error, ErrorKind},
    extension::SendOptions,
    transport::{LmtpTransport, LmtpTransportBuilder, SmtpTransport, SmtpTransportBuilder},
};
use crate::transport::smtp::{
    authentication::{Credentials, DEFAULT_MECHANISMS, Mechanism},
    extension::ClientId,
    response::Response,
};

mod account_error;
#[cfg(feature = "tokio")]
mod async_transport;
// pub: users select credential kinds and explicit SASL mechanisms.
pub mod authentication;
mod batch;
mod client;
mod commands;
mod connection_url;
pub(crate) mod error;
// pub: users build typed ESMTP send options and parameters.
pub mod extension;
mod pool;
// pub: transport results and errors expose typed SMTP replies.
pub mod response;
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod test_support;
#[cfg(feature = "test-support")]
pub use test_support::{ScriptedConnector, Transcript};
mod transport;
pub(super) mod util;

// Registered port numbers:
// https://www.iana.
// org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml

/// Default smtp port
// pub: callers use standard SMTP ports when building custom configs.
pub const SMTP_PORT: u16 = 25;
/// Common LMTP TCP port.
///
/// RFC 2033 does not assign a TCP port for LMTP. Port 24 is the closest
/// deployed convention, notably used by Dovecot for TCP LMTP listeners.
// pub: callers use the deployed LMTP TCP convention for local delivery.
pub const LMTP_PORT: u16 = 24;
/// Default submission port
// pub: callers use the standard message-submission port in configs.
pub const SUBMISSION_PORT: u16 = 587;
/// Default submission over TLS port
///
/// Defined in [RFC8314](https://tools.ietf.org/html/rfc8314)
// pub: callers use the standard implicit-TLS submission port in configs.
pub const SUBMISSIONS_PORT: u16 = 465;

/// Default timeout
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct SmtpInfo {
    /// Wire protocol used for this connection.
    protocol: Protocol,
    /// Name sent during EHLO
    hello_name: ClientId,
    /// Server we are connecting to
    server: String,
    /// Port to connect to
    port: u16,
    /// Unix-domain socket path for local LMTP delivery.
    unix_socket: Option<PathBuf>,
    /// TLS security configuration
    tls: Tls,
    /// Optional enforced authentication mechanism
    authentication: Vec<Mechanism>,
    /// Whether the authentication mechanism list was explicitly configured.
    authentication_configured: bool,
    /// Credentials
    credentials: Option<Credentials>,
    /// Allow AUTH over an unencrypted connection.
    allow_insecure_auth: bool,
    /// Define network timeout
    /// It can be changed later for specific needs (like a different timeout for each SMTP command)
    timeout: Option<Duration>,
    /// Per-account byte accounting and bandwidth cap, applied to every
    /// connection this transport dials. `disabled()` unless a consumer
    /// supplied one, which is the shape every non-account caller gets.
    ///
    /// Cloned into each connection rather than owned by it, so a pooled
    /// transport's connections share ONE budget instead of each getting
    /// the full cap.
    metering: WireMetering,
    #[cfg(feature = "test-support")]
    connector: Option<test_support::ScriptedConnector>,
}

impl Default for SmtpInfo {
    fn default() -> Self {
        Self {
            protocol: Protocol::Smtp,
            server: "localhost".to_owned(),
            port: SMTP_PORT,
            unix_socket: None,
            hello_name: ClientId::default(),
            credentials: None,
            allow_insecure_auth: false,
            authentication: DEFAULT_MECHANISMS.into(),
            authentication_configured: false,
            timeout: Some(DEFAULT_TIMEOUT),
            tls: Tls::None,
            metering: WireMetering::disabled(),
            #[cfg(feature = "test-support")]
            connector: None,
        }
    }
}

impl SmtpInfo {
    fn new<T: Into<String>>(server: T, protocol: Protocol) -> Self {
        let port = match protocol {
            Protocol::Smtp => SMTP_PORT,
            Protocol::Lmtp => LMTP_PORT,
            _ => SMTP_PORT,
        };
        Self {
            protocol,
            server: server.into(),
            port,
            ..Default::default()
        }
    }

    fn set_credentials(&mut self, credentials: Credentials) {
        if !self.authentication_configured {
            self.authentication = credentials.preferred_mechanisms().into();
        }
        self.credentials = Some(credentials);
    }

    fn set_authentication(&mut self, mechanisms: Vec<Mechanism>) {
        self.authentication = mechanisms;
        self.authentication_configured = true;
    }

    fn uses_tls(&self) -> bool {
        !matches!(self.tls, Tls::None)
    }

    fn ensure_can_authenticate(&self, encrypted: bool) -> Result<(), Error> {
        if encrypted || self.allow_insecure_auth {
            Ok(())
        } else {
            let protocol = match self.protocol {
                Protocol::Smtp => "SMTP",
                Protocol::Lmtp => "LMTP",
                _ => "SMTP",
            };
            Err(error::policy(format!(
                "refusing to authenticate over an unencrypted {protocol} connection"
            )))
        }
    }
}
