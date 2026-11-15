use std::borrow::Cow;

use url::Url;

#[cfg(feature = "tokio")]
use super::AsyncSmtpTransportBuilder;
use super::{Error, SmtpTransportBuilder, authentication::Credentials, error, extension::ClientId};
use super::{PortTls, default_port};
use super::{Tls, TlsParameters};

pub(crate) trait TransportBuilder {
    fn new<T: Into<String>>(server: T) -> Self;
    fn tls(self, tls: super::Tls) -> Self;
    fn port(self, port: u16) -> Self;
    fn credentials(self, credentials: Credentials) -> Self;
    fn hello_name(self, name: ClientId) -> Self;
}

impl TransportBuilder for SmtpTransportBuilder {
    fn new<T: Into<String>>(server: T) -> Self {
        Self::new(server)
    }

    fn tls(self, tls: super::Tls) -> Self {
        self.tls(tls)
    }

    fn port(self, port: u16) -> Self {
        self.port(port)
    }

    fn credentials(self, credentials: Credentials) -> Self {
        self.credentials(credentials)
    }

    fn hello_name(self, name: ClientId) -> Self {
        self.hello_name(name)
    }
}

#[cfg(feature = "tokio")]
impl TransportBuilder for AsyncSmtpTransportBuilder {
    fn new<T: Into<String>>(server: T) -> Self {
        Self::new(server)
    }

    fn tls(self, tls: super::Tls) -> Self {
        self.tls(tls)
    }

    fn port(self, port: u16) -> Self {
        self.port(port)
    }

    fn credentials(self, credentials: Credentials) -> Self {
        self.credentials(credentials)
    }

    fn hello_name(self, name: ClientId) -> Self {
        self.hello_name(name)
    }
}

/// Create a new `SmtpTransportBuilder` or `AsyncSmtpTransportBuilder` from a connection URL
pub(crate) fn from_connection_url<B: TransportBuilder>(connection_url: &str) -> Result<B, Error> {
    // These are local configuration faults. Retrying them like network outages
    // would leave an enterprise user's send queue stuck until manual repair.
    let connection_url = Url::parse(connection_url).map_err(error::invalid_input)?;
    let tls: Option<String> = connection_url
        .query_pairs()
        .find(|(k, _)| k == "tls")
        .map(|(_, v)| v.to_string());

    let host = connection_url
        .host_str()
        // `smtp://` is a non-special scheme, so WHATWG parsing yields an
        // EMPTY host rather than none; both are the same missing host.
        .filter(|host| !host.is_empty())
        .ok_or_else(|| error::invalid_input("smtp host undefined"))?;

    let mut builder = B::new(host);

    match (connection_url.scheme(), tls.as_deref()) {
        ("smtp", None) => {
            builder = builder.port(connection_url.port().unwrap_or(default_port(
                bifrost_types::error::Protocol::Smtp,
                PortTls::Plaintext,
            )));
        }
        ("smtp", Some("required")) => {
            builder = builder
                .port(connection_url.port().unwrap_or(default_port(
                    bifrost_types::error::Protocol::Smtp,
                    PortTls::StartTls,
                )))
                .tls(Tls::Required(TlsParameters::new(host.into())?));
        }
        ("smtp", Some("opportunistic")) => {
            builder = builder
                .port(connection_url.port().unwrap_or(default_port(
                    bifrost_types::error::Protocol::Smtp,
                    PortTls::StartTls,
                )))
                .tls(Tls::Opportunistic(TlsParameters::new(host.into())?));
        }
        ("smtps", _) => {
            builder = builder
                .port(connection_url.port().unwrap_or(default_port(
                    bifrost_types::error::Protocol::Smtp,
                    PortTls::Implicit,
                )))
                .tls(Tls::Wrapper(TlsParameters::new(host.into())?));
        }
        (scheme, tls) => {
            return Err(error::invalid_input(format!(
                "Unknown scheme '{scheme}' or tls parameter '{tls:?}'"
            )));
        }
    }

    let percent_decode = |s: &str| {
        for (index, byte) in s.bytes().enumerate() {
            if byte == b'%'
                && !s
                    .as_bytes()
                    .get(index + 1..index + 3)
                    .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit))
            {
                return Err(error::invalid_input(
                    "invalid percent escape in connection URL",
                ));
            }
        }
        percent_encoding::percent_decode_str(s)
            .decode_utf8()
            .map(Cow::into_owned)
            .map_err(error::invalid_input)
    };

    // use the path segment of the URL as name in the name in the HELO / EHLO command
    if connection_url.path().len() > 1 {
        let name = percent_decode(connection_url.path().trim_matches('/'))?;
        builder = builder.hello_name(ClientId::domain(name)?);
    }

    // A username without a password is still a credential intent: refuse it
    // loudly rather than silently connecting unauthenticated.
    match (connection_url.username(), connection_url.password()) {
        (_, Some(password)) => {
            let credentials = Credentials::password(
                percent_decode(connection_url.username())?,
                percent_decode(password)?,
            );
            builder = builder.credentials(credentials);
        }
        ("", None) => {}
        (_, None) => {
            return Err(error::invalid_input(
                "connection URL has a username but no password",
            ));
        }
    }

    Ok(builder)
}
