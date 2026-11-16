use std::borrow::Cow;

use url::Url;

use super::surface::{Builder, Smtp};
use super::{Error, authentication::Credentials, error, extension::ClientId};
use super::{PortTls, Tls, TlsParameters, default_port};

/// Parse SMTP configuration for either runtime.
pub(crate) fn from_connection_url<R>(connection_url: &str) -> Result<Builder<Smtp, R>, Error> {
    // These are local configuration faults. Retrying them like network outages
    // would leave an enterprise user's send queue stuck until manual repair.
    let connection_url = Url::parse(connection_url).map_err(error::invalid_input)?;
    let mut tls = None;
    for (key, value) in connection_url.query_pairs() {
        if key == "tls" {
            // An administrator expects ambiguous security settings to be
            // refused, even when the repeated values happen to agree.
            if tls.is_some() {
                return Err(error::invalid_input("duplicate tls parameter"));
            }
            tls = Some(value.into_owned());
        }
    }

    let host = connection_url
        .host_str()
        // `smtp://` is a non-special scheme, so WHATWG parsing yields an
        // EMPTY host rather than none; both are the same missing host.
        .filter(|host| !host.is_empty())
        .ok_or_else(|| error::invalid_input("smtp host undefined"))?;

    let mut builder = Builder::new(host);

    match (connection_url.scheme(), tls.as_deref()) {
        ("smtp", None) => {
            builder = builder.port(connection_url.port().unwrap_or(default_port(
                crate::transport::smtp::Dialect::Smtp,
                PortTls::None,
            )));
        }
        ("smtp", Some("required")) => {
            builder = builder
                .port(connection_url.port().unwrap_or(default_port(
                    crate::transport::smtp::Dialect::Smtp,
                    PortTls::StartTls,
                )))
                .tls(Tls::Required(TlsParameters::new(host.into())?));
        }
        ("smtp", Some("opportunistic")) => {
            builder = builder
                .port(connection_url.port().unwrap_or(default_port(
                    crate::transport::smtp::Dialect::Smtp,
                    PortTls::StartTls,
                )))
                .tls(Tls::Opportunistic(TlsParameters::new(host.into())?));
        }
        // Implicit TLS stays selected, but an administrator must still learn
        // about a misspelled security setting rather than have it ignored.
        ("smtps", None | Some("required" | "opportunistic")) => {
            builder = builder
                .port(connection_url.port().unwrap_or(default_port(
                    crate::transport::smtp::Dialect::Smtp,
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

#[cfg(test)]
mod tests {
    use super::super::{SmtpTransport, Tls};

    #[test]
    fn both_schemes_refuse_duplicate_tls_parameters() {
        for scheme in ["smtp", "smtps"] {
            for query in [
                "tls=required&tls=wrong",
                "tls=required&tls=opportunistic",
                "tls=opportunistic&tls=required",
                "tls=required&tls=required",
                "tls=required&tls=",
                "tls=required&tls",
                "tls=required&ignored=value&%74ls=required",
                "%74ls=opportunistic&tls=required",
            ] {
                let url = format!("{scheme}://relay.example?{query}");
                assert!(
                    SmtpTransport::from_url(&url)
                        .unwrap_err()
                        .is_invalid_input(),
                    "{url}"
                );
                #[cfg(feature = "tokio")]
                assert!(
                    super::super::AsyncSmtpTransport::from_url(&url)
                        .unwrap_err()
                        .is_invalid_input(),
                    "{url}"
                );
            }
        }
    }

    #[test]
    fn both_schemes_refuse_unrecognized_tls_values() {
        for scheme in ["smtp", "smtps"] {
            for value in ["wrong", "", "REQUIRED", "false"] {
                let url = format!("{scheme}://relay.example?tls={value}");
                assert!(
                    SmtpTransport::from_url(&url)
                        .unwrap_err()
                        .is_invalid_input()
                );
                #[cfg(feature = "tokio")]
                assert!(
                    super::super::AsyncSmtpTransport::from_url(&url)
                        .unwrap_err()
                        .is_invalid_input()
                );
            }
        }
    }

    #[test]
    fn recognized_smtps_tls_values_retain_implicit_tls() {
        for query in ["", "?tls=required", "?tls=opportunistic"] {
            let builder =
                SmtpTransport::from_url(&format!("smtps://relay.example{query}")).unwrap();
            assert!(matches!(builder.info.tls, Tls::Wrapper(_)));
            assert_eq!(builder.info.port, 465);
        }
    }
}
