use std::fmt::{self, Debug};

use native_tls::{Protocol, TlsConnector};

use crate::transport::smtp::{Error, error};

/// TLS protocol versions.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
// pub: users choose the native-tls minimum version for SMTP connections.
pub enum TlsVersion {
    /// TLS 1.0
    ///
    /// Should only be used when trying to support legacy SMTP servers that
    /// have not updated to at least TLS 1.2 yet.
    Tlsv10,
    /// TLS 1.1
    ///
    /// Should only be used when trying to support legacy SMTP servers that
    /// have not updated to at least TLS 1.2 yet.
    Tlsv11,
    /// TLS 1.2
    ///
    /// A good option for most SMTP servers.
    Tlsv12,
    /// TLS 1.3
    ///
    /// This is not configurable through native-tls and will return an error
    /// if used as the minimum version.
    Tlsv13,
}

/// Specifies how to establish a TLS connection.
///
/// Use [`Tls::Wrapper`] or [`Tls::Required`] when connecting to a remote
/// server, and [`Tls::None`] when connecting to a trusted local server.
#[derive(Clone)]
#[allow(missing_copy_implementations)]
// pub: users choose plaintext, STARTTLS, required STARTTLS, or wrapper TLS.
pub enum Tls {
    /// Insecure plaintext connection only.
    ///
    /// This option should only be used for trusted local relays. Remote
    /// servers can reject it, and credentials or messages sent over it are
    /// observable on the network.
    None,
    /// Begin with a plaintext connection and attempt to use `STARTTLS` if
    /// available.
    ///
    /// This is kept for compatibility with servers that advertise STARTTLS
    /// opportunistically. Prefer [`Tls::Required`] or [`Tls::Wrapper`] for
    /// authenticated remote SMTP.
    Opportunistic(TlsParameters),
    /// Begin with a plaintext connection and require `STARTTLS` before
    /// transmitting credentials or messages.
    Required(TlsParameters),
    /// Establish a TLS connection immediately.
    Wrapper(TlsParameters),
}

impl Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::None => f.pad("None"),
            Self::Opportunistic(_) => f.pad("Opportunistic"),
            Self::Required(_) => f.pad("Required"),
            Self::Wrapper(_) => f.pad("Wrapper"),
        }
    }
}

/// Source for the base set of root certificates to trust.
#[allow(missing_copy_implementations)]
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
// pub: users choose platform roots or an explicit custom root set.
pub enum CertificateStore {
    /// Use the platform certificate store selected by native-tls.
    ///
    /// Since bifrost-smtp only supports native-tls, this variant always means
    /// the native-tls platform verifier rather than a backend-dependent
    /// default.
    ///
    /// This uses schannel on Windows, Security-Framework on macOS, and
    /// OpenSSL directories on Linux.
    #[default]
    Default,
    /// Do not use any system certificates.
    None,
}

/// Parameters to use for secure clients.
#[derive(Clone)]
// pub: SMTP owns TLS parameters because STARTTLS upgrades raw TCP, not HTTP.
pub struct TlsParameters {
    pub(crate) connector: TlsConnector,
    /// The domain name expected in the server TLS certificate.
    pub(super) domain: String,
}

/// Builder for [`TlsParameters`].
#[derive(Debug, Clone)]
// pub: users configure native-tls roots, identity, and validation policy.
pub struct TlsParametersBuilder {
    domain: String,
    cert_store: CertificateStore,
    root_certs: Vec<Certificate>,
    identity: Option<Identity>,
    accept_invalid_hostnames: bool,
    accept_invalid_certs: bool,
    min_tls_version: TlsVersion,
}

impl TlsParametersBuilder {
    /// Creates a new builder for [`TlsParameters`].
    pub fn new(domain: String) -> Self {
        Self {
            domain,
            cert_store: CertificateStore::Default,
            root_certs: Vec::new(),
            identity: None,
            accept_invalid_hostnames: false,
            accept_invalid_certs: false,
            min_tls_version: TlsVersion::Tlsv12,
        }
    }

    /// Set the source for the base set of root certificates to trust.
    pub fn certificate_store(mut self, cert_store: CertificateStore) -> Self {
        self.cert_store = cert_store;
        self
    }

    /// Add a custom root certificate.
    ///
    /// Can be used to safely connect to a server using a self-signed
    /// certificate, for example.
    pub fn add_root_certificate(mut self, cert: Certificate) -> Self {
        self.root_certs.push(cert);
        self
    }

    /// Add a client certificate.
    pub fn identify_with(mut self, identity: Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Controls whether certificates with an invalid hostname are accepted.
    ///
    /// Defaults to `false`.
    ///
    /// You should think very carefully before using this method. If hostname
    /// verification is disabled, any valid certificate, including those from
    /// other sites, is trusted.
    pub fn dangerous_accept_invalid_hostnames(mut self, accept_invalid_hostnames: bool) -> Self {
        self.accept_invalid_hostnames = accept_invalid_hostnames;
        self
    }

    /// Controls which minimum TLS version is allowed.
    ///
    /// Defaults to [`TlsVersion::Tlsv12`].
    pub fn set_min_tls_version(mut self, min_tls_version: TlsVersion) -> Self {
        self.min_tls_version = min_tls_version;
        self
    }

    /// Controls whether invalid certificates are accepted.
    ///
    /// Defaults to `false`.
    ///
    /// This should only be used as a last resort, as it accepts self-signed,
    /// wrong-host, and expired certificates.
    pub fn dangerous_accept_invalid_certs(mut self, accept_invalid_certs: bool) -> Self {
        self.accept_invalid_certs = accept_invalid_certs;
        self
    }

    /// Creates a new [`TlsParameters`] using native-tls.
    pub fn build(self) -> Result<TlsParameters, Error> {
        let mut tls_builder = TlsConnector::builder();

        match self.cert_store {
            CertificateStore::Default => {}
            CertificateStore::None => {
                tls_builder.disable_built_in_roots(true);
            }
        }

        for cert in self.root_certs {
            tls_builder.add_root_certificate(cert.native_tls);
        }

        tls_builder.danger_accept_invalid_hostnames(self.accept_invalid_hostnames);
        tls_builder.danger_accept_invalid_certs(self.accept_invalid_certs);

        let min_tls_version = match self.min_tls_version {
            TlsVersion::Tlsv10 => Protocol::Tlsv10,
            TlsVersion::Tlsv11 => Protocol::Tlsv11,
            TlsVersion::Tlsv12 => Protocol::Tlsv12,
            TlsVersion::Tlsv13 => {
                return Err(error::invalid_input(
                    "min tls version Tlsv13 is not supported by native-tls",
                ));
            }
        };

        tls_builder.min_protocol_version(Some(min_tls_version));
        if let Some(identity) = self.identity {
            tls_builder.identity(identity.native_tls);
        }

        // Construction failures need configuration repair, not a retry of the
        // user's queued send. Handshake failures remain transport TLS errors.
        let connector = tls_builder.build().map_err(error::invalid_input)?;
        Ok(TlsParameters {
            connector,
            domain: self.domain,
        })
    }
}

impl TlsParameters {
    /// Creates a new [`TlsParameters`] using native-tls.
    pub fn new(domain: String) -> Result<Self, Error> {
        TlsParametersBuilder::new(domain).build()
    }

    /// Creates a new [`TlsParameters`] builder.
    pub fn builder(domain: String) -> TlsParametersBuilder {
        TlsParametersBuilder::new(domain)
    }

    /// Creates new [`TlsParameters`] from an existing native-tls connector.
    ///
    /// This is useful when callers need native-tls settings that are not
    /// mirrored directly by [`TlsParametersBuilder`].
    pub fn from_inner(domain: String, connector: TlsConnector) -> Self {
        Self { connector, domain }
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }
}

impl From<(String, TlsConnector)> for TlsParameters {
    fn from((domain, connector): (String, TlsConnector)) -> Self {
        Self::from_inner(domain, connector)
    }
}

/// A certificate that can be used with
/// [`TlsParametersBuilder::add_root_certificate`].
#[derive(Clone)]
#[allow(missing_copy_implementations)]
// pub: users pass native-tls certificates without depending on internals.
pub struct Certificate {
    native_tls: native_tls::Certificate,
}

impl Certificate {
    /// Create a [`Certificate`] from a DER encoded certificate.
    pub fn from_der(der: Vec<u8>) -> Result<Self, Error> {
        Ok(Self {
            native_tls: native_tls::Certificate::from_der(&der).map_err(error::invalid_input)?,
        })
    }

    /// Create a [`Certificate`] from a PEM encoded certificate.
    pub fn from_pem(pem: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            native_tls: native_tls::Certificate::from_pem(pem).map_err(error::invalid_input)?,
        })
    }

    /// Create a [`Certificate`] from an existing native-tls certificate.
    pub fn from_inner(native_tls: native_tls::Certificate) -> Self {
        Self { native_tls }
    }
}

impl From<native_tls::Certificate> for Certificate {
    fn from(native_tls: native_tls::Certificate) -> Self {
        Self::from_inner(native_tls)
    }
}

impl Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Certificate").finish()
    }
}

/// An identity that can be used with [`TlsParametersBuilder::identify_with`].
#[derive(Clone)]
#[allow(missing_copy_implementations)]
// pub: users configure client certificates for SMTP TLS.
pub struct Identity {
    native_tls: native_tls::Identity,
}

impl Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity").finish()
    }
}

impl Identity {
    pub fn from_pem(pem: &[u8], key: &[u8]) -> Result<Self, Error> {
        Ok(Self {
            native_tls: native_tls::Identity::from_pkcs8(pem, key).map_err(error::invalid_input)?,
        })
    }

    /// Create an [`Identity`] from an existing native-tls identity.
    pub fn from_inner(native_tls: native_tls::Identity) -> Self {
        Self { native_tls }
    }
}

impl From<native_tls::Identity> for Identity {
    fn from(native_tls: native_tls::Identity) -> Self {
        Self::from_inner(native_tls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_parameters_can_wrap_native_tls_connector() {
        let connector = native_tls::TlsConnector::builder().build().unwrap();
        let parameters = TlsParameters::from(("smtp.example.com".to_owned(), connector));

        assert_eq!(parameters.domain(), "smtp.example.com");
    }
}

#[cfg(test)]
mod configuration_tests {
    use super::*;

    #[test]
    fn malformed_security_material_is_invalid_input() {
        assert!(
            Certificate::from_der(b"invalid".to_vec())
                .unwrap_err()
                .is_invalid_input()
        );
        assert!(
            Certificate::from_pem(b"invalid")
                .unwrap_err()
                .is_invalid_input()
        );
        assert!(
            Identity::from_pem(b"invalid", b"invalid")
                .unwrap_err()
                .is_invalid_input()
        );
    }
}
