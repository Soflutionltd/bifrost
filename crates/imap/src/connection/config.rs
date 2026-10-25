#![allow(clippy::wildcard_imports)]
use super::*;

/// Connection configuration for [`ImapConnection`].
#[non_exhaustive]
#[derive(Clone)]
pub struct ImapConfig {
    /// Server hostname.
    pub(crate) host: String,
    /// Server port.
    pub(crate) port: u16,
    /// TLS policy.
    pub(crate) tls_mode: TlsMode,
    /// Timeout for connect and initial greeting/capability negotiation.
    pub(crate) connect_timeout: Duration,
    /// Timeout used for IMAP commands during account open and runtime calls.
    pub(crate) command_timeout: Duration,
    /// Optional TCP keepalive configuration.
    pub(crate) keepalive: Option<TcpKeepalive>,
    /// Optional custom native-tls connector.
    pub(crate) tls_connector: Option<native_tls::TlsConnector>,
}

impl ImapConfig {
    /// Create an implicit-TLS IMAP configuration for port 993.
    pub fn tls(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 993,
            tls_mode: TlsMode::Implicit,
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(60),
            keepalive: Some(TcpKeepalive::new(
                Duration::from_secs(120),
                Duration::from_secs(60),
            )),
            tls_connector: None,
        }
    }

    /// Create a STARTTLS IMAP configuration for port 143.
    pub fn starttls(host: impl Into<String>) -> Self {
        Self {
            tls_mode: TlsMode::StartTls,
            port: 143,
            ..Self::tls(host)
        }
    }

    /// Create a plaintext IMAP configuration for port 143.
    pub fn plaintext(host: impl Into<String>) -> Self {
        Self {
            tls_mode: TlsMode::None,
            port: 143,
            ..Self::tls(host)
        }
    }

    /// Override the port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Override the connect timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Override the default command timeout.
    pub fn with_command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Disable TCP keepalive.
    pub fn without_keepalive(mut self) -> Self {
        self.keepalive = None;
        self
    }

    /// Use a custom native-tls connector.
    pub fn with_tls_connector(mut self, connector: native_tls::TlsConnector) -> Self {
        self.tls_connector = Some(connector);
        self
    }

    pub(crate) async fn connect_authenticated_metered(
        &self,
        credentials: &crate::types::Credentials,
        policy: &crate::types::AuthPolicy,
        meter_sink: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_budget: Option<bifrost_net::BandwidthBudget>,
    ) -> Result<(ImapConnection, crate::types::AuthOutcome), Error> {
        let conn = ImapConnection::connect_config_metered(
            self,
            meter_sink,
            bandwidth_budget,
            &super::LeaseHandoff::none(),
        )
        .await?;
        let outcome = conn
            .authenticate_best(credentials, policy, self.command_timeout)
            .await?;
        Ok((conn, outcome))
    }
}

impl std::fmt::Debug for ImapConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImapConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls_mode", &self.tls_mode)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("keepalive", &self.keepalive)
            .field(
                "tls_connector",
                &self.tls_connector.as_ref().map(|_| "<custom>"),
            )
            .finish()
    }
}

impl ImapConnection {
    pub(crate) async fn connect_config_metered(
        config: &ImapConfig,
        meter_sink: Option<bifrost_net::MeterSinkHandle>,
        bandwidth_budget: Option<bifrost_net::BandwidthBudget>,
        leases: &super::LeaseHandoff,
    ) -> Result<Self, Error> {
        let conn = if let Some(connector) = &config.tls_connector {
            Self::connect_with_tls_connector_metered(
                &config.host,
                config.port,
                config.tls_mode,
                connector.clone(),
                config.connect_timeout,
                meter_sink,
                bandwidth_budget,
                leases,
            )
            .await?
        } else {
            Self::connect_with_tls_connector_metered(
                &config.host,
                config.port,
                config.tls_mode,
                build_default_tls_connector()?,
                config.connect_timeout,
                meter_sink,
                bandwidth_budget,
                leases,
            )
            .await?
        };

        if let Some(keepalive) = config.keepalive {
            setup_keepalive_cancel_safe(&conn, conn.set_keepalive(keepalive)).await?;
        }

        Ok(conn)
    }
}

async fn setup_keepalive_cancel_safe<Fut>(conn: &ImapConnection, setup: Fut) -> Result<(), Error>
where
    Fut: std::future::Future<Output = Result<(), Error>>,
{
    // The driver already owns the socket here. If this direct setup caller is
    // cancelled while the keepalive command is queued, dropping the last
    // connection handle must abort that driver instead of detaching it.
    let guard = conn.in_flight();
    let result = setup.await;
    if result.is_ok() {
        guard.completed();
    }
    result
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
