//! Configuration for the shared `Net` HTTP transport.

use std::time::Duration;

/// Default ceiling on a buffered response body: 64 MiB. Chosen to be
/// far above any JSON envelope the protocol crates exchange (the
/// largest realistic one is a JMAP `Email/get` batch with full bodies)
/// and far below a size that threatens the process.
pub const DEFAULT_MAX_BUFFERED_RESPONSE: usize = 64 * 1024 * 1024;

/// Tunable parameters for the underlying reqwest client. Hidden behind
/// an explicit config struct so future migrations off reqwest can
/// remap fields without breaking the public surface.
#[derive(Clone)]
#[non_exhaustive]
pub struct NetConfig {
    /// Proxy overrides. None snapshots system/environment routing at construction.
    /// Some with empty fields disables proxies.
    pub proxy: Option<crate::proxy::ProxyConfig>,
    /// Deadline for DNS resolution and establishing the TCP/TLS
    /// connection. Reqwest can therefore report expiry before any
    /// target-request bytes were transmitted.
    pub connect_timeout: Option<Duration>,
    /// Idle eviction timeout for pooled connections.
    pub pool_idle_timeout: Duration,
    /// Maximum idle connections kept per host.
    pub pool_max_idle_per_host: usize,
    /// HTTP/2 PING frame interval for keepalive on long-lived
    /// connections.
    pub http2_keep_alive_interval: Duration,
    /// HTTP/2 PING ack timeout before the connection is closed.
    pub http2_keep_alive_timeout: Duration,
    /// TCP-level keepalive probe interval.
    pub tcp_keepalive: Duration,
    /// Extra trusted root certificates injected into the native-tls
    /// trust store. Empty in the default; populated by callers that
    /// pin a private CA.
    pub root_certs: Vec<native_tls::Certificate>,
    /// Test-fixture escape hatch. Production callers must never set
    /// this true.
    pub dangerous_accept_invalid_certs: bool,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            proxy: None,
            connect_timeout: Some(Duration::from_secs(10)),
            pool_idle_timeout: Duration::from_secs(60),
            pool_max_idle_per_host: 8,
            http2_keep_alive_interval: Duration::from_secs(30),
            http2_keep_alive_timeout: Duration::from_secs(10),
            tcp_keepalive: Duration::from_secs(60),
            root_certs: Vec::new(),
            dangerous_accept_invalid_certs: false,
        }
    }
}

impl NetConfig {
    /// Construct a `NetConfig` with all defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a native-tls root certificate to the trust store. Returns
    /// `self` for chaining.
    #[must_use]
    pub fn with_root_cert(mut self, cert: native_tls::Certificate) -> Self {
        self.root_certs.push(cert);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_safe_for_production() {
        let config = NetConfig::default();
        assert!(
            !config.dangerous_accept_invalid_certs,
            "the invalid-cert escape hatch must never default on"
        );
        assert!(config.root_certs.is_empty());
        assert!(
            config.http2_keep_alive_timeout < config.http2_keep_alive_interval,
            "the PING ack deadline must be shorter than the PING interval"
        );
    }
}
