//! Construction-time proxy routing shared by HTTP and protocol upgrades.

use std::io;
use std::pin::Pin;
use std::sync::Arc;

use base64::Engine;
use hyper_util::client::proxy::matcher::Matcher;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Explicit proxy routing. Empty fields select direct connections.
/// Values use the same syntax as HTTP_PROXY, HTTPS_PROXY and NO_PROXY.
///
/// ```
/// let mut proxy = bifrost_net::proxy::ProxyConfig::new();
/// proxy.https = "http://proxy.example:8080".into();
/// proxy.no_proxy = "localhost,internal.example".into();
/// let mut config = bifrost_net::NetConfig::new();
/// config.proxy = Some(proxy);
/// ```
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct ProxyConfig {
    /// Proxy for HTTP and ws destinations.
    pub http: String,
    /// Proxy for HTTPS and wss destinations.
    pub https: String,
    /// Bypass list, including host suffixes, addresses and CIDR ranges.
    pub no_proxy: String,
}

impl ProxyConfig {
    /// Construct direct routing with all proxy fields empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// A TLS or CONNECT refusal reported by a selected HTTP(S) proxy.
///
/// This is stored as the source of the `io::Error` returned by
/// [`crate::AccountNet::connect_tunnel`]. Callers can recover it with
/// `error.get_ref().and_then(|source| source.downcast_ref::<TunnelError>())`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TunnelError {
    /// TLS to the selected HTTPS proxy failed, either while building the
    /// connector from Net's TLS policy or during the handshake itself.
    #[error("TLS to proxy failed: {source}")]
    ProxyTls {
        /// Original TLS error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// The proxy requires or rejected authentication for CONNECT.
    #[error("proxy authentication required for CONNECT (HTTP 407)")]
    ProxyAuthenticationRequired,
    /// The proxy refused CONNECT with a status other than 2xx.
    #[error("proxy refused CONNECT with HTTP {status}")]
    ProxyRefused {
        /// CONNECT response status.
        status: u16,
    },
}

fn proxy_tls_error(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(TunnelError::ProxyTls {
        source: Box::new(error),
    })
}

pub(crate) struct Routing(Matcher);

impl Routing {
    pub(crate) fn new(config: Option<&ProxyConfig>) -> Self {
        Self(match config {
            Some(config) => Matcher::builder()
                .http(config.http.clone())
                .https(config.https.clone())
                .no(config.no_proxy.clone())
                .build(),
            None => Matcher::from_system(),
        })
    }

    pub(crate) fn reqwest_proxy(self: &Arc<Self>) -> reqwest::Proxy {
        let routing = Arc::clone(self);
        reqwest::Proxy::custom(move |url| routing.proxy_url(url))
    }

    fn proxy_url(&self, url: &reqwest::Url) -> Option<reqwest::Url> {
        let uri = url.as_str().parse::<http::Uri>().ok()?;
        let proxy = self.0.intercept(&uri)?;
        let mut url = reqwest::Url::parse(&proxy.uri().to_string()).ok()?;
        // Matcher separates credentials from the proxy URI. Put them back
        // in the custom proxy result so reqwest retains its auth behavior.
        // A credential that cannot be carried over must not turn into a
        // direct connection: returning None here would bypass the proxy.
        // Fall back to the proxy URL without credentials instead, so the
        // proxy itself refuses the request.
        if let Some(auth) = proxy.basic_auth()
            && let Some((user, password)) = decode_basic(auth)
        {
            // URL setters preserve percent escapes. Escape literal percent
            // bytes too, otherwise reqwest would decode credentials twice.
            let user = percent_encoding::percent_encode(&user, percent_encoding::NON_ALPHANUMERIC)
                .to_string();
            let password =
                percent_encoding::percent_encode(&password, percent_encoding::NON_ALPHANUMERIC)
                    .to_string();
            let mut with_auth = url.clone();
            if with_auth.set_username(&user).is_ok()
                && with_auth.set_password(Some(&password)).is_ok()
            {
                url = with_auth;
            }
        }
        Some(url)
    }
}

/// Split a `Basic` header value back into raw user and password bytes.
fn decode_basic(auth: &http::HeaderValue) -> Option<(Vec<u8>, Vec<u8>)> {
    let encoded = auth.as_bytes().strip_prefix(b"Basic ")?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let colon = decoded.iter().position(|&b| b == b':')?;
    Some((decoded[..colon].to_vec(), decoded[colon + 1..].to_vec()))
}

/// Async transport beneath an upgrade, possibly including TLS to a proxy.
pub trait TunnelIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> TunnelIo for T {}

/// Direct TCP or an established proxy tunnel. Origin TLS is applied by caller.
pub type TunnelStream = Pin<Box<dyn TunnelIo>>;

impl crate::AccountNet {
    /// Build upgrade TLS with the owning HTTP client's trust configuration.
    pub fn upgrade_tls_connector(&self) -> Result<native_tls::TlsConnector, native_tls::Error> {
        let config = &self.inner.net.inner.config;
        let mut builder = native_tls::TlsConnector::builder();
        builder.danger_accept_invalid_certs(config.dangerous_accept_invalid_certs);
        for cert in &config.root_certs {
            builder.add_root_certificate(cert.clone());
        }
        builder.build()
    }

    /// Open the raw connection used by a WebSocket upgrade or caller-managed
    /// HTTP(S) session, using this Net's routing snapshot. This does not send an
    /// origin request or perform origin TLS. A selected HTTP(S) proxy receives
    /// CONNECT, never origin auth. Unsupported proxy protocols, including
    /// SOCKS, fail closed rather than bypassing the proxy. TLS and CONNECT
    /// refusal errors retain a [`TunnelError`] source.
    pub async fn connect_tunnel(&self, destination: &str) -> io::Result<TunnelStream> {
        self.connect_tunnel_with(destination, |host, port| async move {
            let tcp = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
            Ok(Box::pin(tcp) as TunnelStream)
        })
        .await
    }

    async fn connect_tunnel_with<F, Fut>(
        &self,
        destination: &str,
        mut dial: F,
    ) -> io::Result<TunnelStream>
    where
        F: FnMut(String, u16) -> Fut,
        Fut: std::future::Future<Output = io::Result<TunnelStream>>,
    {
        let connect = async {
            let uri = upgrade_destination(destination)?;
            let scheme = uri.scheme_str().unwrap_or("http");
            let host = uri
                .host()
                .ok_or_else(|| io::Error::other("missing upgrade host"))?;
            let port = uri
                .port_u16()
                .unwrap_or(if scheme == "https" { 443 } else { 80 });
            let proxy = self.inner.net.inner.proxy.0.intercept(&uri);
            let Some(proxy) = proxy else {
                return dial(unbracket(host).to_owned(), port).await;
            };
            let proxy_uri = proxy.uri();
            let proxy_scheme = proxy_uri.scheme_str().unwrap_or("http");
            if !matches!(proxy_scheme, "http" | "https") {
                return Err(io::Error::other("upgrade proxy requires HTTP or HTTPS"));
            }
            let proxy_host = proxy_uri
                .host()
                .ok_or_else(|| io::Error::other("missing proxy host"))?;
            let proxy_port =
                proxy_uri
                    .port_u16()
                    .unwrap_or(if proxy_scheme == "https" { 443 } else { 80 });
            let tcp = dial(unbracket(proxy_host).to_owned(), proxy_port).await?;
            let mut stream: TunnelStream = if proxy_scheme == "https" {
                let tls = tokio_native_tls::TlsConnector::from(
                    self.upgrade_tls_connector().map_err(proxy_tls_error)?,
                );
                Box::pin(
                    tls.connect(unbracket(proxy_host), tcp)
                        .await
                        .map_err(proxy_tls_error)?,
                )
            } else {
                tcp
            };
            let authority = format!("{host}:{port}");
            establish_connect(&mut stream, &authority, proxy.basic_auth()).await?;
            Ok(stream)
        };
        match self.inner.net.inner.config.connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, connect).await.map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "upgrade connection timed out")
            })?,
            None => connect.await,
        }
    }
}

fn upgrade_destination(destination: &str) -> io::Result<http::Uri> {
    let mut url = reqwest::Url::parse(destination).map_err(io::Error::other)?;
    let scheme = match url.scheme() {
        "ws" | "http" => "http",
        "wss" | "https" => "https",
        _ => return Err(io::Error::other("unsupported upgrade scheme")),
    };
    url.set_scheme(scheme)
        .map_err(|()| io::Error::other("invalid upgrade scheme"))?;
    url.as_str().parse().map_err(io::Error::other)
}

fn unbracket(host: &str) -> &str {
    host.trim_start_matches('[').trim_end_matches(']')
}

async fn establish_connect<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    authority: &str,
    auth: Option<&http::HeaderValue>,
) -> io::Result<()> {
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(auth) = auth {
        request.push_str("Proxy-Authorization: ");
        request.push_str(auth.to_str().map_err(io::Error::other)?);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;
    // Read only through the header terminator: bytes from the tunnel must
    // remain available to origin TLS/the upgrade, even when coalesced.
    let mut response = Vec::new();
    loop {
        if response.len() == 16 * 1024 {
            return Err(io::Error::other("proxy CONNECT headers too large"));
        }
        response.push(stream.read_u8().await?);
        if response.ends_with(b"\r\n\r\n") {
            let line_end = response.windows(2).position(|w| w == b"\r\n").unwrap_or(0);
            let line = std::str::from_utf8(&response[..line_end]).map_err(io::Error::other)?;
            let mut fields = line.split_whitespace();
            let version = fields.next();
            let status = fields.next().and_then(|s| s.parse::<u16>().ok());
            if !matches!(version, Some("HTTP/1.0" | "HTTP/1.1")) {
                return Err(io::Error::other("invalid proxy CONNECT response"));
            }
            match status {
                Some(200..=299) => return Ok(()),
                Some(100..=199) if status != Some(101) => response.clear(),
                Some(407) => {
                    return Err(io::Error::other(TunnelError::ProxyAuthenticationRequired));
                }
                Some(status) => {
                    return Err(io::Error::other(TunnelError::ProxyRefused { status }));
                }
                None => return Err(io::Error::other("invalid proxy CONNECT status")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn routing() -> Arc<Routing> {
        Arc::new(Routing::new(Some(&ProxyConfig {
            http: "http://user:pass@proxy.test:8080".into(),
            https: "https://user:pass@secure-proxy.test:8443".into(),
            no_proxy: "internal.test,127.0.0.0/8,::1".into(),
        })))
    }

    #[test]
    fn routing_snapshot_preserves_auth_and_bypass() {
        let routing = routing();
        let http_url = routing
            .proxy_url(&reqwest::Url::parse("http://mail.test/").unwrap())
            .unwrap();
        assert_eq!(http_url.as_str(), "http://user:pass@proxy.test:8080/");
        let https_url = routing
            .proxy_url(&reqwest::Url::parse("https://mail.test/").unwrap())
            .unwrap();
        assert_eq!(
            https_url.as_str(),
            "https://user:pass@secure-proxy.test:8443/"
        );
        assert!(
            routing
                .proxy_url(&reqwest::Url::parse("https://internal.test/").unwrap())
                .is_none()
        );
        let proxy = routing
            .0
            .intercept(&"https://mail.test/".parse().unwrap())
            .unwrap();
        assert_eq!(proxy.uri().to_string(), "https://secure-proxy.test:8443/");
        let direct = Routing::new(Some(&ProxyConfig::default()));
        assert!(
            direct
                .0
                .intercept(&"https://mail.test/".parse().unwrap())
                .is_none()
        );
        assert_eq!(proxy.basic_auth().unwrap(), "Basic dXNlcjpwYXNz");
        for url in [
            "https://internal.test/",
            "http://127.0.0.2/",
            "https://[::1]/",
        ] {
            assert!(routing.0.intercept(&url.parse().unwrap()).is_none());
        }
    }

    #[test]
    fn custom_proxy_preserves_percent_encoded_credentials() {
        let routing = Routing::new(Some(&ProxyConfig {
            http: "http://u%2540:p%2525@proxy.test:8080".into(),
            ..ProxyConfig::default()
        }));
        let destination = reqwest::Url::parse("http://mail.test/").unwrap();
        let proxy_url = routing.proxy_url(&destination).unwrap();
        let roundtrip = Matcher::builder().all(proxy_url.as_str()).build();
        let uri = destination.as_str().parse().unwrap();
        assert_eq!(
            routing.0.intercept(&uri).unwrap().basic_auth(),
            roundtrip.intercept(&uri).unwrap().basic_auth(),
        );
    }

    #[test]
    fn proxy_config_constructor_starts_with_direct_routing() {
        let proxy = ProxyConfig::new();
        assert!(proxy.http.is_empty());
        assert!(proxy.https.is_empty());
        assert!(proxy.no_proxy.is_empty());
    }

    #[test]
    fn proxy_tls_failures_keep_a_typed_source() {
        let error = proxy_tls_error(io::Error::other("certificate rejected"));
        let tunnel_error = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<TunnelError>())
            .expect("proxy TLS failures expose a typed source");
        match tunnel_error {
            TunnelError::ProxyTls { source } => {
                assert_eq!(source.to_string(), "certificate rejected");
            }
            other => panic!("expected proxy TLS classification, got {other:?}"),
        }
    }

    #[test]
    fn injected_net_owns_upgrade_routing() {
        let config = crate::NetConfig {
            proxy: Some(ProxyConfig {
                http: "http://proxy.test:8080".into(),
                https: "https://secure-proxy.test:8443".into(),
                no_proxy: "internal.test".into(),
            }),
            ..crate::NetConfig::default()
        };
        let net = crate::Net::new(config).unwrap();
        let account = net.attach_account(
            crate::AccountId("upgrade-test".into()),
            crate::AccountSpec::new(None),
        );
        for (destination, expected) in [
            ("ws://mail.test/push", Some("http://proxy.test:8080/")),
            (
                "WSS://mail.test/push",
                Some("https://secure-proxy.test:8443/"),
            ),
            ("wss://internal.test/push", None),
        ] {
            let uri = upgrade_destination(destination).unwrap();
            let route = account.inner.net.inner.proxy.0.intercept(&uri);
            assert_eq!(route.map(|p| p.uri().to_string()).as_deref(), expected);
            let http_url = reqwest::Url::parse(&uri.to_string()).unwrap();
            assert_eq!(
                account
                    .inner
                    .net
                    .inner
                    .proxy
                    .proxy_url(&http_url)
                    .map(|u| u.to_string())
                    .as_deref(),
                expected
            );
        }
        assert!(upgrade_destination("ftp://mail.test/").is_err());
    }

    #[tokio::test]
    async fn account_connector_dials_proxy_or_direct_from_its_snapshot() {
        let config = crate::NetConfig {
            proxy: Some(ProxyConfig {
                https: "http://user:pass@proxy.test:8080".into(),
                no_proxy: "internal.test".into(),
                ..ProxyConfig::default()
            }),
            ..crate::NetConfig::default()
        };
        let net = crate::Net::new(config).unwrap();
        let account = net.attach_account(
            crate::AccountId("dial-test".into()),
            crate::AccountSpec::new(None),
        );
        for (destination, expected_host, expected_port, proxied) in [
            ("wss://mail.test/push", "proxy.test", 8080, true),
            ("wss://internal.test/push", "internal.test", 443, false),
        ] {
            let (client, mut server) = tokio::io::duplex(1024);
            let mut client = Some(client);
            let dial = |host: String, port| {
                assert_eq!(host, expected_host);
                assert_eq!(port, expected_port);
                let stream = client.take().expect("one dial");
                async move { Ok(Box::pin(stream) as TunnelStream) }
            };
            let peer = async {
                if proxied {
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        request.push(server.read_u8().await.unwrap());
                    }
                    assert_eq!(request, b"CONNECT mail.test:443 HTTP/1.1\r\nHost: mail.test:443\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n\r\n");
                    server.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await.unwrap();
                }
                server.write_all(b"origin").await.unwrap();
            };
            let caller = async {
                let mut stream = account
                    .connect_tunnel_with(destination, dial)
                    .await
                    .unwrap();
                let mut payload = [0; 6];
                stream.read_exact(&mut payload).await.unwrap();
                assert_eq!(&payload, b"origin");
            };
            tokio::join!(peer, caller);
        }
    }

    #[tokio::test]
    async fn connect_sends_only_proxy_auth_and_preserves_tunnel_bytes() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let proxy = routing()
            .0
            .intercept(&"https://mail.test/".parse().unwrap())
            .unwrap();
        let peer = async {
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(server.read_u8().await.unwrap());
            }
            assert_eq!(request, b"CONNECT [::1]:443 HTTP/1.1\r\nHost: [::1]:443\r\nProxy-Authorization: Basic dXNlcjpwYXNz\r\n\r\n");
            server.write_all(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 Connection established\r\n\r\ntunnel").await.unwrap();
        };
        let caller = async {
            establish_connect(&mut client, "[::1]:443", proxy.basic_auth())
                .await
                .unwrap();
            let mut bytes = [0; 6];
            client.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"tunnel");
        };
        tokio::join!(peer, caller);
    }

    #[tokio::test]
    async fn connect_classifies_authentication_and_other_proxy_refusals() {
        for (status, expected) in [
            (407, TunnelError::ProxyAuthenticationRequired),
            (403, TunnelError::ProxyRefused { status: 403 }),
        ] {
            let (mut client, mut server) = tokio::io::duplex(1024);
            let peer = async move {
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(server.read_u8().await.unwrap());
                }
                let response = format!("HTTP/1.1 {status} Refused\r\n\r\n");
                server.write_all(response.as_bytes()).await.unwrap();
            };
            let caller = async {
                let error = match establish_connect(&mut client, "mail.test:443", None).await {
                    Err(error) => error,
                    Ok(()) => panic!("proxy refusal unexpectedly established a tunnel"),
                };
                let tunnel_error = error
                    .get_ref()
                    .and_then(|source| source.downcast_ref::<TunnelError>())
                    .expect("CONNECT refusals expose a typed source");
                match (&expected, tunnel_error) {
                    (
                        TunnelError::ProxyAuthenticationRequired,
                        TunnelError::ProxyAuthenticationRequired,
                    ) => {}
                    (
                        TunnelError::ProxyRefused { status: expected },
                        TunnelError::ProxyRefused { status },
                    ) => assert_eq!(status, expected),
                    (expected, actual) => {
                        panic!("expected proxy refusal {expected:?}, got {actual:?}")
                    }
                }
            };
            tokio::join!(peer, caller);
        }
    }

    #[tokio::test]
    async fn selected_socks_proxy_fails_closed_before_dialing() {
        let config = crate::NetConfig {
            proxy: Some(ProxyConfig {
                https: "socks5://proxy.test:1080".into(),
                ..ProxyConfig::default()
            }),
            ..crate::NetConfig::default()
        };
        let net = crate::Net::new(config).unwrap();
        let account = net.attach_account(
            crate::AccountId("socks-upgrade-test".into()),
            crate::AccountSpec::new(None),
        );
        let dial =
            |_: String, _| async { Err::<TunnelStream, _>(io::Error::other("unexpected dial")) };
        let error = match account
            .connect_tunnel_with("wss://mail.test/push", dial)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("unsupported proxy unexpectedly opened a tunnel"),
        };
        assert_eq!(error.to_string(), "upgrade proxy requires HTTP or HTTPS");
    }

    #[tokio::test]
    async fn connect_refuses_failure_eof_and_oversized_headers() {
        for response in [
            b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n".to_vec(),
            b"HTTP/1.1 200".to_vec(),
            vec![b'x'; 16 * 1024],
        ] {
            let (mut client, mut server) = tokio::io::duplex(32 * 1024);
            let peer = async move {
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(server.read_u8().await.unwrap());
                }
                server.write_all(&response).await.unwrap();
                drop(server);
            };
            let caller = async {
                assert!(
                    establish_connect(&mut client, "mail.test:443", None)
                        .await
                        .is_err()
                );
            };
            // Drop the peer after its write so a truncated reply produces EOF.
            tokio::join!(peer, caller);
        }
    }
}
