//! Optional ManageSieve client and the Stage 2 script filter surface.
//!
//! Audit boundary: not line-audited by the 2026-09-04 bug hunt, which read
//! the driver, framing, pool, push, auth, change strategies, mutations,
//! inventory, hydration and cursor envelope instead. An absence of findings
//! in this module is an absence of reading. Noted so a later auditor knows
//! where coverage stops.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine;
use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, FilterDiagnostic, FilterDiagnosticSeverity,
    FilterScript, FilterValidation, ScriptLanguage, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use crate::error::{
    AuthMechanismRejection, AuthMechanismRejectionReason, AuthPolicyFailure, Error,
};
use crate::types::{AuthMechanism, AuthPolicy, Credentials, CredentialsKind};

use super::error::ImapErrorContext;
use super::{ImapAccount, account_error_with};

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ManageSieveTlsMode {
    Implicit,
    StartTls,
    None,
}

impl ManageSieveTlsMode {
    fn uses_implicit_tls(self) -> bool {
        matches!(self, Self::Implicit)
    }

    fn uses_starttls(self) -> bool {
        matches!(self, Self::StartTls)
    }
}

#[non_exhaustive]
#[derive(Clone)]
pub struct ManageSieveConfig {
    host: String,
    port: u16,
    tls_mode: ManageSieveTlsMode,
    connect_timeout: Duration,
    command_timeout: Duration,
    tls_connector: Option<native_tls::TlsConnector>,
}

impl ManageSieveConfig {
    pub fn starttls(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 4190,
            tls_mode: ManageSieveTlsMode::StartTls,
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_secs(60),
            tls_connector: None,
        }
    }

    pub fn tls(host: impl Into<String>) -> Self {
        Self {
            tls_mode: ManageSieveTlsMode::Implicit,
            ..Self::starttls(host)
        }
    }

    pub fn plaintext(host: impl Into<String>) -> Self {
        Self {
            tls_mode: ManageSieveTlsMode::None,
            ..Self::starttls(host)
        }
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn with_command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    pub fn with_tls_connector(mut self, connector: native_tls::TlsConnector) -> Self {
        self.tls_connector = Some(connector);
        self
    }
}

impl std::fmt::Debug for ManageSieveConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManageSieveConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls_mode", &self.tls_mode)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field(
                "tls_connector",
                &self.tls_connector.as_ref().map(|_| "<custom>"),
            )
            .finish()
    }
}

pub(crate) fn filters_list(
    account: ImapAccount,
) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
    Box::pin(async move {
        let mut client = open_client(&account, AccountOperation::FiltersList).await?;
        let scripts = client
            .list_scripts()
            .await
            .map_err(to_acct_err(AccountOperation::FiltersList))?;
        let mut filters = Vec::with_capacity(scripts.len());
        for script in scripts {
            let body = client
                .get_script(&script.name)
                .await
                .map_err(to_acct_err(AccountOperation::FiltersList))?;
            filters.push(ServerFilter::Script(FilterScript {
                id: ServerFilterId(script.name.clone()),
                name: Some(script.name),
                language: ScriptLanguage::Sieve,
                body,
                is_active: script.is_active,
            }));
        }
        Ok(filters)
    })
}

pub(crate) fn filter_create(
    account: ImapAccount,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<ServerFilterId, AccountError>> {
    Box::pin(async move {
        let ServerFilterCreate::Script(script) = filter else {
            return Err(super::error::unsupported(AccountOperation::FilterCreate));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Err(invalid(
                AccountOperation::FilterCreate,
                "IMAP ManageSieve only supports Sieve scripts",
            ));
        }
        let name = script.name.ok_or_else(|| {
            invalid(
                AccountOperation::FilterCreate,
                "IMAP ManageSieve script creation requires a name",
            )
        })?;
        let mut client = open_client(&account, AccountOperation::FilterCreate).await?;
        client
            .put_script(&name, &script.body)
            .await
            .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        if script.is_active {
            client
                .set_active(Some(&name))
                .await
                .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        }
        Ok(ServerFilterId(name))
    })
}

pub(crate) fn filter_update(
    account: ImapAccount,
    filter: ServerFilterId,
    patch: ServerFilterPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let ServerFilterPatch::Script(patch) = patch else {
            return Err(super::error::unsupported(AccountOperation::FilterUpdate));
        };
        if patch.name.is_some() {
            return Err(invalid(
                AccountOperation::FilterUpdate,
                "IMAP ManageSieve script names are filter ids and cannot be patched",
            ));
        }
        let mut client = open_client(&account, AccountOperation::FilterUpdate).await?;
        if let Some(body) = patch.body {
            client
                .put_script(&filter.0, &body)
                .await
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        }
        if let Some(is_active) = patch.is_active {
            let active = is_active.then_some(filter.0.as_str());
            client
                .set_active(active)
                .await
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        }
        Ok(())
    })
}

pub(crate) fn filter_delete(
    account: ImapAccount,
    filter: ServerFilterId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let mut client = open_client(&account, AccountOperation::FilterDelete).await?;
        client
            .delete_script(&filter.0)
            .await
            .map_err(to_acct_err(AccountOperation::FilterDelete))
    })
}

pub(crate) fn filter_validate(
    account: ImapAccount,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<FilterValidation, AccountError>> {
    Box::pin(async move {
        let ServerFilterCreate::Script(script) = filter else {
            return Ok(validation_error(
                "IMAP ManageSieve only validates literal script filters",
            ));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Ok(validation_error(
                "IMAP ManageSieve only validates Sieve scripts",
            ));
        }
        let mut client = open_client(&account, AccountOperation::FilterValidate).await?;
        client
            .check_script(&script.body)
            .await
            .map_err(to_acct_err(AccountOperation::FilterValidate))
    })
}

async fn open_client(
    account: &ImapAccount,
    op: AccountOperation,
) -> Result<ManageSieveClient, AccountError> {
    let config = account
        .config
        .sieve
        .clone()
        .ok_or_else(|| super::error::unsupported(op))?;
    ManageSieveClient::connect(
        &config,
        &account.config.credentials,
        &account.config.auth_policy,
        account.bandwidth_budget.clone(),
    )
    .await
    .map_err(to_acct_err(op))
}

fn to_acct_err(op: AccountOperation) -> impl Fn(Error) -> AccountError {
    move |err| account_error_with(err, ImapErrorContext::operation(op))
}

fn invalid(op: AccountOperation, detail: impl Into<String>) -> AccountError {
    account_error_with(
        Error::InvalidInput(detail.into()),
        ImapErrorContext::operation(op),
    )
}

fn validation_error(message: impl Into<String>) -> FilterValidation {
    FilterValidation {
        diagnostics: vec![FilterDiagnostic {
            severity: FilterDiagnosticSeverity::Error,
            message: message.into(),
            line: None,
            column: None,
        }],
    }
}

struct ManageSieveClient {
    stream: SieveStream,
    timeout: Duration,
}

struct ListedScript {
    name: String,
    is_active: bool,
}

impl ManageSieveClient {
    async fn connect(
        config: &ManageSieveConfig,
        credentials: &Credentials,
        policy: &AuthPolicy,
        budget: bifrost_net::BandwidthBudget,
    ) -> Result<Self, Error> {
        validate_tls_server_name(&config.host)?;
        let tcp = tokio::time::timeout(
            config.connect_timeout,
            TcpStream::connect((config.host.as_str(), config.port)),
        )
        .await
        .map_err(|_| Error::Timeout { attempt: None })??;
        let transport = if config.tls_mode.uses_implicit_tls() {
            SieveTransport::Tls(Box::new(tls_connect(config, tcp).await?))
        } else {
            SieveTransport::Plain(tcp)
        };
        let mut stream = SieveStream { transport, budget };
        let mut capabilities = read_response(&mut stream, config.command_timeout)
            .await?
            .ensure_ok()?
            .capabilities();
        let mut tls_active = config.tls_mode.uses_implicit_tls();

        if config.tls_mode.uses_starttls() {
            if !capabilities.starttls {
                return Err(Error::StartTlsUnavailable);
            }
            write_command(&mut stream, config.command_timeout, b"STARTTLS\r\n").await?;
            read_response(&mut stream, config.command_timeout)
                .await?
                .ensure_ok()?;
            stream.transport = match stream.transport {
                SieveTransport::Plain(tcp) => {
                    SieveTransport::Tls(Box::new(tls_connect(config, tcp).await?))
                }
                #[cfg(test)]
                SieveTransport::Memory(_) => unreachable!("memory streams cannot negotiate TLS"),
                SieveTransport::Tls(_) => {
                    return Err(Error::Internal("STARTTLS on TLS stream".into()));
                }
            };
            tls_active = true;
            capabilities = request_capability(&mut stream, config.command_timeout).await?;
        }

        authenticate(
            &mut stream,
            config.command_timeout,
            credentials,
            policy,
            &capabilities,
            tls_active,
        )
        .await?;

        Ok(Self {
            stream,
            timeout: config.command_timeout,
        })
    }

    async fn list_scripts(&mut self) -> Result<Vec<ListedScript>, Error> {
        write_command(&mut self.stream, self.timeout, b"LISTSCRIPTS\r\n").await?;
        let response = read_response(&mut self.stream, self.timeout)
            .await?
            .ensure_ok()?;
        response
            .items
            .iter()
            .filter_map(|item| match item {
                SieveResponseItem::Line(line) => parse_list_script(line),
                SieveResponseItem::Literal(_) => None,
            })
            .collect()
    }

    async fn get_script(&mut self, name: &str) -> Result<String, Error> {
        let command = format!("GETSCRIPT {}\r\n", script_name_arg(name)?);
        write_command(&mut self.stream, self.timeout, command.as_bytes()).await?;
        let response = read_response(&mut self.stream, self.timeout)
            .await?
            .ensure_ok()?;
        for item in response.items {
            if let SieveResponseItem::Literal(bytes) = item {
                return String::from_utf8(bytes)
                    .map_err(|error| Error::Parse(format!("Sieve script is not UTF-8: {error}")));
            }
        }
        Err(Error::Parse(
            "GETSCRIPT response did not include a script literal".into(),
        ))
    }

    async fn put_script(&mut self, name: &str, body: &str) -> Result<(), Error> {
        let command = literal_command("PUTSCRIPT", Some(name), body)?;
        write_command(&mut self.stream, self.timeout, &command).await?;
        read_response(&mut self.stream, self.timeout)
            .await?
            .ensure_ok()?;
        Ok(())
    }

    async fn set_active(&mut self, name: Option<&str>) -> Result<(), Error> {
        let command = match name {
            Some(name) => format!("SETACTIVE {}\r\n", script_name_arg(name)?),
            None => format!("SETACTIVE {}\r\n", quote_string("")?),
        };
        write_command(&mut self.stream, self.timeout, command.as_bytes()).await?;
        read_response(&mut self.stream, self.timeout)
            .await?
            .ensure_ok()?;
        Ok(())
    }

    async fn delete_script(&mut self, name: &str) -> Result<(), Error> {
        let command = format!("DELETESCRIPT {}\r\n", script_name_arg(name)?);
        write_command(&mut self.stream, self.timeout, command.as_bytes()).await?;
        read_response(&mut self.stream, self.timeout)
            .await?
            .ensure_ok()?;
        Ok(())
    }

    async fn check_script(&mut self, body: &str) -> Result<FilterValidation, Error> {
        let command = literal_command("CHECKSCRIPT", None, body)?;
        write_command(&mut self.stream, self.timeout, &command).await?;
        let response = read_response(&mut self.stream, self.timeout).await?;
        validation_outcome(response.status)
    }
}

/// Decide whether a CHECKSCRIPT reply is a validation verdict or a
/// failure to reach one.
///
/// A `NO` normally IS the verdict - the server compiled the script and
/// rejected it - so it becomes diagnostics rather than an error. But a
/// transient code means the server never judged the script at all.
/// Reporting that as "your script is invalid" tells the user something
/// false about their input AND discards the retryable classification, so
/// it stays an error.
fn validation_outcome(status: SieveStatus) -> Result<FilterValidation, Error> {
    match status {
        SieveStatus::Ok => Ok(FilterValidation::default()),
        SieveStatus::No { code, message } | SieveStatus::Bye { code, message } => {
            if code.as_ref().is_some_and(SieveResponseCode::is_transient) {
                return Err(Error::Sieve { code, message });
            }
            Ok(validation_error(message))
        }
    }
}

async fn request_capability(
    stream: &mut SieveStream,
    timeout: Duration,
) -> Result<SieveCapabilities, Error> {
    write_command(stream, timeout, b"CAPABILITY\r\n").await?;
    Ok(read_response(stream, timeout)
        .await?
        .ensure_ok()?
        .capabilities())
}

async fn authenticate(
    stream: &mut SieveStream,
    timeout: Duration,
    credentials: &Credentials,
    policy: &AuthPolicy,
    capabilities: &SieveCapabilities,
    tls_active: bool,
) -> Result<(), Error> {
    let offered = capabilities.sasl.clone();
    let (mechanism, payload) = match credentials.kind() {
        CredentialsKind::Password { username, password } => {
            if !supports_mechanism(&offered, "PLAIN") {
                return Err(Error::AuthPolicy(AuthPolicyFailure::new(
                    offered,
                    Vec::new(),
                )));
            }
            let mut payload = Vec::new();
            payload.push(0);
            payload.extend_from_slice(username.as_bytes());
            payload.push(0);
            payload.extend_from_slice(password.as_bytes());
            (AuthMechanism::Plain, payload)
        }
        CredentialsKind::OAuth2 {
            identity,
            token_source,
        } => {
            if !supports_mechanism(&offered, "XOAUTH2") {
                return Err(Error::AuthPolicy(AuthPolicyFailure::new(
                    offered,
                    Vec::new(),
                )));
            }
            // Read the current token from the shared source at auth time,
            // mirroring the IMAP connect path, so a rotated token is
            // presented on every ManageSieve (re)authentication.
            let access_token = token_source.current().await.map_err(|e| {
                Error::auth_with_code(format!("failed to read OAuth access token: {e}"), None)
            })?;
            (
                AuthMechanism::XOAuth2,
                format!(
                    "user={identity}\x01auth=Bearer {}\x01\x01",
                    access_token.as_str()
                )
                .into_bytes(),
            )
        }
    };
    if !tls_active && !policy.allow_cleartext_without_tls {
        return Err(Error::AuthPolicy(AuthPolicyFailure::new(
            offered,
            vec![AuthMechanismRejection::new(
                mechanism,
                AuthMechanismRejectionReason::CleartextWithoutTls,
            )],
        )));
    }
    let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
    let command = format!(
        "AUTHENTICATE {} {}\r\n",
        quote_string(mechanism.name())?,
        quote_string(&encoded)?
    );
    write_command(stream, timeout, command.as_bytes()).await?;
    match read_response(stream, timeout).await?.status {
        SieveStatus::Ok => Ok(()),
        // The four RFC 5804 codes that say WHY authentication was refused
        // classify better than a bare auth failure: a too-weak mechanism
        // or a missing TLS layer is a policy block the user cannot fix by
        // re-entering a password. Anything else (including no code at all)
        // stays a plain auth failure rather than falling through to the
        // sieve table's server-refused default.
        SieveStatus::No { code, message } => Err(match code {
            Some(
                code @ (SieveResponseCode::AuthTooWeak
                | SieveResponseCode::EncryptNeeded
                | SieveResponseCode::TransitionNeeded
                | SieveResponseCode::Sasl),
            ) => Error::Sieve {
                code: Some(code),
                message,
            },
            // The mechanism is known here, so the refusal can say which rung
            // the server turned down rather than only that one was turned
            // down.
            _ => Error::auth_with_mechanism(message, None, mechanism.name()),
        }),
        SieveStatus::Bye { code, message } => Err(Error::Sieve { code, message }),
    }
}

fn supports_mechanism(offered: &[String], mechanism: &str) -> bool {
    offered
        .iter()
        .any(|item| item.eq_ignore_ascii_case(mechanism))
}

async fn tls_connect(
    config: &ManageSieveConfig,
    tcp: TcpStream,
) -> Result<tokio_native_tls::TlsStream<TcpStream>, Error> {
    let connector = match &config.tls_connector {
        Some(connector) => connector.clone(),
        None => native_tls::TlsConnector::new().map_err(io_error)?,
    };
    let connector = tokio_native_tls::TlsConnector::from(connector);
    tokio::time::timeout(config.connect_timeout, connector.connect(&config.host, tcp))
        .await
        .map_err(|_| Error::Timeout { attempt: None })?
        .map_err(io_error)
}

fn io_error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Io {
        source: Arc::new(std::io::Error::other(error)),
        attempt: None,
    }
}

fn validate_tls_server_name(host: &str) -> Result<(), Error> {
    if host.is_empty() {
        return Err(Error::InvalidInput(
            "TLS server name must not be empty".into(),
        ));
    }
    if host.bytes().any(|b| b == 0 || b.is_ascii_whitespace()) {
        return Err(Error::InvalidInput(format!(
            "invalid TLS server name: {host:?}"
        )));
    }
    Ok(())
}

struct SieveStream {
    transport: SieveTransport,
    budget: bifrost_net::BandwidthBudget,
}

enum SieveTransport {
    Plain(TcpStream),
    #[cfg(test)]
    Memory(tokio::io::DuplexStream),
    Tls(Box<tokio_native_tls::TlsStream<TcpStream>>),
}

impl AsyncRead for SieveTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(test)]
            Self::Memory(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for SieveTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(test)]
            Self::Memory(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(test)]
            Self::Memory(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(test)]
            Self::Memory(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

async fn write_command(
    stream: &mut SieveStream,
    timeout: Duration,
    bytes: &[u8],
) -> Result<(), Error> {
    let mut stream = bifrost_net::MeteredStream::new(&mut stream.transport, stream.budget.clone());
    tokio::time::timeout(timeout, async {
        stream.write_all(bytes).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| Error::Timeout { attempt: None })?
    .map_err(Error::from)
}

async fn read_response(
    stream: &mut SieveStream,
    timeout: Duration,
) -> Result<SieveResponse, Error> {
    let mut items = Vec::new();
    loop {
        let line = read_line(stream, timeout).await?;
        if let Some(status) = SieveStatus::parse(&line) {
            return Ok(SieveResponse { items, status });
        }
        if let Some(len) = literal_len(&line) {
            let bytes = read_exact_len(stream, timeout, len).await?;
            items.push(SieveResponseItem::Literal(bytes));
        } else {
            items.push(SieveResponseItem::Line(line));
        }
    }
}

// This adapter sits below admission, so its timer exists only while peer I/O
// is pending. A cap wait cannot consume the peer's response timeout.
struct PeerReadTimeout<S> {
    inner: S,
    remaining: Duration,
    pending_since: Option<tokio::time::Instant>,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for PeerReadTimeout<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        use std::future::Future;
        if let Some(started) = self.pending_since.take() {
            self.remaining = self.remaining.saturating_sub(started.elapsed());
        }
        if self.remaining.is_zero() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "ManageSieve peer read timed out",
            )));
        }
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(result) => {
                self.timer = None;
                Poll::Ready(result)
            }
            Poll::Pending => {
                let timeout = self.remaining;
                self.pending_since = Some(tokio::time::Instant::now());
                let timer = self
                    .timer
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
                if timer.as_mut().poll(cx).is_ready() {
                    Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "ManageSieve peer read timed out",
                    )))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

fn metered_reader(
    stream: &mut SieveStream,
    timeout: Duration,
) -> bifrost_net::MeteredStream<PeerReadTimeout<&mut SieveTransport>> {
    bifrost_net::MeteredStream::new(
        PeerReadTimeout {
            inner: &mut stream.transport,
            remaining: timeout,
            pending_since: None,
            timer: None,
        },
        stream.budget.clone(),
    )
}

fn peer_read_error(error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::TimedOut {
        Error::Timeout { attempt: None }
    } else {
        Error::from(error)
    }
}

async fn read_line(stream: &mut SieveStream, timeout: Duration) -> Result<String, Error> {
    let mut stream = metered_reader(stream, timeout);
    let mut buf = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        stream
            .read_exact(&mut byte)
            .await
            .map_err(peer_read_error)?;
        buf.push(byte[0]);
        if byte[0] == b'\n' {
            break;
        }
        if buf.len() > 64 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "ManageSieve response line exceeded 64KiB",
            )
            .into());
        }
    }
    buf.pop();
    if buf.ends_with(b"\r") {
        buf.pop();
    }
    String::from_utf8(buf)
        .map_err(|error| Error::Parse(format!("ManageSieve response is not UTF-8: {error}")))
}

async fn read_exact_len(
    stream: &mut SieveStream,
    timeout: Duration,
    len: usize,
) -> Result<Vec<u8>, Error> {
    let mut stream = metered_reader(stream, timeout);
    let mut buf = vec![0_u8; len];
    stream.read_exact(&mut buf).await.map_err(peer_read_error)?;
    Ok(buf)
}

struct SieveResponse {
    items: Vec<SieveResponseItem>,
    status: SieveStatus,
}

impl SieveResponse {
    fn ensure_ok(self) -> Result<Self, Error> {
        match self.status {
            SieveStatus::Ok => Ok(self),
            // Both carry the ManageSieve code so the account boundary can
            // classify it; a `BYE` additionally means the server is
            // closing the connection, but the rejection reason is what
            // decides recovery.
            SieveStatus::No { code, message } | SieveStatus::Bye { code, message } => {
                Err(Error::Sieve { code, message })
            }
        }
    }

    fn capabilities(&self) -> SieveCapabilities {
        let mut capabilities = SieveCapabilities::default();
        for item in &self.items {
            let SieveResponseItem::Line(line) = item else {
                continue;
            };
            if let Some((key, rest)) = parse_quoted(line, 0) {
                if key.eq_ignore_ascii_case("STARTTLS") {
                    capabilities.starttls = true;
                } else if key.eq_ignore_ascii_case("SASL")
                    && let Some((mechanisms, _)) = parse_quoted(line, rest)
                {
                    capabilities.sasl = mechanisms.split_whitespace().map(str::to_string).collect();
                }
            }
        }
        capabilities
    }
}

enum SieveResponseItem {
    Line(String),
    Literal(Vec<u8>),
}

#[derive(Debug)]
enum SieveStatus {
    Ok,
    No {
        code: Option<SieveResponseCode>,
        message: String,
    },
    Bye {
        code: Option<SieveResponseCode>,
        message: String,
    },
}

impl SieveStatus {
    fn parse(line: &str) -> Option<Self> {
        let (token, rest) = split_token(line);
        // RFC 5804 1.3:
        //   response-oknobye = ("OK"/"NO"/"BYE") [SP "(" resp-code ")"]
        //                      [SP string] CRLF
        // The parenthesized code comes BEFORE the human-readable string,
        // so it has to be lifted off first or it lands inside the message
        // as opaque text and its semantics are lost.
        let (code, rest) = split_response_code(rest);
        if token.eq_ignore_ascii_case("OK") {
            Some(Self::Ok)
        } else if token.eq_ignore_ascii_case("NO") {
            Some(Self::No {
                code,
                message: status_message(rest),
            })
        } else if token.eq_ignore_ascii_case("BYE") {
            Some(Self::Bye {
                code,
                message: status_message(rest),
            })
        } else {
            None
        }
    }
}

/// A ManageSieve response code (RFC 5804 1.3).
///
/// Its own vocabulary, deliberately not folded into the IMAP
/// `ResponseCode`: these arrive on a different protocol and recording an
/// IMAP code the server never sent would corrupt the wire evidence a
/// support export exists to preserve.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SieveResponseCode {
    AuthTooWeak,
    EncryptNeeded,
    /// `QUOTA`, `QUOTA/MAXSCRIPTS`, `QUOTA/MAXSIZE`. The sub-kind rides
    /// in the diagnostic text; all three are the same classification.
    Quota,
    Referral,
    Sasl,
    TransitionNeeded,
    /// The one that matters most: an explicitly TRANSIENT failure. Left
    /// unparsed it derived terminal `ProviderRefused`, so the engine
    /// never retried a server that asked to be retried.
    TryLater,
    /// The named script is the active one (e.g. DELETESCRIPT on it).
    Active,
    NonExistent,
    AlreadyExists,
    /// Script compiled with warnings. Advisory - it rides on `OK` too.
    Warnings,
    Tag,
    /// An extension code this client does not model. Preserved verbatim
    /// for diagnostics; classification falls back to the status.
    Other(String),
}

impl SieveResponseCode {
    fn parse(raw: &str) -> Self {
        // `QUOTA/MAXSCRIPTS` and `SASL "..."` / `REFERRAL "..."` carry a
        // payload after the code atom; key off the atom alone.
        let atom = raw
            .split(|c: char| c == '/' || c.is_whitespace())
            .next()
            .unwrap_or(raw);
        match atom.to_ascii_uppercase().as_str() {
            "AUTH-TOO-WEAK" => Self::AuthTooWeak,
            "ENCRYPT-NEEDED" => Self::EncryptNeeded,
            "QUOTA" => Self::Quota,
            "REFERRAL" => Self::Referral,
            "SASL" => Self::Sasl,
            "TRANSITION-NEEDED" => Self::TransitionNeeded,
            "TRYLATER" => Self::TryLater,
            "ACTIVE" => Self::Active,
            "NONEXISTENT" => Self::NonExistent,
            "ALREADYEXISTS" => Self::AlreadyExists,
            "WARNINGS" => Self::Warnings,
            "TAG" => Self::Tag,
            _ => Self::Other(raw.trim().to_owned()),
        }
    }

    /// The on-wire spelling, for `native_code` telemetry.
    pub(crate) fn code(&self) -> &str {
        match self {
            Self::AuthTooWeak => "AUTH-TOO-WEAK",
            Self::EncryptNeeded => "ENCRYPT-NEEDED",
            Self::Quota => "QUOTA",
            Self::Referral => "REFERRAL",
            Self::Sasl => "SASL",
            Self::TransitionNeeded => "TRANSITION-NEEDED",
            Self::TryLater => "TRYLATER",
            Self::Active => "ACTIVE",
            Self::NonExistent => "NONEXISTENT",
            Self::AlreadyExists => "ALREADYEXISTS",
            Self::Warnings => "WARNINGS",
            Self::Tag => "TAG",
            Self::Other(raw) => raw,
        }
    }

    /// Whether a `NO` carrying this code is a transient condition the
    /// caller should retry rather than a verdict about the request.
    fn is_transient(&self) -> bool {
        matches!(self, Self::TryLater)
    }
}

/// Split a leading `"(" resp-code ")"` off a status line remainder.
/// Returns the remainder unchanged when no code is present.
fn split_response_code(rest: &str) -> (Option<SieveResponseCode>, &str) {
    let Some(after_paren) = rest.strip_prefix('(') else {
        return (None, rest);
    };
    let Some(close) = after_paren.find(')') else {
        // Unbalanced - treat the whole thing as message text rather than
        // inventing a code.
        return (None, rest);
    };
    let (raw, tail) = after_paren.split_at(close);
    (Some(SieveResponseCode::parse(raw)), tail[1..].trim_start())
}

#[derive(Default)]
struct SieveCapabilities {
    starttls: bool,
    sasl: Vec<String>,
}

fn split_token(line: &str) -> (&str, &str) {
    match line.split_once(char::is_whitespace) {
        Some((token, rest)) => (token, rest.trim_start()),
        None => (line, ""),
    }
}

fn status_message(rest: &str) -> String {
    parse_quoted(rest, 0)
        .map(|(message, _)| message)
        .unwrap_or_else(|| rest.trim().to_string())
}

fn literal_len(line: &str) -> Option<usize> {
    let trimmed = line.trim();
    let inner = trimmed.strip_prefix('{')?.strip_suffix('}')?;
    inner.parse().ok()
}

fn parse_list_script(line: &str) -> Option<Result<ListedScript, Error>> {
    let (name, offset) = match parse_quoted(line, 0) {
        Some(parsed) => parsed,
        None => {
            return Some(Err(Error::Parse(format!(
                "invalid LISTSCRIPTS line: {line}"
            ))));
        }
    };
    let is_active = line[offset..]
        .split_whitespace()
        .any(|token| token.eq_ignore_ascii_case("ACTIVE"));
    Some(Ok(ListedScript { name, is_active }))
}

fn parse_quoted(input: &str, start: usize) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    let mut index = start;
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    if bytes.get(index) != Some(&b'"') {
        return None;
    }
    index += 1;
    let mut out = String::new();
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => {
                index += 1;
                let escaped = *bytes.get(index)?;
                out.push(char::from(escaped));
                index += 1;
            }
            b'"' => return Some((out, index + 1)),
            byte => {
                out.push(char::from(byte));
                index += 1;
            }
        }
    }
    None
}

fn quote_string(value: &str) -> Result<String, Error> {
    if value.bytes().any(|byte| matches!(byte, 0 | b'\r' | b'\n')) {
        return Err(Error::InvalidInput(
            "ManageSieve strings cannot contain NUL or CRLF".into(),
        ));
    }
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    Ok(format!("\"{escaped}\""))
}

fn script_name_arg(name: &str) -> Result<String, Error> {
    if name.is_empty() {
        return Err(Error::InvalidInput(
            "ManageSieve script names cannot be empty".into(),
        ));
    }
    quote_string(name)
}

fn literal_command(command: &str, name: Option<&str>, body: &str) -> Result<Vec<u8>, Error> {
    if command
        .bytes()
        .any(|byte| !byte.is_ascii_uppercase() && byte != b'_')
    {
        return Err(Error::InvalidInput(
            "ManageSieve command token must be uppercase ASCII".into(),
        ));
    }
    let mut bytes = match name {
        Some(name) => format!(
            "{command} {} {{{}+}}\r\n",
            script_name_arg(name)?,
            body.len()
        )
        .into_bytes(),
        None => format!("{command} {{{}+}}\r\n", body.len()).into_bytes(),
    };
    bytes.extend_from_slice(body.as_bytes());
    bytes.extend_from_slice(b"\r\n");
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    #[tokio::test(start_paused = true)]
    async fn bandwidth_sieve_charges_the_account_and_excludes_admission_from_peer_timeout() {
        use bifrost_net::{BandwidthBudget, BandwidthDirection};
        use tokio::time::Instant;
        let budget = BandwidthBudget::new(Some(4));
        budget.admit(BandwidthDirection::Inbound, 4).await.finish(4);
        let (client, mut peer) = tokio::io::duplex(64);
        let mut stream = SieveStream {
            transport: SieveTransport::Memory(client),
            budget: budget.clone(),
        };
        peer.write_all(b"OK\r\n").await.unwrap();
        let start = Instant::now();
        assert_eq!(
            read_line(&mut stream, Duration::from_millis(10))
                .await
                .unwrap(),
            "OK"
        );
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        let next = budget.reserve(BandwidthDirection::Inbound, 4);
        assert_eq!(
            next.ready_at().duration_since(Instant::now()),
            Duration::from_secs(1)
        );
        write_command(&mut stream, Duration::from_secs(5), b"NOOP\r\n")
            .await
            .unwrap();
        assert_eq!(start.elapsed(), Duration::from_millis(1500));
        let next = budget.reserve(BandwidthDirection::Outbound, 4);
        assert_eq!(
            next.ready_at().duration_since(Instant::now()),
            Duration::from_secs(1)
        );
    }

    async fn sieve_trickle_case(literal: bool) {
        use tokio::time::Instant;
        let (client, mut peer) = tokio::io::duplex(64);
        let mut stream = SieveStream {
            transport: SieveTransport::Memory(client),
            budget: bifrost_net::BandwidthBudget::default(),
        };
        let writer = tokio::spawn(async move {
            for byte in b"OK\r\n" {
                tokio::time::sleep(Duration::from_millis(40)).await;
                if peer.write_all(&[*byte]).await.is_err() {
                    return;
                }
            }
        });
        let start = Instant::now();
        let result = if literal {
            read_exact_len(&mut stream, Duration::from_millis(100), 4)
                .await
                .map(|_| ())
        } else {
            read_line(&mut stream, Duration::from_millis(100))
                .await
                .map(|_| ())
        };
        assert!(matches!(result, Err(Error::Timeout { .. })));
        assert_eq!(start.elapsed(), Duration::from_millis(100));
        writer.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_sieve_trickled_line_spends_one_peer_budget() {
        sieve_trickle_case(false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn bandwidth_sieve_trickled_literal_spends_one_peer_budget() {
        sieve_trickle_case(true).await;
    }

    use super::*;

    fn no_status(line: &str) -> (Option<SieveResponseCode>, String) {
        match SieveStatus::parse(line).expect("status parses") {
            SieveStatus::No { code, message } => (code, message),
            other => panic!("expected NO, got {other:?}"),
        }
    }

    /// RFC 5804 puts the parenthesized code BEFORE the human string, so a
    /// parser that reads the message first swallows the code as opaque
    /// text - which is how `[TRYLATER]` came to derive terminal.
    #[test]
    fn a_response_code_is_lifted_off_ahead_of_the_message() {
        let (code, message) = no_status(r#"NO (TRYLATER) "Server busy, try again""#);
        assert_eq!(code, Some(SieveResponseCode::TryLater));
        assert_eq!(message, "Server busy, try again");
    }

    /// `QUOTA/MAXSCRIPTS` and `QUOTA/MAXSIZE` are the same classification;
    /// the sub-kind is diagnostic only.
    #[test]
    fn a_quota_subkind_parses_as_quota() {
        let (code, _) = no_status(r#"NO (QUOTA/MAXSCRIPTS) "Too many scripts""#);
        assert_eq!(code, Some(SieveResponseCode::Quota));
        let (code, _) = no_status(r#"NO (QUOTA/MAXSIZE) "Script too large""#);
        assert_eq!(code, Some(SieveResponseCode::Quota));
    }

    /// An unmodelled extension code must be preserved verbatim rather
    /// than dropped, and must not be mistaken for one we do model.
    #[test]
    fn an_unknown_response_code_is_preserved_verbatim() {
        let (code, message) = no_status(r#"NO (FROBNICATE) "nope""#);
        assert_eq!(code, Some(SieveResponseCode::Other("FROBNICATE".into())));
        assert_eq!(message, "nope");
    }

    #[test]
    fn a_status_line_without_a_code_still_parses_its_message() {
        let (code, message) = no_status(r#"NO "plain refusal""#);
        assert_eq!(code, None);
        assert_eq!(message, "plain refusal");
    }

    /// An unbalanced parenthesis must not be read as a code - inventing
    /// one would classify off text the server never framed as a code.
    #[test]
    fn an_unbalanced_paren_is_treated_as_message_text() {
        let (code, _) = no_status("NO (TRYLATER oops");
        assert_eq!(code, None);
    }

    /// A real compile failure is the verdict, and stays diagnostics.
    #[test]
    fn checkscript_rejection_without_a_transient_code_is_a_verdict() {
        let outcome = validation_outcome(SieveStatus::No {
            code: None,
            message: "line 3: unknown command".into(),
        })
        .expect("a compile failure is a validation result, not an error");
        assert_eq!(outcome.diagnostics.len(), 1);
        assert_eq!(outcome.diagnostics[0].message, "line 3: unknown command");
    }

    /// But a busy server never judged the script. Reporting that as a
    /// validation failure tells the user their script is broken when it
    /// may be fine, and throws away the retryable classification.
    #[test]
    fn checkscript_trylater_is_an_error_not_a_validation_verdict() {
        let error = validation_outcome(SieveStatus::No {
            code: Some(SieveResponseCode::TryLater),
            message: "server busy".into(),
        })
        .expect_err("a transient refusal is not a verdict about the script");
        assert!(matches!(
            error,
            Error::Sieve {
                code: Some(SieveResponseCode::TryLater),
                ..
            }
        ));
    }

    #[test]
    fn parse_capabilities_extracts_starttls_and_sasl() {
        let response = SieveResponse {
            items: vec![
                SieveResponseItem::Line("\"IMPLEMENTATION\" \"Example\"".to_string()),
                SieveResponseItem::Line("\"SASL\" \"PLAIN XOAUTH2\"".to_string()),
                SieveResponseItem::Line("\"STARTTLS\"".to_string()),
            ],
            status: SieveStatus::Ok,
        };
        let caps = response.capabilities();
        assert!(caps.starttls);
        assert_eq!(caps.sasl, vec!["PLAIN".to_string(), "XOAUTH2".to_string()]);
    }

    #[test]
    fn parse_listscripts_line_reads_active_marker() {
        let script = parse_list_script("\"main\" ACTIVE")
            .expect("line parsed")
            .expect("script parsed");
        assert_eq!(script.name, "main");
        assert!(script.is_active);
    }

    #[test]
    fn quote_string_escapes_quotes_and_backslashes() {
        assert_eq!(quote_string("a\"b\\c").expect("quoted"), "\"a\\\"b\\\\c\"");
    }

    #[test]
    fn literal_len_accepts_server_literal_shape_only() {
        assert_eq!(literal_len("{12}"), Some(12));
        assert_eq!(literal_len("{12+}"), None);
    }

    #[test]
    fn script_name_arg_rejects_empty_name() {
        assert!(script_name_arg("").is_err());
        assert_eq!(script_name_arg("main").expect("name"), "\"main\"");
    }

    #[test]
    fn putscript_literal_command_includes_trailing_crlf() {
        let command =
            literal_command("PUTSCRIPT", Some("main"), "keep;").expect("command serialized");
        assert_eq!(command, b"PUTSCRIPT \"main\" {5+}\r\nkeep;\r\n");
    }

    #[test]
    fn checkscript_literal_command_includes_trailing_crlf() {
        let command = literal_command("CHECKSCRIPT", None, "keep;").expect("command serialized");
        assert_eq!(command, b"CHECKSCRIPT {5+}\r\nkeep;\r\n");
    }
}
