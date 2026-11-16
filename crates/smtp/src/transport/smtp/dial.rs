//! Tokio dialing policy belongs to connection setup, not runtime services.
#[cfg(feature = "test-support")]
use super::Tls;
use super::{AsyncSmtpConnection, Error, SmtpInfo};

pub(super) async fn connect_async(info: &SmtpInfo) -> Result<AsyncSmtpConnection, Error> {
    let hostname = info.server.as_str();
    let port = info.port;
    let unix_socket = info.unix_socket.as_deref();
    let timeout = info.timeout;
    let hello_name = &info.hello_name;
    let tls = &info.tls;
    let protocol = info.protocol;
    let metering = info.metering.clone();
    #[cfg(feature = "test-support")]
    let connector = info.connector.clone();

    #[cfg(feature = "test-support")]
    if connector.is_some() && (!matches!(tls, Tls::None) || unix_socket.is_some()) {
        return Err(crate::transport::smtp::error::invalid_input(
            "scripted connectors require plaintext TCP configuration",
        ));
    }
    let plan = super::DialPlan::new(tls, unix_socket)?;
    if let Some(path) = unix_socket {
        #[cfg(unix)]
        {
            return AsyncSmtpConnection::connect_unix_with_protocol(
                path, timeout, hello_name, protocol, metering,
            )
            .await;
        }
        #[cfg(not(unix))]
        {
            // Keep the binding used when Unix socket support is cfg-gated out.
            let _ = path;
            return Err(crate::transport::smtp::error::invalid_input(
                "Unix-domain LMTP sockets are only supported on Unix platforms",
            ));
        }
    }

    #[allow(unused_mut)]
    let mut conn = AsyncSmtpConnection::connect_with_protocol(
        (hostname, port),
        timeout,
        hello_name,
        plan.implicit_tls.cloned(),
        None,
        protocol,
        metering,
        #[cfg(feature = "test-support")]
        connector,
    )
    .await?;

    if let Some(parameters) = plan.starttls(conn.can_starttls()) {
        conn.starttls(parameters.clone(), hello_name).await?;
    }

    Ok(conn)
}
