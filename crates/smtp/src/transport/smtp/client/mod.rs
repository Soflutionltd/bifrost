//! SMTP client internals: the blocking (`SmtpConnection`) and async
//! (`AsyncSmtpConnection`) protocol drivers, the transparency codec, TLS
//! plumbing, and wire metering. This module is private; consumers drive
//! sends through the transports in `crate::transport`.

#[cfg(feature = "serde")]
use std::fmt::Debug;

use crate::transport::smtp::{Error, error, error::SmtpCommandPhase, response::Response};

#[cfg(feature = "tokio")]
pub(crate) use self::async_connection::AsyncSmtpConnection;
pub(crate) use self::connection::SmtpConnection;
use self::net::NetworkStream;
// pub: re-exported by smtp for caller-supplied native-tls credentials.
pub use self::tls::{Certificate, Identity};
// pub: re-exported by smtp for caller-supplied SMTP TLS configuration.
pub use self::tls::{CertificateStore, Tls, TlsParameters, TlsParametersBuilder, TlsVersion};

#[cfg(feature = "tokio")]
mod async_connection;
#[cfg(feature = "tokio")]
mod async_net;
#[cfg(all(test, feature = "tokio"))]
pub(crate) use async_net::AsyncNetworkStream;
mod connection;
mod core;
pub(crate) mod metering;
mod net;
mod tls;

pub(crate) use self::metering::WireMetering;
// Retain the legacy unlimited sentinel at its published path.
pub use self::metering::UNLIMITED_BANDWIDTH;

/// Total bytes cap on an SMTP response (Postfix `smtp_response_limit`).
pub(super) const MAX_RESPONSE_BYTES: usize = 100_000;

/// Single-line byte cap (Postfix `line_length_limit`).
pub(super) const MAX_RESPONSE_LINE_BYTES: usize = 1000;

/// Bound on a TLS handshake when the caller configured no timeout.
///
/// `timeout(None)` means "do not time out my SMTP conversation", not "let a TLS
/// negotiation hang": a peer that accepts the connection and then never answers
/// the `ClientHello` would otherwise park the caller forever. The value is
/// 30 seconds - an order of magnitude above a healthy handshake (two round
/// trips plus platform certificate verification, which can include revocation
/// fetches) even on a poor mobile link, and far below the five-minute command
/// timeouts RFC 5321 section 4.5.3.2 allows for a peer, so it never fails a
/// working server yet still ends a wedged one within a caller-tolerable time.
/// It applies ONLY when no timeout is configured; a configured timeout is used
/// as given, larger or smaller.
pub(super) const DEFAULT_TLS_HANDSHAKE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Maximum number of RCPT commands outstanding in a PIPELINING window.
///
/// RFC 2920 requires clients to respect the peer's TCP window. Draining each
/// window before writing the next prevents a very large recipient group from
/// filling both directions at once when a peer waits to send replies.
pub(super) const PIPELINING_RECIPIENT_WINDOW: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnectionState {
    Ok,
    Broken,
    Closed,
}

impl ConnectionState {
    fn verify(self) -> Result<(), Error> {
        match self {
            ConnectionState::Ok => Ok(()),
            ConnectionState::Broken => Err(error::connection("SMTP connection is broken")),
            ConnectionState::Closed => Err(error::connection("SMTP connection is closed")),
        }
    }
}

/// The codec used for transparency.
///
/// Bare LF is treated as a line break (defended by test), so LF-smuggled
/// `.`-lines get stuffed. Bare CR deliberately is NOT: after `\r` +
/// non-LF the state is `MiddleOfLine`, so `...\r.` is never stuffed.
/// Exploiting that would require a relay that treats a lone CR as EOL,
/// which is theoretical; the asymmetry is a documented accepted shape.
#[derive(Debug)]
struct ClientCodec {
    status: CodecStatus,
}

impl ClientCodec {
    /// Creates a new client codec
    pub(crate) fn new() -> Self {
        Self {
            status: CodecStatus::StartOfNewLine,
        }
    }

    /// Adds transparency
    fn encode(&mut self, frame: &[u8], buf: &mut Vec<u8>) {
        for &b in frame {
            buf.push(b);
            match (b, self.status) {
                (b'\r', _) => {
                    self.status = CodecStatus::StartingNewLine;
                }
                (b'\n', _) => {
                    self.status = CodecStatus::StartOfNewLine;
                }
                (_, CodecStatus::StartingNewLine) => {
                    self.status = CodecStatus::MiddleOfLine;
                }
                (b'.', CodecStatus::StartOfNewLine) => {
                    self.status = CodecStatus::MiddleOfLine;
                    buf.push(b'.');
                }
                (_, CodecStatus::StartOfNewLine) => {
                    self.status = CodecStatus::MiddleOfLine;
                }
                _ => {}
            }
        }
    }
}

/// Return the RFC 1870 message size for a DATA transfer.
///
/// The leading CRLF of the DATA terminator is the message's final CRLF. Reuse
/// it when supplied by the caller and otherwise count the two octets the
/// writer adds. Transparency dots and the terminator line itself stay
/// excluded, as RFC 1870 requires.
pub(crate) fn smtp_data_size(message: &[u8]) -> usize {
    message.len() + usize::from(!message.ends_with(b"\r\n")) * 2
}

fn data_terminator(ends_with_crlf: bool) -> &'static [u8] {
    if ends_with_crlf {
        b".\r\n"
    } else {
        b"\r\n.\r\n"
    }
}

/// A failure at a pipelined wire boundary, carrying the boundary it happened
/// at.
///
/// This exists to make the phase decoration structural rather than a
/// convention. The pipelined drivers run inside inner functions that return
/// this type, and it deliberately has:
///
/// - no `From<Error>` impl, so `?` on an undecorated `Result<_, Error>` does
///   not compile inside those functions, and
/// - no constructor that does not take a `SmtpCommandPhase`.
///
/// So the only way out of a pipelined driver is through a phase. The outer
/// wrapper is the single place that converts back to `Error`, which is where
/// the phase is stamped. Adding a new boundary to the pipelined path cannot
/// silently ship undecorated: it will not build.
///
/// Negative replies matter here as much as transport failures. A server
/// rejecting `MAIL FROM`, a `RCPT TO` or `DATA` is a normal outcome whose
/// phase feeds `classify_response`, including the recipient-lane split, so an
/// undecorated rejection makes PIPELINING classify differently from the
/// non-pipelined path for the same wire exchange.
pub(super) struct PhasedError {
    phase: SmtpCommandPhase,
    error: Error,
}

impl PhasedError {
    pub(super) fn new(phase: SmtpCommandPhase, error: Error) -> Self {
        Self { phase, error }
    }

    pub(super) fn into_error(self) -> Error {
        self.error.with_phase(self.phase)
    }
}

/// Merge RCPT-time rejections with the final statuses for accepted LMTP
/// recipients without trusting a server-controlled count at the call site.
///
/// Both count mismatches return `error::internal`, and they ARE internal: the
/// only caller (`DirectLmtp` in `core.rs`) counts an accepted recipient for
/// every `None` it pushes and then reads exactly that many final statuses, so
/// the two counts cannot disagree over anything the server sent - only if that
/// bookkeeping is broken. Neither carries a `debug_assert`, deliberately: the
/// `batch.rs` assert-plus-release-fallback idiom exists where a LANE has to
/// resolve to something and the fallback is a guess; this is an invariant with
/// a real error to return.
fn merge_lmtp_statuses(
    recipient_statuses: Vec<Option<Response>>,
    delivery_statuses: Vec<Response>,
) -> Result<Vec<Response>, Error> {
    let mut delivery_statuses = delivery_statuses.into_iter();
    let mut statuses = Vec::with_capacity(recipient_statuses.len());

    for response in recipient_statuses {
        match response {
            Some(response) => statuses.push(response),
            None => statuses.push(delivery_statuses.next().ok_or_else(|| {
                error::internal("server returned fewer LMTP statuses than accepted recipients")
            })?),
        }
    }

    if delivery_statuses.next().is_some() {
        return Err(error::internal(
            "server returned more LMTP statuses than accepted recipients",
        ));
    }

    Ok(statuses)
}

#[derive(Debug, Copy, Clone)]
#[allow(clippy::enum_variant_names)]
enum CodecStatus {
    /// We are past the first character of the current line
    MiddleOfLine,
    /// We just read a `\r` character
    StartingNewLine,
    /// We are at the start of a new line
    StartOfNewLine,
}

/// Returns the string replacing all the CRLF with "\<CRLF\>"
/// Used for debug displays
#[cfg(feature = "tracing")]
pub(super) fn escape_crlf(string: &str) -> String {
    string.replace("\r\n", "<CRLF>")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_codec() {
        let mut buf = Vec::new();
        let mut codec = ClientCodec::new();

        codec.encode(b".\r\n", &mut buf);
        codec.encode(b"test\r\n", &mut buf);
        codec.encode(b"test\r\n\r\n", &mut buf);
        codec.encode(b".\r\n", &mut buf);
        codec.encode(b"\r\ntest", &mut buf);
        codec.encode(b"te\r\n.\r\nst", &mut buf);
        codec.encode(b"test", &mut buf);
        codec.encode(b"test.", &mut buf);
        codec.encode(b"test\n", &mut buf);
        codec.encode(b".test\n", &mut buf);
        codec.encode(b"test", &mut buf);
        codec.encode(b"test", &mut buf);
        codec.encode(b"test\r\n", &mut buf);
        codec.encode(b".test\r\n", &mut buf);
        codec.encode(b"test.\r\n", &mut buf);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "..\r\ntest\r\ntest\r\n\r\n..\r\n\r\ntestte\r\n..\r\nsttesttest.test\n..test\ntesttesttest\r\n..test\r\ntest.\r\n"
        );
    }

    #[test]
    fn codec_stuffs_a_dot_at_the_very_start_of_the_message() {
        let mut buf = Vec::new();
        let mut codec = ClientCodec::new();

        codec.encode(b".", &mut buf);

        assert_eq!(String::from_utf8(buf).unwrap(), "..");
    }

    #[test]
    fn codec_stuffs_across_chunk_boundaries() {
        // The codec is fed one chunk per `message_iter` item, so the
        // start-of-line state must survive a CRLF split across two calls.
        let mut buf = Vec::new();
        let mut codec = ClientCodec::new();

        codec.encode(b"line\r", &mut buf);
        codec.encode(b"\n", &mut buf);
        codec.encode(b".hidden\r\n", &mut buf);

        assert_eq!(String::from_utf8(buf).unwrap(), "line\r\n..hidden\r\n");
    }

    #[test]
    fn codec_only_stuffs_a_dot_that_starts_a_line() {
        let mut buf = Vec::new();
        let mut codec = ClientCodec::new();

        codec.encode(b"a.b\r\n.c\r\n", &mut buf);

        assert_eq!(String::from_utf8(buf).unwrap(), "a.b\r\n..c\r\n");
    }

    #[test]
    fn codec_treats_a_bare_lf_as_a_line_break() {
        let mut buf = Vec::new();
        let mut codec = ClientCodec::new();

        codec.encode(b"body\n.\nMAIL FROM:<attacker@example.com>\r\n", &mut buf);

        assert_eq!(
            String::from_utf8(buf).unwrap(),
            "body\n..\nMAIL FROM:<attacker@example.com>\r\n"
        );
    }

    #[test]
    fn data_size_reuses_an_existing_final_crlf() {
        assert_eq!(smtp_data_size(b"line\r\n"), 6);
        assert_eq!(smtp_data_size(b"line"), 6);
        assert_eq!(smtp_data_size(b""), 2);
        assert_eq!(smtp_data_size(b"line\r"), 7);
        assert_eq!(smtp_data_size(b"line\n"), 7);
        assert_eq!(smtp_data_size(b".quoted\r\n"), 9);
    }

    #[test]
    fn connection_state_verify_rejects_anything_but_ok() {
        assert!(ConnectionState::Ok.verify().is_ok());
        assert!(ConnectionState::Broken.verify().is_err());
        assert!(ConnectionState::Closed.verify().is_err());
        assert!(
            ConnectionState::Broken
                .verify()
                .unwrap_err()
                .is_connection()
        );
    }

    #[test]
    fn response_caps_match_the_postfix_defaults() {
        // A regression guard on the caps the response reader enforces: a
        // change here silently changes how much attacker-controlled data the
        // reader will buffer.
        assert_eq!(MAX_RESPONSE_LINE_BYTES, 1000);
        assert_eq!(MAX_RESPONSE_BYTES, 100_000);
    }

    #[test]
    #[cfg(feature = "tracing")]
    fn test_escape_crlf() {
        assert_eq!(escape_crlf("\r\n"), "<CRLF>");
        assert_eq!(escape_crlf("EHLO my_name\r\n"), "EHLO my_name<CRLF>");
        assert_eq!(
            escape_crlf("EHLO my_name\r\nSIZE 42\r\n"),
            "EHLO my_name<CRLF>SIZE 42<CRLF>"
        );
    }
}
