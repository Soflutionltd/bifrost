//! Reply framing shared by the blocking and async byte adapters.

use super::{MAX_RESPONSE_BYTES, MAX_RESPONSE_LINE_BYTES, ReplyMode};
use crate::transport::smtp::{
    error::{self, Error},
    response::{Response, parse_response},
};

/// Accept arbitrarily split byte chunks without retaining bytes past the line cap.
/// Deadlines and buffer admission belong to the adapter; syntax and limits do not.
#[derive(Default)]
pub(super) struct ReplyAssembler {
    response: String,
    line: Vec<u8>,
}

impl ReplyAssembler {
    /// Consume at most one line from `bytes`. Return its consumed length and
    /// the completed reply, leaving any surplus in the adapter's read buffer.
    pub(super) fn feed(&mut self, bytes: &[u8]) -> Result<(usize, Option<Response>), Error> {
        if bytes.is_empty() {
            // A peer closing under a send is a transport loss, not malformed
            // protocol. The transaction core supplies Unsent or InFlight from
            // the boundary so an enterprise send queue can retry safely.
            return Err(error::network(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "incomplete SMTP response",
            )));
        }
        let count = bytes
            .iter()
            .position(|&byte| byte == b'\n')
            .map_or(bytes.len(), |index| index + 1);
        if self.line.len() + count > MAX_RESPONSE_LINE_BYTES {
            return Err(error::parse("SMTP response line too long"));
        }
        if self.response.len() + self.line.len() + count > MAX_RESPONSE_BYTES {
            return Err(error::parse("SMTP response too large"));
        }
        self.line.extend_from_slice(&bytes[..count]);
        if self.line.last() != Some(&b'\n') {
            return Ok((count, None));
        }
        let line = std::str::from_utf8(&self.line)
            .map_err(|_| error::parse("SMTP response is not valid UTF-8"))?;
        #[cfg(feature = "tracing")]
        tracing::debug!("<< {}", super::escape_crlf(line));
        self.response.push_str(line);
        self.line.clear();
        match parse_response(&self.response) {
            Ok((_, response)) => Ok((count, Some(response))),
            Err(nom::Err::Incomplete(_)) => Ok((count, None)),
            Err(nom::Err::Failure(error) | nom::Err::Error(error)) => {
                Err(error::parse(error.to_string()))
            }
        }
    }

    pub(super) fn complete(response: Response, mode: ReplyMode) -> Result<Response, Error> {
        if mode.accepts_negative() || response.is_positive() {
            Ok(response)
        } else {
            Err(error::status(response))
        }
    }

    pub(super) fn check_surplus(buffered: bool, upgrading: bool) -> Result<(), Error> {
        if !buffered {
            return Ok(());
        }
        Err(error::parse(if upgrading {
            "SMTP server sent an unsolicited reply before the STARTTLS upgrade"
        } else {
            "SMTP server sent an unsolicited reply"
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_assembly_accepts_every_split_and_leaves_surplus_unconsumed() {
        let reply = "250-example with \u{e5}\r\n250 done\r\n".as_bytes();
        for split in 1..reply.len() {
            let mut assembler = ReplyAssembler::default();
            let mut result = None;
            for chunk in [&reply[..split], &reply[split..]] {
                let mut bytes = chunk;
                while !bytes.is_empty() {
                    let (count, response) = assembler.feed(bytes).unwrap();
                    bytes = &bytes[count..];
                    if let Some(response) = response {
                        result = Some(response);
                    }
                }
            }
            assert_eq!(result.unwrap().first_line(), Some("example with \u{e5}"));
        }
        let mut assembler = ReplyAssembler::default();
        let (consumed, response) = assembler.feed(b"250 ok\r\n421 surplus\r\n").unwrap();
        assert_eq!(consumed, 8);
        assert!(response.unwrap().has_code(250));
    }

    #[test]
    fn reply_limits_apply_across_chunks_before_allocation() {
        let mut assembler = ReplyAssembler::default();
        assembler
            .feed(&vec![b'x'; MAX_RESPONSE_LINE_BYTES])
            .unwrap();
        assert!(assembler.feed(b"x").unwrap_err().is_parse());
        assert_eq!(assembler.line.len(), MAX_RESPONSE_LINE_BYTES);

        let mut assembler = ReplyAssembler::default();
        let line = format!("250-{}\r\n", "x".repeat(MAX_RESPONSE_LINE_BYTES - 6));
        for _ in 0..MAX_RESPONSE_BYTES / MAX_RESPONSE_LINE_BYTES {
            assert!(assembler.feed(line.as_bytes()).unwrap().1.is_none());
        }
        let error = assembler.feed(b"250 done\r\n").unwrap_err();
        assert!(error.to_string().contains("response too large"));
        assert_eq!(assembler.response.len(), MAX_RESPONSE_BYTES);
    }

    #[test]
    fn malformed_utf8_is_protocol_but_eof_is_transport() {
        let mut assembler = ReplyAssembler::default();
        assert!(assembler.feed(b"250 \xff\r\n").unwrap_err().is_parse());
        for partial in [b"".as_slice(), b"250 partially read", b"250-first\r\n"] {
            let mut assembler = ReplyAssembler::default();
            if !partial.is_empty() {
                assert!(assembler.feed(partial).unwrap().1.is_none());
            }
            let error = assembler.feed(b"").unwrap_err();
            assert!(error.is_network(), "{error:?}");
        }
    }
}
