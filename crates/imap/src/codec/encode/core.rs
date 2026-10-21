use bytes::{Bytes, BytesMut};

use crate::error::Error;
use crate::types::response::Capability;

/// Controls how the encoder produces literal markers.
///
/// RFC 3501 Section 4.3 defines synchronizing literals (`{N}\r\n`) which
/// require the client to wait for a `+` continuation response. RFC 7888
/// introduces two extensions that allow non-synchronizing literals (`{N+}\r\n`):
///
/// - **LITERAL+** (RFC 7888 Section 4): non-synchronizing literals of any size.
/// - **LITERAL-** (RFC 7888 Section 5): non-synchronizing literals up to 4096
///   bytes; larger literals MUST use synchronizing form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralMode {
    /// No literal extension  -  all literals use synchronizing form `{N}\r\n`
    /// (RFC 3501 Section 4.3).
    Synchronizing,
    /// LITERAL+ (RFC 7888 Section 4)  -  non-synchronizing `{N+}\r\n` for all sizes.
    LiteralPlus,
    /// LITERAL- (RFC 7888 Section 5)  -  non-synchronizing `{N+}\r\n` only when
    /// the literal is <= 4096 bytes; larger literals use synchronizing `{N}\r\n`.
    LiteralMinus,
}

/// Encoding context derived from the current connection state.
///
/// Carries the capability set, enabled extensions, and negotiated literal
/// mode so the encoder can:
/// - reject commands whose prerequisite capability is not advertised (I6),
/// - select the correct wire encoding for mailbox names and literals.
///
/// Constructed in production by the driver from the protocol state it owns
/// (`connection::driver::build_encode_options`), at the moment a command is
/// encoded; the connection handle has no builder of its own.
#[derive(Debug, Clone)]
pub(crate) struct EncodeOptions {
    /// RFC 6855 / RFC 9051 `UTF8=ACCEPT` mode.
    ///
    /// When `true`, non-ASCII UTF-8 bytes are allowed in quoted strings
    /// (RFC 6855 Section 3 / RFC 9051 Section 9) and mailbox names are
    /// sent as raw UTF-8 instead of modified UTF-7.
    pub(crate) utf8_mode: bool,
    /// RFC 7888 LITERAL+ vs synchronizing literal (RFC 3501 Section 4.3).
    pub(crate) literal_mode: LiteralMode,
    /// Server-advertised capabilities (RFC 3501 Section 7.2.1).
    pub(crate) capabilities: Vec<Capability>,
    /// Extensions successfully enabled with ENABLE.
    pub(crate) enabled: Vec<String>,
}

impl EncodeOptions {
    /// Check whether the capability is usable on this connection, through the
    /// single authority `crate::types::profile::supports` (advertised, implied
    /// by QRESYNC for CONDSTORE, or folded into active IMAP4rev2).
    pub(super) fn has_capability(&self, cap: &Capability) -> bool {
        crate::types::profile::supports(&self.capabilities, &self.enabled, cap)
    }

    /// Check whether CONDSTORE is available (explicitly or via QRESYNC).
    ///
    /// RFC 7162 Section 3.2.3: a server that advertises QRESYNC implicitly
    /// supports CONDSTORE; the authority carries that rule. The encoder checks
    /// this whenever a CONDSTORE modifier (CHANGEDSINCE, UNCHANGEDSINCE,
    /// VANISHED) is present.
    pub(super) fn has_condstore(&self) -> bool {
        self.has_capability(&Capability::Condstore)
    }

    /// Whether `UTF8=ACCEPT` (RFC 6855) has been enabled on this connection.
    ///
    /// Narrower than [`utf8_mode`](Self::utf8_mode), which is also true under
    /// active `IMAP4rev2`: only `UTF8=ACCEPT` obliges APPEND to wrap message
    /// data in the RFC 6855 Section 4 `UTF8 (...)` data extension.
    pub(super) fn utf8_accept_enabled(&self) -> bool {
        self.enabled
            .iter()
            .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"))
    }

    /// Whether `literal8` may carry the non-synchronizing `+` modifier.
    ///
    /// RFC 7888 Section 6: on `IMAP4rev1` only when BOTH BINARY and a literal
    /// extension apply (the literal-extension half is the caller's
    /// [`LiteralMode`]). RFC 9051 Section 9 redefines `literal8` for pure
    /// `IMAP4rev2` with no `+` modifier, so it is never eligible there.
    ///
    /// Deliberately the ADVERTISED token, not `supports(Binary)`: rev2 folds
    /// in only BINARY's FETCH side, and a rev2 connection is excluded here
    /// anyway.
    pub(super) fn literal8_non_sync_allowed(&self) -> bool {
        self.capabilities.contains(&Capability::Binary) && !self.imap4rev2_active()
    }

    /// RFC 9051 Section6.3.1, via the single authority in
    /// `crate::types::profile`. The rule is NOT restated here: this view owns a
    /// coherent `(capabilities, enabled)` pair and hands it over.
    fn imap4rev2_active(&self) -> bool {
        crate::types::profile::imap4rev2_active(&self.capabilities, &self.enabled)
    }
}

/// RFC 7888 Section 5: maximum non-synchronizing literal size for LITERAL-.
pub(crate) const LITERAL_MINUS_MAX: usize = 4096;

/// Which literal syntax carries a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralForm {
    /// Classic `{n}` literal (RFC 3501 Section 4.3, RFC 9051 Section 4.3).
    Classic,
    /// `~{n}` literal8 (RFC 3516 Section 4.2, RFC 4466 Section 2.1): may carry
    /// NUL. Its non-synchronizing form has the extra eligibility rule of
    /// [`EncodeOptions::literal8_non_sync_allowed`].
    Literal8,
}

/// One encoded IMAP command, ready for the wire.
///
/// The single encoded form every command takes. Each segment is a list of
/// chunks; the sender writes every chunk of a segment in order, then waits for
/// a server `+` continuation before the next segment (RFC 3501 Section 4.3),
/// with no wait after the last. A segment ends exactly on a synchronizing
/// literal marker, so N synchronizing literals give N+1 segments, and no
/// segment is ever empty.
///
/// The boundaries are STRUCTURAL: [`CommandWriter`] records them as it emits
/// each marker. Nothing ever rescans encoded bytes to rediscover where a
/// literal begins, which is what keeps marker-shaped bytes inside a literal
/// payload from being mistaken for framing - there is no scanner to mistake
/// them.
///
/// Chunks are `Bytes`. Command syntax is coalesced into few chunks; a payload
/// handed over as `Bytes` (an APPEND message body) is carried by reference,
/// never copied into a command buffer.
#[derive(Clone)]
pub(crate) struct WireCommand {
    segments: Vec<Vec<Bytes>>,
}

impl WireCommand {
    /// The wire segments. Between consecutive segments the connection must
    /// wait for a `+` continuation response (RFC 3501 Section 4.3).
    pub(crate) fn segments(&self) -> &[Vec<Bytes>] {
        &self.segments
    }

    /// The whole command as one byte string, as the server receives it.
    #[cfg(test)]
    pub(crate) fn to_vec(&self) -> Vec<u8> {
        self.segments
            .iter()
            .flatten()
            .flat_map(|chunk| chunk.iter().copied())
            .collect()
    }
}

/// Deliberately prints only the shape: a command can carry a whole message
/// body or a credential literal, and `?cmd`-style tracing must never put
/// either in a log.
impl std::fmt::Debug for WireCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let octets: usize = self.segments.iter().flatten().map(Bytes::len).sum();
        f.debug_struct("WireCommand")
            .field("segments", &self.segments.len())
            .field("octets", &octets)
            .finish()
    }
}

/// The one place literal framing is decided.
///
/// Encoders write command syntax through [`raw`](Self::raw), strings through
/// [`string`](Self::string), and payloads through [`literal`](Self::literal) /
/// [`literal_bytes`](Self::literal_bytes). The writer chooses each marker's
/// `+` modifier ONCE, from the [`EncodeOptions`] the driver built from live
/// state, and records the segment boundary at every synchronizing marker as it
/// writes it. What a command may carry at all (APPEND's BINARY requirement for
/// a NUL body, METADATA's literal8 values) stays with that command's encoder;
/// the writer owns marker choice and boundaries and nothing else.
pub(crate) struct CommandWriter {
    literal_mode: LiteralMode,
    literal8_non_sync: bool,
    segments: Vec<Vec<Bytes>>,
    current: Vec<Bytes>,
    buf: BytesMut,
    invalid_nul: bool,
}

impl CommandWriter {
    pub(crate) fn new(opts: &EncodeOptions) -> Self {
        Self {
            literal_mode: opts.literal_mode,
            literal8_non_sync: opts.literal8_non_sync_allowed(),
            segments: Vec::new(),
            current: Vec::new(),
            buf: BytesMut::new(),
            invalid_nul: false,
        }
    }

    /// The negotiated literal mode, for validators that judge caller-written
    /// literal markers against it.
    pub(crate) const fn literal_mode(&self) -> LiteralMode {
        self.literal_mode
    }

    /// Append command syntax verbatim.
    pub(crate) fn raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Encode a byte string as a quoted string or a literal, depending on
    /// content (RFC 3501 Section 9 / RFC 9051 Section 9).
    ///
    /// NUL bytes (%x00) are rejected: RFC 3501 Section 9 rule (3) says
    /// "The ASCII NUL character, %x00, MUST NOT be used at any time". Removing
    /// it would silently change mailbox and metadata identities.
    ///
    /// Without `utf8`, only printable ASCII (`0x20..0x7F`) is quoted. DEL is
    /// excluded because RFC 9051 Section 9 drops it from CHAR, and control
    /// characters other than CR/LF, though valid CHAR under RFC 3501, are
    /// rejected in quoted strings by many servers, so "be conservative in what
    /// you send" sends them as a literal. With `utf8` (UTF8=ACCEPT per RFC 6855
    /// Section 3, or active rev2 per RFC 9051 Section 9) valid UTF-8 is also
    /// quotable, under the same control-character and DEL exclusions.
    pub(crate) fn string(&mut self, data: &[u8], utf8: bool) {
        if data.contains(&0) {
            self.invalid_nul = true;
            return;
        }
        let quotable = if utf8 {
            std::str::from_utf8(data).is_ok()
                && data.iter().all(|&b| (b >= 0x20 && b != 0x7F) || b >= 0x80)
        } else {
            data.iter().all(|&b| (0x20..0x7F).contains(&b))
        };
        if quotable {
            self.quoted(data);
        } else {
            self.literal(data, LiteralForm::Classic);
        }
    }

    /// Write `data` as a quoted string, escaping `\` and `"`.
    ///
    /// RFC 3501 Section 9: `quoted = DQUOTE *QUOTED-CHAR DQUOTE`, where
    /// quoted-specials (backslash and double-quote) are escaped with
    /// backslash. The caller must have established that `data` is quotable.
    fn quoted(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(b"\"");
        for &byte in data {
            if byte == b'\\' || byte == b'"' {
                self.buf.extend_from_slice(b"\\");
            }
            self.buf.extend_from_slice(&[byte]);
        }
        self.buf.extend_from_slice(b"\"");
    }

    /// Write a literal whose payload is copied into the command buffer. For
    /// short payloads; a large one should use [`literal_bytes`](Self::literal_bytes).
    pub(crate) fn literal(&mut self, data: &[u8], form: LiteralForm) {
        if self.marker(data.len(), form) {
            self.end_segment();
        }
        self.buf.extend_from_slice(data);
    }

    /// Write a literal whose payload is carried by reference: the `Bytes` is
    /// cloned into the command (a reference-count bump), never copied.
    pub(crate) fn literal_bytes(&mut self, data: Bytes, form: LiteralForm) {
        if self.marker(data.len(), form) {
            self.end_segment();
        } else {
            self.flush_buf();
        }
        self.current.push(data);
    }

    /// Write `{n}` / `{n+}` (or the `~` literal8 forms) and CRLF, returning
    /// whether the marker is synchronizing.
    ///
    /// RFC 7888 Section 4: LITERAL+ is non-synchronizing for any size.
    /// RFC 7888 Section 5 / RFC 9051 Section 4.3: LITERAL- (and rev2) only up
    /// to 4096 octets. RFC 3501 Section 4.3: no extension, always
    /// synchronizing. RFC 7888 Section 6 / RFC 9051 Section 9: literal8
    /// additionally needs BINARY on a non-rev2 connection.
    fn marker(&mut self, len: usize, form: LiteralForm) -> bool {
        let non_sync_by_mode = match self.literal_mode {
            LiteralMode::LiteralPlus => true,
            LiteralMode::LiteralMinus => len <= LITERAL_MINUS_MAX,
            LiteralMode::Synchronizing => false,
        };
        let non_sync = non_sync_by_mode
            && match form {
                LiteralForm::Classic => true,
                LiteralForm::Literal8 => self.literal8_non_sync,
            };
        self.buf.extend_from_slice(match form {
            LiteralForm::Classic => b"{".as_slice(),
            LiteralForm::Literal8 => b"~{".as_slice(),
        });
        self.buf.extend_from_slice(len.to_string().as_bytes());
        self.buf
            .extend_from_slice(if non_sync { b"+}\r\n" } else { b"}\r\n" });
        !non_sync
    }

    /// Move buffered syntax into the current segment as one chunk.
    fn flush_buf(&mut self) {
        if !self.buf.is_empty() {
            self.current.push(self.buf.split().freeze());
        }
    }

    /// End the current segment on the synchronizing marker just written: the
    /// sender waits for `+` before writing what follows.
    fn end_segment(&mut self) {
        self.flush_buf();
        self.segments.push(std::mem::take(&mut self.current));
    }

    /// The finished command. Every command ends with its CRLF, written by the
    /// encoder, so the last segment is never empty.
    pub(crate) fn finish(mut self) -> Result<WireCommand, Error> {
        if self.invalid_nul {
            return Err(Error::InvalidInput(
                "IMAP string data must not contain NUL (RFC 3501 Section 9)".into(),
            ));
        }
        self.flush_buf();
        debug_assert!(
            !self.current.is_empty(),
            "a command always ends with syntax after its last literal"
        );
        self.segments.push(self.current);
        Ok(WireCommand {
            segments: self.segments,
        })
    }
}
