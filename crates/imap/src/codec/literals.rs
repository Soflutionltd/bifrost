/// RFC 9051 Section 9 ceiling for `number64`: an unsigned 63-bit integer, so
/// the largest legal literal count is `i64::MAX`.
pub(crate) const NUMBER64_MAX: u64 = i64::MAX as u64;

/// The three answers [`literal_marker_at`] can give.
///
/// "Not a marker" and "a marker naming octets that cannot legally exist" are
/// different facts and the right reaction differs by call site: on the read
/// path an out-of-range count is fatal framing (waiting for those octets would
/// stall forever), while in a caller-supplied SEARCH criteria string it is
/// text the validator has already judged. Collapsing the two into `None` is
/// what let the ceiling go unenforced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralMarker {
    /// Well-formed marker whose count is a legal RFC 9051 `number64`.
    Counted {
        /// Offset of the first literal-body octet.
        data_start: usize,
        /// Declared octet count; always `<= NUMBER64_MAX`.
        size: u64,
        /// `false` for the LITERAL+/LITERAL- `{N+}` form.
        synchronizing: bool,
    },
    /// Well-formed framing whose count is above the `number64` ceiling.
    /// `data_start` is where the body would have begun, so a caller can still
    /// tell whether this marker sits at the boundary it was asking about.
    CountOutOfRange { data_start: usize, size: u64 },
    /// No literal marker begins at this position.
    NotAMarker,
}

/// Parse a literal marker at `pos`.
///
/// The one marker parser, shared by the read path's framing pre-check and the
/// encoder's walk over caller-written SEARCH-family criteria. The encoder
/// never scans its OWN output: the boundaries of the literals it emits are
/// recorded structurally by `codec::encode::CommandWriter` as it writes them.
///
/// The count is carried as `u64`, not `usize`: RFC 9051 Section 9 declares it
/// as `number64`, so a marker can name a value that no `usize` on a 32-bit
/// target can hold. Narrowing here would make the parse result depend on the
/// pointer width; callers decide what an unrepresentable count means for them.
///
/// A digit run too long for `u64` is [`LiteralMarker::NotAMarker`], not
/// `CountOutOfRange`: such a line is not recognizable framing at all, and the
/// read path deliberately hands it to the decoder rather than declaring a
/// framing error of its own.
///
/// Both classic and LITERAL+ markers use the same counted framing. Callers
/// that scan a complete command must skip an already non-synchronizing body,
/// too, or marker-like data in that body becomes command syntax.
pub(crate) fn literal_marker_at(buf: &[u8], pos: usize) -> LiteralMarker {
    if buf.get(pos) != Some(&b'{') {
        return LiteralMarker::NotAMarker;
    }

    let start = pos + 1;
    let mut end = start;
    while end < buf.len() && buf[end].is_ascii_digit() {
        end += 1;
    }
    if end == start {
        return LiteralMarker::NotAMarker;
    }

    let synchronizing = match buf.get(end) {
        Some(b'}') => true,
        Some(b'+') => {
            end += 1;
            false
        }
        _ => return LiteralMarker::NotAMarker,
    };
    if buf.get(end) != Some(&b'}')
        || buf.get(end + 1) != Some(&b'\r')
        || buf.get(end + 2) != Some(&b'\n')
    {
        return LiteralMarker::NotAMarker;
    }

    let digit_end = if synchronizing { end } else { end - 1 };
    let Some(size) = std::str::from_utf8(&buf[start..digit_end])
        .ok()
        .and_then(|digits| digits.parse::<u64>().ok())
    else {
        return LiteralMarker::NotAMarker;
    };
    if size > NUMBER64_MAX {
        return LiteralMarker::CountOutOfRange {
            data_start: end + 3,
            size,
        };
    }
    LiteralMarker::Counted {
        data_start: end + 3,
        size,
        synchronizing,
    }
}
