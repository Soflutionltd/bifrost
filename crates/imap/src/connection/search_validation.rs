#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // Capability checks belong to the live encoder, including for commands
    // queued behind CAPABILITY or ENABLE. Keep this test adapter on that same
    // path rather than a second implementation of the gates.
    #[cfg(test)]
    fn validate_search_criteria_capabilities(&self, criteria: &str) -> Result<(), Error> {
        let snap = self.state_rx.borrow();
        crate::codec::encode::encode_command(
            "test",
            &Command::Search {
                criteria: criteria.into(),
            },
            &crate::codec::encode::EncodeOptions {
                utf8_mode: false,
                literal_mode: crate::codec::encode::LiteralMode::LiteralPlus,
                capabilities: snap.capabilities.clone(),
                enabled: snap.enabled.clone(),
            },
        )
        .map(|_| ())
    }

    /// Returns `true` when `criteria` contains the given search-key atom
    /// outside quoted strings.
    ///
    /// SEARCH, SORT, and THREAD criteria are free-form IMAP search syntax, so
    /// extension keys such as `MODSEQ` can appear nested inside parenthesized
    /// groups. RFC 3501 Section 6.4.4 and RFC 9051 Section 6.4.4 also define
    /// many standard keys whose operands are `astring`, dates, numbers, or
    /// sequence-sets. The scanner therefore walks the SEARCH grammar and skips
    /// operands for known keys, so payloads like `HEADER Subject "MODSEQ"`,
    /// `BODY {12}\r\nhello MODSEQ`, and `BODY MODSEQ` do not trigger extension
    /// capability checks just because their data happens to equal a gated
    /// search-key atom.
    pub(crate) fn search_criteria_contains_atom(criteria: &str, atom: &str) -> bool {
        let bytes = criteria.as_bytes();
        let mut i = 0usize;

        Self::search_criteria_skip_whitespace(bytes, &mut i);

        // RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4: SEARCH criteria
        // may start with an optional `CHARSET <astring>` prefix.
        let mut lookahead = i;
        if let Some(SearchCriteriaItem::Bare(token)) =
            Self::search_criteria_consume_item(criteria, bytes, &mut lookahead)
            && token.eq_ignore_ascii_case("CHARSET")
        {
            i = lookahead;
            let _ = Self::search_criteria_consume_item(criteria, bytes, &mut i);
        }

        while i < bytes.len() {
            if Self::search_criteria_contains_atom_in_key(criteria, bytes, &mut i, atom) {
                return true;
            }
            Self::search_criteria_skip_whitespace(bytes, &mut i);
        }

        false
    }

    /// RFC 3501 Section 4.3: a client literal is `{number}` or `{number+}`
    /// followed by CRLF and exactly `number` octets of data.
    ///
    /// SEARCH-family criteria can embed such literals because many search keys
    /// take `astring` operands (RFC 3501 Section 6.4.4, RFC 5256 Section 5).
    /// When scanning criteria for extension atoms, the literal payload must be
    /// skipped verbatim so its contents are not mistaken for syntax.
    fn search_criteria_literal_end(bytes: &[u8], pos: usize) -> Option<usize> {
        use super::literals::{LiteralMarker, literal_marker_at};
        let LiteralMarker::Counted {
            data_start, size, ..
        } = literal_marker_at(bytes, pos)
        else {
            return None;
        };
        let end = data_start.checked_add(usize::try_from(size).ok()?)?;
        (end <= bytes.len()).then_some(end)
    }

    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4: SEARCH keys are
    /// separated by linear whitespace.
    fn search_criteria_skip_whitespace(bytes: &[u8], pos: &mut usize) {
        while *pos < bytes.len() && matches!(bytes[*pos], b' ' | b'\t' | b'\r' | b'\n') {
            *pos += 1;
        }
    }

    /// RFC 3501 Section 6.4.4 / RFC 9051 Section 6.4.4: a SEARCH program is a
    /// sequence of search keys, each of which can be a parenthesized group or
    /// an atom with zero or more operands.
    fn search_criteria_contains_atom_in_key(
        criteria: &str,
        bytes: &[u8],
        pos: &mut usize,
        atom: &str,
    ) -> bool {
        Self::search_criteria_skip_whitespace(bytes, pos);

        if *pos >= bytes.len() {
            return false;
        }

        if bytes[*pos] == b'(' {
            *pos += 1;
            loop {
                Self::search_criteria_skip_whitespace(bytes, pos);
                if *pos >= bytes.len() {
                    return false;
                }
                if bytes[*pos] == b')' {
                    *pos += 1;
                    return false;
                }
                if Self::search_criteria_contains_atom_in_key(criteria, bytes, pos, atom) {
                    return true;
                }
            }
        }

        let item = Self::search_criteria_consume_item(criteria, bytes, pos);

        let Some(SearchCriteriaItem::Bare(token)) = item else {
            return false;
        };

        if token.eq_ignore_ascii_case(atom) {
            return true;
        }

        let upper = token.to_ascii_uppercase();
        match upper.as_str() {
            // 1-operand keys (astring or number/date)
            "BCC" | "BODY" | "CC" | "FROM" | "KEYWORD" | "SUBJECT" | "TEXT" | "TO"
            | "UNKEYWORD" | "EMAILID" | "THREADID" | "LARGER" | "SMALLER" | "BEFORE" | "ON"
            | "SINCE" | "SENTBEFORE" | "SENTON" | "SENTSINCE" | "OLDER" | "YOUNGER"
            | "SAVEDBEFORE" | "SAVEDON" | "SAVEDSINCE" | "UID" => {
                let operand = Self::search_criteria_consume_item(criteria, bytes, pos);
                // RFC 5182 Section 2.1 permits `$` as UID's sequence-set
                // operand. It needs the same SEARCHRES gate as bare `$`.
                if upper == "UID"
                    && atom == "$"
                    && matches!(operand, Some(SearchCriteriaItem::Bare("$")))
                {
                    return true;
                }
            }

            // HEADER takes two astring operands
            "HEADER" => {
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
            }

            // NOT takes one search-key operand (handled recursively)
            "NOT" => {
                return Self::search_criteria_contains_atom_in_key(criteria, bytes, pos, atom);
            }

            // OR takes two search-key operands (handled recursively)
            "OR" => {
                if Self::search_criteria_contains_atom_in_key(criteria, bytes, pos, atom) {
                    return true;
                }
                return Self::search_criteria_contains_atom_in_key(criteria, bytes, pos, atom);
            }

            // MODSEQ has a variable number of operands
            "MODSEQ" => {
                Self::search_criteria_consume_modseq_operands(criteria, bytes, pos);
            }

            // Sequence set or unknown key  -  treat as 0-operand
            _ => {}
        }

        false
    }

    /// Consume the next item from the search criteria stream.
    ///
    /// Returns `Some(SearchCriteriaItem::Bare(token))` for a bare atom,
    /// `Some(SearchCriteriaItem::Quoted)` for a quoted string,
    /// `Some(SearchCriteriaItem::Literal)` for a literal `{N}\r\n...`,
    /// or `None` at end-of-input.
    fn search_criteria_consume_item<'a>(
        criteria: &'a str,
        bytes: &[u8],
        pos: &mut usize,
    ) -> Option<SearchCriteriaItem<'a>> {
        Self::search_criteria_skip_whitespace(bytes, pos);

        if *pos >= bytes.len() {
            return None;
        }

        if bytes[*pos] == b'"' {
            *pos += 1;
            while *pos < bytes.len() && bytes[*pos] != b'"' {
                if bytes[*pos] == b'\\' {
                    *pos += 1;
                }
                *pos += 1;
            }
            if *pos < bytes.len() {
                *pos += 1;
            }
            return Some(SearchCriteriaItem::Quoted);
        }

        if let Some(end) = Self::search_criteria_literal_end(bytes, *pos) {
            *pos = end;
            return Some(SearchCriteriaItem::Literal);
        }

        let start = *pos;
        while *pos < bytes.len()
            && !matches!(bytes[*pos], b' ' | b'\t' | b'\r' | b'\n' | b'(' | b')')
        {
            *pos += 1;
        }

        if *pos > start {
            Some(SearchCriteriaItem::Bare(&criteria[start..*pos]))
        } else {
            // A malformed top-level closing parenthesis is not an item,
            // but the scanner must still make progress. Parenthesized
            // groups consume their own closing delimiter before reaching
            // this branch.
            if bytes[*pos] == b')' {
                *pos += 1;
            }
            None
        }
    }

    /// RFC 7162 Section 3.1.5: `MODSEQ` is followed by either just
    /// `mod-sequence-valzer` or by `entry-name SP entry-type-req SP
    /// mod-sequence-valzer`. The gate only needs to skip those operands so
    /// later search keys continue to be parsed in the right position.
    fn search_criteria_consume_modseq_operands(criteria: &str, bytes: &[u8], pos: &mut usize) {
        let Some(first) = Self::search_criteria_consume_item(criteria, bytes, pos) else {
            return;
        };

        match first {
            SearchCriteriaItem::Bare(value) if value.bytes().all(|b| b.is_ascii_digit()) => {}
            _ => {
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
                let _ = Self::search_criteria_consume_item(criteria, bytes, pos);
            }
        }
    }
}

/// Item from a SEARCH criteria scan.
enum SearchCriteriaItem<'a> {
    Bare(&'a str),
    Quoted,
    Literal,
}

#[cfg(test)]
#[path = "search_validation_tests.rs"]
mod tests;
