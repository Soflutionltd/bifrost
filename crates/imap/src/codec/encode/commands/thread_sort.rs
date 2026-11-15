//! THREAD and SORT command encoder (RFC 5256).

use super::{
    CommandWriter, validate_atom, validate_non_empty_search_criteria, validate_sort_thread_charset,
};

/// Encode a THREAD or SORT command (RFC 5256 Sections 2-3).
///
/// Shared by THREAD, UID THREAD, SORT, and UID SORT.
/// THREAD format: `<cmd> <algorithm> <charset> <criteria>`.
/// SORT format:   `<cmd> (<algorithm>) <charset> <criteria>`.
/// `parenthesize_algo` controls whether the algorithm is wrapped in `()`.
pub(in crate::codec::encode) fn encode_thread_or_sort_cmd(
    w: &mut CommandWriter,
    tag: &str,
    cmd: &str,
    algorithm: &str,
    charset: &str,
    criteria: &crate::types::SearchCriteria,
    parenthesize_algo: bool,
) -> Result<(), crate::Error> {
    if parenthesize_algo {
        // RFC 5256 Section 2: sort-criteria = "(" sort-key *(SP sort-key) ")"
        // Each sort-key is an atom (RFC 3501 Section 9).
        for key in algorithm.split(' ') {
            validate_atom(key, "SORT sort-key")?;
        }
    } else {
        // RFC 5256 Section 3: thread-alg = atom (RFC 3501 Section 9).
        validate_atom(algorithm, "THREAD algorithm")?;
    }
    // RFC 5256 Section 5: charset = atom / quoted.
    validate_sort_thread_charset(charset)?;
    criteria.validate_raw(super::super::LiteralMode::LiteralPlus)?;
    validate_non_empty_search_criteria(criteria, cmd)?;

    w.raw(tag.as_bytes());
    w.raw(b" ");
    w.raw(cmd.as_bytes());
    if parenthesize_algo {
        // RFC 5256 Section 2: sort-criteria = "(" sort-key *(SP sort-key) ")"
        w.raw(b" (");
        w.raw(algorithm.as_bytes());
        w.raw(b") ");
    } else {
        // RFC 5256 Section 3: thread-alg is unparenthesized
        w.raw(b" ");
        w.raw(algorithm.as_bytes());
        w.raw(b" ");
    }
    w.raw(charset.as_bytes());
    w.raw(b" ");
    super::super::write_search(w, criteria, crate::codec::utf7::WireMode::ModifiedUtf7);
    w.raw(b"\r\n");
    Ok(())
}
