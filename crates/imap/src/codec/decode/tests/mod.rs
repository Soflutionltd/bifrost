#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::similar_names,
    clippy::unreadable_literal,
    clippy::wildcard_in_or_patterns,
    clippy::vec_init_then_push,
    clippy::match_wild_err_arm,
    clippy::same_item_push
)]

use super::*;
use crate::types::validated::MailboxName;

#[cfg(any())]
mod moved_q_encoding_tests;

// ========================================================================
// Property-based invariant tests
// ========================================================================

mod prop_invariants;

/// Tests that parsers terminate in bounded time on adversarial inputs.
/// These catch infinite loops, excessive recursion, and algorithmic
/// complexity attacks that wouldn't be caught by single-call property tests.
mod stuck_tests;

// ========================================================================
// Adversarial input: pathological headers, silent degradation, dropped
// extension data.
// ========================================================================

#[cfg(any())]
mod moved_encoded_word_window_tests;

// ===== skip_tagged_ext_simple terminator matrix (imap-T4) =====
//
// `skip_tagged_ext_simple` has exactly two callers, each with its own
// terminator set: ESEARCH (`parse_untagged_esearch`) passes SP/CR, STATUS
// (`parse_status_items`) passes SP/`)`. The four `tagged-ext-simple` forms
// (NIL, literal, quoted string, atom) interact with the terminator set in
// non-obvious ways, because only the NIL arm boundary-checks against it and
// the atom fallback is defined as "everything that is not a terminator".
// The matrix below pins the whole cross product.

/// The ESEARCH terminator set (RFC 9051 Section 7.3.4): SP or CR.
fn esearch_terminator(b: u8) -> bool {
    b == b' ' || b == b'\r'
}

/// The STATUS terminator set (RFC 9051 Section 7.3.2): SP or `)`.
fn status_terminator(b: u8) -> bool {
    b == b' ' || b == b')'
}

/// Run both caller terminator sets over one input and return the remaining
/// bytes for each, or `None` where the parser rejects the input.
fn skip_ext_both(input: &[u8]) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let esearch = skip_tagged_ext_simple(esearch_terminator)(input)
        .ok()
        .map(|(rest, ())| rest.to_vec());
    let status = skip_tagged_ext_simple(status_terminator)(input)
        .ok()
        .map(|(rest, ())| rest.to_vec());
    (esearch, status)
}

/// `esearch` / `status` give the expected remaining bytes, or `None` where the
/// parser must reject the input.
fn assert_skip_ext(input: &[u8], esearch: Option<&str>, status: Option<&str>) {
    let (got_esearch, got_status) = skip_ext_both(input);
    assert_eq!(
        got_esearch.as_deref(),
        esearch.map(str::as_bytes),
        "ESEARCH terminator set (SP/CR) on {:?}",
        String::from_utf8_lossy(input)
    );
    assert_eq!(
        got_status.as_deref(),
        status.map(str::as_bytes),
        "STATUS terminator set (SP/`)`) on {:?}",
        String::from_utf8_lossy(input)
    );
}

mod bodystructure;

mod envelope_fetch;

mod extensions;

mod flags_caps;

mod mailbox;

mod namespace;

mod primitives;

mod responses;

mod search;
