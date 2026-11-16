use super::*;
use std::time::{Duration, Instant};

/// Maximum time any single parse call should take.
/// If it exceeds this, the parser is effectively "stuck".
const MAX_PARSE_TIME: Duration = Duration::from_secs(5);

fn assert_terminates<F: FnOnce()>(name: &str, f: F) {
    let start = Instant::now();
    f();
    let elapsed = start.elapsed();
    assert!(
        elapsed < MAX_PARSE_TIME,
        "{name} took {elapsed:?}, exceeds {MAX_PARSE_TIME:?}  -  parser may be stuck"
    );
}

#[test]
fn parse_response_repeated_bytes_100kb() {
    // 100KB of \xff bytes  -  tests that non-ASCII bytes don't cause
    // excessive backtracking or infinite loops.
    let data = vec![0xFFu8; 100_000];
    assert_terminates("100KB 0xFF", || {
        let _ = parse_response_utf8(&data, crate::types::wire::WireMode::ModifiedUtf7);
    });
}

#[test]
fn parse_response_repeated_star_100kb() {
    // "* " prefix followed by 100KB of data  -  looks like an untagged
    // response but with garbage body.
    let mut data = b"* ".to_vec();
    data.extend(vec![b'A'; 100_000]);
    data.extend(b"\r\n");
    assert_terminates("100KB untagged garbage", || {
        let _ = parse_response_utf8(&data, crate::types::wire::WireMode::ModifiedUtf7);
    });
}

#[test]
fn parse_response_deeply_nested_parens() {
    // 10,000 nested opening parens  -  tests BODYSTRUCTURE depth limits
    let mut data = b"* 1 FETCH (BODYSTRUCTURE ".to_vec();
    for _ in 0..10_000 {
        data.push(b'(');
    }
    data.extend(b")\r\n");
    assert_terminates("10K nested parens", || {
        let _ = parse_response_utf8(&data, crate::types::wire::WireMode::ModifiedUtf7);
    });
}

#[test]
fn parse_response_long_literal_count() {
    // Literal with absurd byte count  -  should not allocate or loop
    let data = b"* 1 FETCH (BODY[] {999999999}\r\n";
    assert_terminates("huge literal count", || {
        let _ = parse_response_utf8(data, crate::types::wire::WireMode::ModifiedUtf7);
    });
}

#[test]
fn parse_greeting_repeated_bytes_100kb() {
    let mut data = b"* OK ".to_vec();
    data.extend(vec![b'X'; 100_000]);
    data.extend(b"\r\n");
    assert_terminates("100KB greeting", || {
        let _ = parse_greeting(&data);
    });
}
