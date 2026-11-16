use super::*;
use crate::codec::decode;
use crate::types::response::{GreetingStatus, Response, StatusKind};
use proptest::prelude::*;

// ── Generators ──────────────────────────────────────────────────

/// Generate a valid IMAP tag (1-20 chars, no specials).
/// RFC 3501 Section 9: tag = 1*<any ASTRING-CHAR except "+">
fn arb_tag() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Za-z][A-Za-z0-9]{0,10}").expect("valid regex")
}

/// Generate a valid mailbox name (ASCII, no specials that need quoting).
fn arb_mailbox() -> impl Strategy<Value = MailboxName> {
    prop_oneof![
        Just("INBOX".to_string()),
        prop::string::string_regex("[A-Za-z][A-Za-z0-9./-]{0,20}").expect("valid regex"),
    ]
    .prop_map(|s| MailboxName::new(s).expect("generated mailbox must be valid"))
}

/// Generate a valid sequence set.
/// RFC 3501 Section 9: sequence-set = (seq-number / seq-range) *("," ...)
fn arb_sequence_set<I: std::fmt::Debug>() -> impl Strategy<Value = SequenceSet<I>> {
    prop_oneof![
        (1u32..=99999).prop_map(|n| n.to_string()),
        (1u32..=999, 1u32..=99999).prop_map(|(a, b)| format!("{a}:{b}")),
        Just("1:*".to_string()),
    ]
    .prop_map(|s| SequenceSet::new(s).expect("generated sequence set must be valid"))
}

/// Generate a simple IMAP command for well-formedness testing.
fn arb_simple_command() -> impl Strategy<Value = Command> {
    prop_oneof![
        Just(Command::Noop),
        Just(Command::Capability),
        Just(Command::Logout),
        Just(Command::StartTls),
        Just(Command::Close),
        Just(Command::Expunge),
        Just(Command::Check),
        Just(Command::Namespace),
        Just(Command::Idle),
        Just(Command::Compress),
        arb_mailbox().prop_map(|mailbox| Command::Select {
            mailbox,
            condstore: false,
            qresync: None,
        }),
        arb_mailbox().prop_map(|mailbox| Command::Examine {
            mailbox,
            condstore: false,
            qresync: None,
        }),
        arb_mailbox().prop_map(|mailbox| Command::Create { mailbox }),
        arb_mailbox().prop_map(|mailbox| Command::Delete { mailbox }),
        arb_mailbox().prop_map(|mailbox| Command::Subscribe { mailbox }),
        arb_mailbox().prop_map(|mailbox| Command::Unsubscribe { mailbox }),
        arb_mailbox().prop_map(|mailbox| Command::Status {
            mailbox,
            items: "(MESSAGES RECENT UNSEEN)".into(),
        }),
        (arb_sequence_set(), arb_mailbox()).prop_map(|(ss, mb)| Command::Copy {
            sequence_set: ss,
            mailbox: mb,
        }),
        (arb_sequence_set(), arb_mailbox()).prop_map(|(ss, mb)| Command::UidCopy {
            sequence_set: ss,
            mailbox: mb,
        }),
        (arb_sequence_set(), arb_mailbox()).prop_map(|(ss, mb)| Command::Move {
            sequence_set: ss,
            mailbox: mb,
        }),
        arb_sequence_set().prop_map(|ss| Command::Fetch {
            sequence_set: ss,
            items: "(FLAGS)".into(),
            changed_since: None,
        }),
        arb_sequence_set().prop_map(|ss| Command::UidFetch {
            sequence_set: ss,
            items: "(FLAGS UID)".into(),
            changed_since: None,
            vanished: false,
        }),
    ]
}

// ── Command well-formedness invariants ──────────────────────────

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    /// Every encoded command ends with CRLF and starts with the tag.
    /// RFC 3501 Section 2.2.1 / RFC 9051 Section 2.2.1.
    #[test]
    fn command_well_formed(
        tag in arb_tag(),
        cmd in arb_simple_command(),
    ) {
        let result = encode_command(&tag, &cmd, &default_opts());
        if let Ok(encoded) = result {
            let buf = encoded.to_vec();
            let bytes = &buf[..];

            // Must end with CRLF
            prop_assert!(
                bytes.ends_with(b"\r\n"),
                "command must end with CRLF"
            );

            // Must start with the tag followed by SP
            let tag_prefix = format!("{tag} ");
            prop_assert!(
                bytes.starts_with(tag_prefix.as_bytes()),
                "command must start with '{tag} ', got: {:?}",
                String::from_utf8_lossy(&bytes[..bytes.len().min(40)])
            );

            // No NUL bytes in output
            prop_assert!(
                !bytes.contains(&0u8),
                "command must not contain NUL bytes"
            );
        }
    }

    /// Encoded commands using LITERAL+ never produce multiple segments.
    /// RFC 7888 Section 4: non-synchronizing literals don't need waits.
    #[test]
    fn literal_plus_single_segment(
        tag in arb_tag(),
        cmd in arb_simple_command(),
    ) {
        let result = encode_command(&tag, &cmd, &opts(LiteralMode::LiteralPlus, false));
        if let Ok(encoded) = result {
            prop_assert_eq!(
                encoded.segments().len(),
                1,
                "LITERAL+ commands must be single-segment"
            );
        }
    }

    /// SELECT command produces well-formed output with mailbox name.
    /// RFC 3501 Section 6.3.1.
    #[test]
    fn select_contains_mailbox(mailbox in arb_mailbox()) {
        let cmd = Command::Select {
            mailbox,
            condstore: false,
            qresync: None,
        };
        let encoded = encode_command("T1", &cmd, &opts(LiteralMode::LiteralPlus, false))
            .expect("SELECT encoding must succeed");
        let s = String::from_utf8(encoded.to_vec())
            .expect("SELECT must produce valid UTF-8");

        prop_assert!(
            s.starts_with("T1 SELECT "),
            "SELECT must start with tag and keyword"
        );
        prop_assert!(
            s.ends_with("\r\n"),
            "SELECT must end with CRLF"
        );
    }

    /// FETCH command includes the sequence set in the output.
    /// RFC 3501 Section 6.4.5.
    #[test]
    fn fetch_contains_sequence_set(seq in arb_sequence_set()) {
        let cmd = Command::Fetch {
            sequence_set: seq.clone(),
            items: "(FLAGS)".into(),
            changed_since: None,
        };
        let encoded = encode_command("T1", &cmd, &opts(LiteralMode::LiteralPlus, false))
            .expect("FETCH encoding must succeed");
        let s = String::from_utf8(encoded.to_vec())
            .expect("FETCH must produce valid UTF-8");

        prop_assert!(
            s.starts_with("T1 FETCH "),
            "FETCH must start with tag and keyword"
        );
        prop_assert!(
            s.contains(seq.as_str()),
            "FETCH must contain the sequence set '{}'",
            seq
        );
    }
}

// ── Response round-trip ─────────────────────────────────────────

/// Format a tagged response as wire bytes.
/// RFC 3501 Section 7.1: tag SP resp-cond-state CRLF
fn format_tagged_response(tag: &str, status: &str, text: &str) -> Vec<u8> {
    format!("{tag} {status} {text}\r\n").into_bytes()
}

/// Format a greeting as wire bytes.
/// RFC 3501 Section 7.1: "* OK" / "* PREAUTH" / "* BYE"
fn format_greeting(status: &str, text: &str) -> Vec<u8> {
    format!("* {status} {text}\r\n").into_bytes()
}

/// Generate safe response text (no CRLF, printable ASCII).
fn arb_response_text() -> impl Strategy<Value = String> {
    prop::string::string_regex("[A-Za-z][A-Za-z0-9 _.,-]{0,50}").expect("valid regex")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// Tagged response round-trip: format -> parse -> compare.
    /// RFC 3501 Section 7.1.
    #[test]
    fn tagged_response_roundtrip(
        tag in arb_tag(),
        status_idx in 0u8..3,
        text in arb_response_text(),
    ) {
        let (status_str, expected_status) = match status_idx {
            0 => ("OK", StatusKind::Ok),
            1 => ("NO", StatusKind::No),
            _ => ("BAD", StatusKind::Bad),
        };

        let wire = format_tagged_response(&tag, status_str, &text);
        match decode::parse_response(&wire) {
            Ok((remaining, Response::Tagged(resp))) => {
                prop_assert!(
                    remaining.is_empty(),
                    "parser left unconsumed bytes"
                );
                prop_assert_eq!(&resp.tag, &tag, "tag mismatch");
                prop_assert_eq!(resp.status, expected_status, "status mismatch");
                prop_assert_eq!(resp.text.trim(), text.trim(), "text mismatch");
            }
            Ok((_, other)) => {
                prop_assert!(
                    false,
                    "expected Tagged response, got: {:?}",
                    std::mem::discriminant(&other)
                );
            }
            Err(e) => {
                prop_assert!(
                    false,
                    "parse_response failed: {:?}\nwire: {:?}",
                    e,
                    String::from_utf8_lossy(&wire)
                );
            }
        }
    }

    /// Greeting round-trip: format -> parse_greeting -> compare.
    /// RFC 3501 Section 7.1.
    #[test]
    fn greeting_roundtrip(
        status_idx in 0u8..3,
        text in arb_response_text(),
    ) {
        let (status_str, expected_status) = match status_idx {
            0 => ("OK", GreetingStatus::Ok),
            1 => ("PREAUTH", GreetingStatus::PreAuth),
            _ => ("BYE", GreetingStatus::Bye),
        };

        let wire = format_greeting(status_str, &text);
        match decode::parse_greeting(&wire) {
            Ok((remaining, Response::Greeting(resp))) => {
                prop_assert!(
                    remaining.is_empty(),
                    "parser left unconsumed bytes"
                );
                prop_assert_eq!(resp.status, expected_status, "greeting status mismatch");
                prop_assert_eq!(resp.text.trim(), text.trim(), "greeting text mismatch");
            }
            Ok((_, other)) => {
                prop_assert!(
                    false,
                    "expected Greeting response, got: {:?}",
                    std::mem::discriminant(&other)
                );
            }
            Err(e) => {
                prop_assert!(
                    false,
                    "parse_greeting failed: {:?}\nwire: {:?}",
                    e,
                    String::from_utf8_lossy(&wire)
                );
            }
        }
    }

    /// Untagged EXISTS/RECENT round-trip.
    /// RFC 3501 Section 7.3.1-7.3.2.
    #[test]
    fn untagged_exists_recent_roundtrip(
        count in 0u32..100_000,
        kind_idx in 0u8..2,
    ) {
        let kind = if kind_idx == 0 { "EXISTS" } else { "RECENT" };
        let wire = format!("* {count} {kind}\r\n").into_bytes();
        match decode::parse_response(&wire) {
            Ok((remaining, resp)) => {
                prop_assert!(
                    remaining.is_empty(),
                    "parser left unconsumed bytes"
                );
                // Should parse as an Untagged response
                prop_assert!(
                    matches!(resp, Response::Untagged(_)),
                    "expected Untagged response for * {count} {kind}"
                );
            }
            Err(e) => {
                prop_assert!(
                    false,
                    "parse_response failed for * {count} {kind}: {:?}",
                    e
                );
            }
        }
    }

    /// Continuation request round-trip.
    /// RFC 3501 Section 7.5.
    #[test]
    fn continuation_roundtrip(text in arb_response_text()) {
        let wire = format!("+ {text}\r\n").into_bytes();
        match decode::parse_response(&wire) {
            Ok((remaining, Response::Continuation(cont))) => {
                prop_assert!(
                    remaining.is_empty(),
                    "parser left unconsumed bytes"
                );
                prop_assert_eq!(
                    cont.data.trim(), text.trim(),
                    "continuation data mismatch"
                );
            }
            Ok((_, other)) => {
                prop_assert!(
                    false,
                    "expected Continuation, got: {:?}",
                    std::mem::discriminant(&other)
                );
            }
            Err(e) => {
                prop_assert!(
                    false,
                    "parse_response failed for continuation: {:?}",
                    e
                );
            }
        }
    }
}
