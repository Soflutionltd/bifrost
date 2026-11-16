use super::*;

#[test]
fn encode_enable_uses_imap4rev2_base_capability() {
    let opts = EncodeOptions {
        utf8_mode: crate::types::wire::WireMode::ModifiedUtf7,
        literal_mode: LiteralMode::Synchronizing,
        session: crate::types::profile::SessionCapabilities::new(
            vec![Capability::Imap4Rev2],
            Vec::new(),
        ),
    };
    let cmd = Command::Enable {
        capabilities: vec![Capability::QResync],
    };
    let encoded = encode_command("A001", &cmd, &opts).unwrap();
    assert_eq!(&encoded.to_vec()[..], b"A001 ENABLE QRESYNC\r\n");
}

/// The encoder's command admission is a view over the single capability
/// authority: for every capability in every rev2-ACTIVE state it must answer
/// exactly what the oracle does, including QRESYNC implying CONDSTORE.
#[test]
fn encode_options_capability_answers_match_the_authority() {
    use crate::types::profile::capability_matrix::{
        connection_states, every_capability, expected_usable,
    };

    for (capabilities, enabled) in connection_states() {
        let opts = EncodeOptions {
            utf8_mode: crate::types::wire::WireMode::ModifiedUtf7,
            literal_mode: LiteralMode::Synchronizing,
            session: crate::types::profile::SessionCapabilities::new(
                capabilities.clone(),
                enabled.clone(),
            ),
        };
        for capability in every_capability() {
            assert_eq!(
                opts.has_capability(&capability),
                expected_usable(&capabilities, &enabled, &capability),
                "{capability:?} with capabilities={capabilities:?}, enabled={enabled:?}"
            );
        }
        assert_eq!(
            opts.has_condstore(),
            expected_usable(&capabilities, &enabled, &Capability::Condstore)
        );
    }

    let qresync_only = EncodeOptions {
        utf8_mode: crate::types::wire::WireMode::ModifiedUtf7,
        literal_mode: LiteralMode::Synchronizing,
        session: crate::types::profile::SessionCapabilities::new(
            vec![Capability::QResync],
            Vec::new(),
        ),
    };
    assert!(qresync_only.has_condstore());
}

#[test]
fn encode_simple_command() {
    let mut buf = BytesMut::new();
    push_simple(&mut buf, "A001", "NOOP");
    assert_eq!(&buf[..], b"A001 NOOP\r\n");
}

#[test]
fn encode_login_simple() {
    let mut buf = BytesMut::new();
    push_login(
        &mut buf,
        "A001",
        "user",
        "pass",
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    assert_eq!(&buf[..], b"A001 LOGIN \"user\" \"pass\"\r\n");
}

#[test]
fn encode_login_special_chars() {
    let mut buf = BytesMut::new();
    push_login(
        &mut buf,
        "A001",
        "user",
        r#"p"a\ss"#,
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    assert_eq!(&buf[..], b"A001 LOGIN \"user\" \"p\\\"a\\\\ss\"\r\n");
}

#[test]
fn encode_authenticate_with_sasl_ir() {
    let mut buf = BytesMut::new();
    let cmd = Command::Authenticate {
        mechanism: "XOAUTH2".into(),
        initial_response: Some("dXNlcj1hQGIuY29tAWF1dGg9QmVhcmVyIHRva2VuAQE=".into()),
    };
    encode_command_to_buf(&mut buf, "A006", &cmd, &default_opts()).unwrap();
    let expected = b"A006 AUTHENTICATE XOAUTH2 dXNlcj1hQGIuY29tAWF1dGg9QmVhcmVyIHRva2VuAQE=\r\n";
    assert_eq!(&buf[..], &expected[..]);
}

#[test]
fn encode_authenticate_without_sasl_ir() {
    let mut buf = BytesMut::new();
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 AUTHENTICATE PLAIN\r\n");
}

/// RFC 3501 Section 6.2.2: auth-type = atom. Mechanism names containing
/// non-atom characters (spaces, parens, etc.) must be rejected.
#[test]
fn encode_authenticate_rejects_invalid_mechanism() {
    let mut buf = BytesMut::new();
    let cmd = Command::Authenticate {
        mechanism: "PLAIN LOGIN".into(), // space is not an atom-char
        initial_response: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "Mechanism with space must be rejected (RFC 3501 Section 6.2.2: auth-type = atom)"
    );

    let mut buf = BytesMut::new();
    let cmd = Command::Authenticate {
        mechanism: String::new().into(),
        initial_response: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "Empty mechanism must be rejected"
    );
}

#[test]
fn encode_starttls() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::StartTls, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STARTTLS\r\n");
}

#[test]
fn encode_logout() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Logout, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LOGOUT\r\n");
}

#[test]
fn encode_capability() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Capability, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CAPABILITY\r\n");
}

#[test]
fn encode_noop() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Noop, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 NOOP\r\n");
}

#[test]
fn encode_close() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Close, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CLOSE\r\n");
}

#[test]
fn encode_idle() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Idle, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 IDLE\r\n");
}

#[test]
fn encode_id() {
    let mut buf = BytesMut::new();
    let cmd = Command::Id(vec![
        ("name".into(), Some("myapp".into())),
        ("version".into(), Some("1.0".into())),
    ]);
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 ID (\"name\" \"myapp\" \"version\" \"1.0\")\r\n"
    );
}

#[test]
fn encode_check() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Check, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CHECK\r\n");
}

// --- COMPRESS (RFC 4978) encoder test ---

#[test]
fn encode_compress() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Compress, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 COMPRESS DEFLATE\r\n");
}

// ===== Audit: #6 ID empty params -> NIL =====
// RFC 2971 Section 3.1: the request is either a parenthesized list
// or NIL. An empty parenthesized list "()" is not valid.

/// Empty ID params should produce `ID NIL` per RFC 2971 Section 3.1.
#[test]
fn regression_encode_id_empty_params() {
    let mut buf = BytesMut::new();
    let cmd = Command::Id(vec![]);
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 ID NIL\r\n",
        "empty ID params should produce ID NIL per RFC 2971 Section 3.1"
    );
}

// ===== RFC 2971 Section 3.3 ID command limits =====
// RFC 2971 Section 3.3: "Implementations MUST NOT send more than
// 30 field-value pairs." / "Field strings MUST NOT be longer than
// 30 octets." / "Value strings MUST NOT be longer than 1024 octets."

/// RFC 2971 Section 3.3: more than 30 field-value pairs MUST be rejected.
#[test]
fn encode_id_rejects_more_than_30_pairs() {
    let mut buf = BytesMut::new();
    let params: Vec<(String, Option<String>)> = (0..31)
        .map(|i| (format!("k{i}"), Some(format!("v{i}"))))
        .collect();
    let cmd = Command::Id(params);
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "ID with 31 pairs must be rejected per RFC 2971 Section 3.3"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("RFC 2971"),
        "error message should cite RFC 2971, got: {msg}"
    );
}

/// RFC 2971 Section 3.3: field names are case-insensitive and
/// implementations MUST NOT send the same field name more than once.
#[test]
fn encode_id_rejects_duplicate_field_names_case_insensitive() {
    let mut buf = BytesMut::new();
    let cmd = Command::Id(vec![
        ("name".into(), Some("Bifrost".into())),
        ("NAME".into(), Some("duplicate".into())),
    ]);
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "duplicate ID field names must be rejected per RFC 2971 Section 3.3"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("same field name") || msg.contains("duplicate"),
        "error should mention the duplicate ID field name, got: {msg}"
    );
}

/// RFC 2971 Section 3.3: field strings longer than 30 octets MUST be rejected.
#[test]
fn encode_id_rejects_key_longer_than_30_octets() {
    let mut buf = BytesMut::new();
    let long_key = "a".repeat(31);
    let cmd = Command::Id(vec![(long_key, Some("value".into()))]);
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "ID with key > 30 octets must be rejected per RFC 2971 Section 3.3"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("RFC 2971"),
        "error message should cite RFC 2971, got: {msg}"
    );
}

/// RFC 2971 Section 3.3: value strings longer than 1024 octets MUST be rejected.
#[test]
fn encode_id_rejects_value_longer_than_1024_octets() {
    let mut buf = BytesMut::new();
    let long_value = "v".repeat(1025);
    let cmd = Command::Id(vec![("key".into(), Some(long_value))]);
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "ID with value > 1024 octets must be rejected per RFC 2971 Section 3.3"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("RFC 2971"),
        "error message should cite RFC 2971, got: {msg}"
    );
}

/// RFC 2971 Section 3.3: exactly 30 pairs with 30-byte keys and 1024-byte
/// values must succeed (boundary check).
#[test]
fn encode_id_accepts_exactly_at_limits() {
    let mut buf = BytesMut::new();
    let value = "v".repeat(1024); // exactly 1024 octets
    let params: Vec<(String, Option<String>)> = (0..30)
        .map(|i| (format!("{i:02}{}", "k".repeat(28)), Some(value.clone())))
        .collect();
    let cmd = Command::Id(params);
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "ID with exactly 30 pairs, unique 30-byte keys, and 1024-byte values \
         should succeed per RFC 2971 Section 3.3, got: {:?}",
        result.unwrap_err()
    );
}

/// L10: AUTHENTICATE with empty initial response must send `=`.
/// RFC 4959 Section 3.
#[test]
fn audit_l10_authenticate_empty_initial_response() {
    let mut buf = BytesMut::new();
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: Some(String::new().into()),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains(" =\r\n"),
        "empty initial response must be '=': '{output}'"
    );
}

/// ID command values can be NIL per RFC 2971 Section 3.1.
///
/// RFC 2971 Section 3.1: `id_params_list ::= "(" #(string SPACE nstring) ")" / nil`
/// Values are `nstring`  -  `None` must encode as `NIL` on the wire.
#[test]
fn encode_id_nil_value() {
    let mut buf = BytesMut::new();
    let cmd = Command::Id(vec![
        ("name".into(), Some("myapp".into())),
        ("version".into(), None),
    ]);
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 ID (\"name\" \"myapp\" \"version\" NIL)\r\n",
        "None value must encode as NIL per RFC 2971 Section 3.1"
    );
}

// --- AUTHENTICATE SASL-IR validation (RFC 4959 Section 3, RFC 3501 Section 2.2) ---

/// CRLF in SASL-IR must be rejected to prevent command injection (RFC 3501 Section 2.2).
#[test]
fn encode_authenticate_rejects_crlf_in_initial_response() {
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: Some("dXNlcg==\r\nA002 LOGOUT".into()),
    };
    let result = encode_command("A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "CRLF in SASL-IR initial response must be rejected"
    );
}

/// RFC 4959 Section 3: initial-response = base64 / "=".
/// Non-base64 characters must be rejected.
#[test]
fn encode_authenticate_rejects_non_base64_initial_response() {
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: Some("not base64! @#".into()),
    };
    let result = encode_command("A001", &cmd, &default_opts());
    assert!(result.is_err(), "Non-base64 SASL-IR must be rejected");
}

/// Valid base64 SASL-IR must be accepted (RFC 4959 Section 3).
#[test]
fn encode_authenticate_accepts_valid_base64_initial_response() {
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: Some("dXNlcgBwYXNz".into()),
    };
    let result = encode_command("A001", &cmd, &default_opts());
    assert!(result.is_ok());
}

/// RFC 4959 Section 3: `initial-response = base64 / "="`.
/// A string that uses only base64 alphabet characters but has invalid
/// base64 structure must still be rejected.
#[test]
fn encode_authenticate_rejects_malformed_base64_initial_response() {
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: Some("==".into()),
    };
    let result = encode_command("A001", &cmd, &default_opts());
    assert!(result.is_err(), "Malformed base64 SASL-IR must be rejected");
}

/// RFC 4959 Section 3: empty initial response sent as "=".
#[test]
fn encode_authenticate_accepts_empty_initial_response_marker() {
    let cmd = Command::Authenticate {
        mechanism: "PLAIN".into(),
        initial_response: Some(String::new().into()),
    };
    let result = encode_command("A001", &cmd, &default_opts());
    assert!(result.is_ok());
    let encoded = result.unwrap();
    let s = String::from_utf8(encoded.to_vec()).unwrap();
    assert!(
        s.contains(" =\r\n"),
        "Empty initial response should encode as '='"
    );
}
