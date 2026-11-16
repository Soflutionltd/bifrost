use super::*;

// --- QUOTA (RFC 2087) encoder tests ---

#[test]
fn encode_get_quota() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetQuota {
        root: String::new(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 GETQUOTA \"\"\r\n");
}

#[test]
fn encode_get_quota_named_root() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetQuota {
        root: "user.alice".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 GETQUOTA \"user.alice\"\r\n");
}

#[test]
fn encode_get_quota_root() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetQuotaRoot {
        mailbox: MailboxName::new("INBOX").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 GETQUOTAROOT \"INBOX\"\r\n");
}

#[test]
fn encode_get_quota_root_folder() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetQuotaRoot {
        mailbox: MailboxName::new("INBOX.Drafts").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 GETQUOTAROOT \"INBOX.Drafts\"\r\n");
}

// --- ACL (RFC 4314) encoder tests ---

#[test]
fn encode_setacl() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetAcl {
        mailbox: MailboxName::new("INBOX").unwrap(),
        identifier: "fred".into(),
        rights: "lrswipcda".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETACL \"INBOX\" \"fred\" \"lrswipcda\"\r\n"
    );
}

#[test]
fn encode_deleteacl() {
    let mut buf = BytesMut::new();
    let cmd = Command::DeleteAcl {
        mailbox: MailboxName::new("INBOX").unwrap(),
        identifier: "fred".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 DELETEACL \"INBOX\" \"fred\"\r\n");
}

#[test]
fn encode_getacl() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetAcl {
        mailbox: MailboxName::new("INBOX").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 GETACL \"INBOX\"\r\n");
}

#[test]
fn encode_listrights() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListRights {
        mailbox: MailboxName::new("INBOX").unwrap(),
        identifier: "fred".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LISTRIGHTS \"INBOX\" \"fred\"\r\n");
}

#[test]
fn encode_myrights() {
    let mut buf = BytesMut::new();
    let cmd = Command::MyRights {
        mailbox: MailboxName::new("INBOX").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 MYRIGHTS \"INBOX\"\r\n");
}

#[test]
fn encode_setacl_with_special_chars() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetAcl {
        mailbox: MailboxName::new("Shared Folders").unwrap(),
        identifier: "user@example.com".into(),
        rights: "+lrs".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETACL \"Shared Folders\" \"user@example.com\" \"+lrs\"\r\n"
    );
}

#[test]
fn encode_getacl_nested_folder() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetAcl {
        mailbox: MailboxName::new("INBOX.Sent Items").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 GETACL \"INBOX.Sent Items\"\r\n");
}

/// Very long quoted string is still properly quoted if all chars are quotable.
#[test]
fn encode_long_quotable_string() {
    let mut buf = BytesMut::new();
    let data = "a".repeat(10_000);
    push_quoted_or_literal(&mut buf, data.as_bytes(), LiteralMode::Synchronizing);
    // Should still be quoted since all chars are safe
    assert!(buf.starts_with(b"\""));
    assert!(buf.ends_with(b"\""));
    assert_eq!(buf.len(), 10_002); // 10000 + 2 quotes
}

// L15: SETQUOTA command (RFC 2087 Section 4.1)

#[test]
fn encode_setquota_single_resource() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetQuota {
        root: String::new(),
        resources: vec![("STORAGE".into(), 51200)],
    };
    encode_command_to_buf(&mut buf, "A014", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A014 SETQUOTA \"\" (STORAGE 51200)\r\n");
}

#[test]
fn encode_setquota_multiple_resources() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetQuota {
        root: "user.alice".into(),
        resources: vec![("STORAGE".into(), 102_400), ("MESSAGE".into(), 5000)],
    };
    encode_command_to_buf(&mut buf, "A015", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A015 SETQUOTA \"user.alice\" (STORAGE 102400 MESSAGE 5000)\r\n"
    );
}

#[test]
fn encode_setquota_empty_resources() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetQuota {
        root: String::new(),
        resources: vec![],
    };
    encode_command_to_buf(&mut buf, "A016", &cmd, &default_opts()).unwrap();
    // Empty resource list: SETQUOTA "" ()
    assert_eq!(&buf[..], b"A016 SETQUOTA \"\" ()\r\n");
}

/// SETQUOTA command encoding.
#[test]
fn audit_finding8_setquota_command_encodes_correctly() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetQuota {
        root: String::new(),
        resources: vec![("STORAGE".into(), 51200)],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 SETQUOTA \"\" (STORAGE 51200)\r\n",
        "SETQUOTA command should encode correctly (RFC 2087 Section 4.1)"
    );
}

/// SETQUOTA with limit at `u32::MAX` boundary must succeed (RFC 2087 Section 4.1).
///
/// RFC 2087 Section 4.1 uses `number` for resource limits, which per
/// RFC 3501 Section 9 is a 32-bit unsigned integer (0..4294967295).
/// A limit of exactly `u32::MAX` must encode successfully.
#[test]
fn encode_setquota_limit_at_u32_max_succeeds() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetQuota {
        root: String::new(),
        resources: vec![("STORAGE".into(), u64::from(u32::MAX))],
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "SETQUOTA with limit = u32::MAX must succeed (RFC 2087 Section 4.1)"
    );
    assert_eq!(&buf[..], b"A001 SETQUOTA \"\" (STORAGE 4294967295)\r\n");
}

/// SETQUOTA with limit exceeding `u32::MAX` must return an error
/// (RFC 2087 Section 4.1).
///
/// RFC 2087 Section 4.1: `setquota_resource = atom SP number`.
/// RFC 3501 Section 9: `number = 1*DIGIT`  -  constrained to u32.
/// Values > 4294967295 are invalid on the wire.
#[test]
fn encode_setquota_limit_exceeding_u32_max_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetQuota {
        root: String::new(),
        resources: vec![("STORAGE".into(), u64::from(u32::MAX) + 1)],
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SETQUOTA with limit > u32::MAX must fail (RFC 2087 Section 4.1)"
    );
}

/// SETQUOTA resource name containing a space must be rejected.
///
/// RFC 2087 Section 4.1: `setquota_resource = atom SP number`.
/// RFC 3501 Section 9: `atom = 1*ATOM-CHAR`, and SP is an atom-special.
/// A resource name with a space would produce malformed wire format.
#[test]
fn encode_setquota_rejects_resource_name_with_space() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetQuota {
        root: String::new(),
        resources: vec![("BAD RESOURCE".into(), 1024)],
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SETQUOTA resource name with space must be rejected \
         (RFC 2087 Section 4.1: resource is atom; RFC 3501 Section 9: SP is atom-special)"
    );
}

/// SETQUOTA with an empty resource name must be rejected.
///
/// RFC 2087 Section 4.1: `setquota_resource = atom SP number`.
/// RFC 3501 Section 9: `atom = 1*ATOM-CHAR`  -  at least one character is required.
#[test]
fn encode_setquota_rejects_empty_resource_name() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetQuota {
        root: String::new(),
        resources: vec![(String::new(), 1024)],
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SETQUOTA with empty resource name must be rejected \
         (RFC 2087 Section 4.1: resource is atom; RFC 3501 Section 9: atom = 1*ATOM-CHAR)"
    );
}

// ── Pre-refactor lock-down: DELETEACL / LISTRIGHTS edge cases ──

/// RFC 4314 Section 3.2: DELETEACL with a mailbox containing spaces.
#[test]
fn encode_deleteacl_mailbox_with_spaces() {
    let mut buf = BytesMut::new();
    let cmd = Command::DeleteAcl {
        mailbox: MailboxName::new("Shared Folders").unwrap(),
        identifier: "user@example.com".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 DELETEACL \"Shared Folders\" \"user@example.com\"\r\n"
    );
}

/// RFC 4314 Section 3.2: DELETEACL with non-ASCII mailbox
/// forces literal encoding.
#[test]
fn encode_deleteacl_non_ascii_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::DeleteAcl {
        mailbox: MailboxName::new("café").unwrap(),
        identifier: "fred".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // "café" -> modified UTF-7 "caf&AOk-" (ASCII), quoted on the wire.
    assert_eq!(&buf[..], b"A001 DELETEACL \"caf&AOk-\" \"fred\"\r\n");
}

/// RFC 4314 Section 3.4: LISTRIGHTS with a mailbox containing spaces.
#[test]
fn encode_listrights_mailbox_with_spaces() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListRights {
        mailbox: MailboxName::new("Shared Folders").unwrap(),
        identifier: "user@example.com".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LISTRIGHTS \"Shared Folders\" \"user@example.com\"\r\n"
    );
}

/// RFC 4314 Section 3.4: LISTRIGHTS with non-ASCII mailbox.
#[test]
fn encode_listrights_non_ascii_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListRights {
        mailbox: MailboxName::new("café").unwrap(),
        identifier: "fred".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // "café" -> modified UTF-7 "caf&AOk-" (ASCII), quoted on the wire.
    assert_eq!(&buf[..], b"A001 LISTRIGHTS \"caf&AOk-\" \"fred\"\r\n");
}

// ── Pre-refactor lock-down: GETQUOTA / GETQUOTAROOT non-ASCII ──

/// RFC 2087 Section 4.2: GETQUOTA with non-ASCII root.
#[test]
fn encode_get_quota_non_ascii_root() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetQuota {
        root: "café".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 GETQUOTA {5}\r\ncaf\xc3\xa9\r\n");
}

/// RFC 2087 Section 4.3: GETQUOTAROOT with non-ASCII mailbox.
#[test]
fn encode_get_quota_root_non_ascii() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetQuotaRoot {
        mailbox: MailboxName::new("café").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // "café" -> modified UTF-7 "caf&AOk-" (ASCII), quoted on the wire.
    assert_eq!(&buf[..], b"A001 GETQUOTAROOT \"caf&AOk-\"\r\n");
}
