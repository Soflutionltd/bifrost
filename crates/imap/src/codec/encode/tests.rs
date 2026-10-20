use bytes::BytesMut;

use super::*;
use crate::types::response::Capability;
use crate::types::validated::{MailboxName, SequenceSet};

/// Build an [`EncodeOptions`] with the given literal mode, utf8 flag, and a
/// generous set of capabilities that allows all commands to encode.
fn opts(literal_mode: LiteralMode, utf8_mode: bool) -> EncodeOptions {
    EncodeOptions {
        utf8_mode,
        literal_mode,
        // Include all commonly needed capabilities so existing tests
        // do not fail due to the new prerequisite checks.
        capabilities: vec![
            Capability::Imap4Rev1,
            Capability::StartTls,
            Capability::Idle,
            Capability::Enable,
            Capability::Condstore,
            Capability::QResync,
            Capability::Move,
            Capability::Unselect,
            Capability::Unauthenticate,
            Capability::Namespace,
            Capability::Id,
            Capability::Metadata,
            Capability::CompressDeflate,
            Capability::Quota,
            Capability::QuotaSet,
            Capability::Acl,
            Capability::Notify,
            Capability::UidPlus,
            Capability::CreateSpecialUse,
            Capability::LiteralPlus,
            Capability::LiteralMinus,
            Capability::Sort,
        ],
        enabled: Vec::new(),
    }
}

/// Shorthand: synchronizing literals, no UTF-8.
fn default_opts() -> EncodeOptions {
    opts(LiteralMode::Synchronizing, false)
}

// --- Fragment helpers ---
//
// The encoder pieces write through a `CommandWriter`; these run one piece
// against a fresh writer and append everything it produced, as the server
// would receive it, to `buf`.

/// Each segment of `cmd` as one byte string: what the sender writes between
/// two continuation waits.
fn segment_bytes(cmd: &WireCommand) -> Vec<Vec<u8>> {
    cmd.segments()
        .iter()
        .map(|segment| {
            segment
                .iter()
                .flat_map(|chunk| chunk.iter().copied())
                .collect()
        })
        .collect()
}

/// Run `write` against a writer for `opts` and return its segments.
fn written_segments(opts: &EncodeOptions, write: impl FnOnce(&mut CommandWriter)) -> Vec<Vec<u8>> {
    let mut w = CommandWriter::new(opts);
    write(&mut w);
    segment_bytes(&w.finish())
}

/// `A001 X` followed by each payload as a classic literal, then CRLF: the
/// smallest command that exercises the writer's segmentation.
fn x_command(mode: LiteralMode, literals: &[&[u8]]) -> WireCommand {
    let mut w = CommandWriter::new(&opts(mode, false));
    w.raw(b"A001 X");
    for literal in literals {
        w.raw(b" ");
        w.literal(literal, LiteralForm::Classic);
    }
    w.raw(b"\r\n");
    w.finish()
}

/// Run `write` against a writer for `mode` and return every byte it wrote.
fn fragment(mode: LiteralMode, write: impl FnOnce(&mut CommandWriter)) -> Vec<u8> {
    let mut w = CommandWriter::new(&opts(mode, false));
    write(&mut w);
    w.finish().to_vec()
}

/// As [`fragment`], for a piece that can refuse.
fn try_fragment(
    mode: LiteralMode,
    write: impl FnOnce(&mut CommandWriter) -> Result<(), crate::Error>,
) -> Result<Vec<u8>, crate::Error> {
    let mut w = CommandWriter::new(&opts(mode, false));
    write(&mut w)?;
    Ok(w.finish().to_vec())
}

fn push_quoted_or_literal(buf: &mut BytesMut, data: &[u8], mode: LiteralMode) {
    buf.extend_from_slice(&fragment(mode, |w| w.string(data, false)));
}

fn push_quoted_or_literal_utf8(buf: &mut BytesMut, data: &[u8], utf8: bool, mode: LiteralMode) {
    buf.extend_from_slice(&fragment(mode, |w| w.string(data, utf8)));
}

fn push_metadata_value(buf: &mut BytesMut, data: &[u8], mode: LiteralMode) {
    buf.extend_from_slice(&fragment(mode, |w| encode_metadata_value(w, data)));
}

fn push_simple(buf: &mut BytesMut, tag: &str, command: &str) {
    buf.extend_from_slice(&fragment(LiteralMode::Synchronizing, |w| {
        encode_simple(w, tag, command);
    }));
}

fn push_login(
    buf: &mut BytesMut,
    tag: &str,
    user: &str,
    pass: &str,
    utf8: bool,
    mode: LiteralMode,
) -> Result<(), crate::Error> {
    buf.extend_from_slice(&try_fragment(mode, |w| {
        encode_login(w, tag, user, pass, utf8)
    })?);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_select_or_examine(
    buf: &mut BytesMut,
    tag: &str,
    cmd: &str,
    mailbox: &str,
    condstore: bool,
    qresync: Option<&QresyncParams>,
    utf8: bool,
    mode: LiteralMode,
) -> Result<(), crate::Error> {
    buf.extend_from_slice(&try_fragment(mode, |w| {
        encode_select_or_examine(w, tag, cmd, mailbox, condstore, qresync, utf8)
    })?);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_getmetadata(
    buf: &mut BytesMut,
    tag: &str,
    mailbox: &str,
    entries: &[String],
    max_size: Option<u64>,
    depth: Option<&str>,
    utf8: bool,
    mode: LiteralMode,
) -> Result<(), crate::Error> {
    buf.extend_from_slice(&try_fragment(mode, |w| {
        encode_getmetadata(w, tag, mailbox, entries, max_size, depth, utf8)
    })?);
    Ok(())
}

// --- APPEND / MULTIAPPEND helpers ---
//
// These drive the PRODUCTION entry point, `encode_append`, the same function
// the driver calls at send time. Bodies are `x` filler (or NULs where a test
// needs literal8); assertions render a body chunk as `<len>` so the expected
// wire text stays readable while the framing around it is exact.

/// Options for APPEND tests: `opts` plus MULTIAPPEND. UTF-8 mode implies the
/// RFC 6855 `UTF8=ACCEPT` enable, which is what selects the `UTF8 (` wrapper.
fn append_opts(literal_mode: LiteralMode, utf8: bool) -> EncodeOptions {
    let mut o = opts(literal_mode, utf8);
    o.capabilities.push(Capability::MultiAppend);
    if utf8 {
        o.enabled.push("UTF8=ACCEPT".to_owned());
    }
    o
}

/// Encode APPEND (`multi == false`) or MULTIAPPEND as the driver does: a
/// `Command::Append` through `encode_command`. The messages are cloned into
/// the command, which shares each body's allocation, so `is_body_chunk` still
/// recognises them in the output.
fn append_command(
    tag: &str,
    mailbox: &str,
    messages: &[crate::types::AppendMessage],
    multi: bool,
    opts: &EncodeOptions,
) -> Result<WireCommand, crate::Error> {
    encode_command(
        tag,
        &Command::Append {
            mailbox: mailbox.into(),
            messages: messages.to_vec(),
            multi,
        },
        opts,
    )
}

/// A message with `len` bytes of `x` filler.
fn append_msg(
    flags: &[crate::types::Flag],
    date: Option<&str>,
    len: usize,
) -> crate::types::AppendMessage {
    let mut msg = crate::types::AppendMessage::new(vec![b'x'; len]);
    msg.flags = flags.to_vec();
    msg.date = date.map(str::to_owned);
    msg
}

/// A message whose body is `len` NUL octets (forces `literal8`).
fn append_nul_msg(len: usize) -> crate::types::AppendMessage {
    crate::types::AppendMessage::new(vec![0u8; len])
}

/// Whether `chunk` is exactly the allocation of one of `messages`' bodies.
fn is_body_chunk(chunk: &bytes::Bytes, messages: &[crate::types::AppendMessage]) -> bool {
    messages
        .iter()
        .any(|m| chunk.as_ptr() == m.data.as_ptr() && chunk.len() == m.data.len())
}

/// Render each segment as text, with a body chunk shown as `<len>`.
fn append_segments(cmd: &WireCommand, messages: &[crate::types::AppendMessage]) -> Vec<String> {
    cmd.segments()
        .iter()
        .map(|segment| {
            segment
                .iter()
                .map(|chunk| {
                    if is_body_chunk(chunk, messages) {
                        format!("<{}>", chunk.len())
                    } else {
                        String::from_utf8_lossy(chunk).into_owned()
                    }
                })
                .collect::<String>()
        })
        .collect()
}

/// Encode a plain APPEND carrying `date`, for the date-validation tests.
fn append_with_date(date: &str) -> Result<WireCommand, crate::Error> {
    let msgs = [append_msg(&[], Some(date), 100)];
    append_command(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    )
}

/// Encode APPEND (`multi == false`) or MULTIAPPEND and render the whole wire
/// text, with bodies shown as `<len>`.
fn append_wire(
    tag: &str,
    mailbox: &str,
    messages: &[crate::types::AppendMessage],
    multi: bool,
    opts: &EncodeOptions,
) -> String {
    let cmd = append_command(tag, mailbox, messages, multi, opts).unwrap();
    append_segments(&cmd, messages).concat()
}

#[test]
fn encode_enable_uses_imap4rev2_base_capability() {
    let opts = EncodeOptions {
        utf8_mode: false,
        literal_mode: LiteralMode::Synchronizing,
        capabilities: vec![Capability::Imap4Rev2],
        enabled: Vec::new(),
    };
    let cmd = Command::Enable {
        capabilities: vec!["QRESYNC".to_owned()],
    };
    let encoded = encode_command("A001", &cmd, &opts).unwrap();
    assert_eq!(&encoded.to_vec()[..], b"A001 ENABLE QRESYNC\r\n");
}

#[test]
fn encode_options_treats_list_status_as_imap4rev2_base_capability() {
    let opts = EncodeOptions {
        utf8_mode: false,
        literal_mode: LiteralMode::Synchronizing,
        capabilities: vec![Capability::Imap4Rev2],
        enabled: Vec::new(),
    };

    assert!(opts.has_capability(&Capability::ListStatus));
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
            utf8_mode: false,
            literal_mode: LiteralMode::Synchronizing,
            capabilities: capabilities.clone(),
            enabled: enabled.clone(),
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
        utf8_mode: false,
        literal_mode: LiteralMode::Synchronizing,
        capabilities: vec![Capability::QResync],
        enabled: Vec::new(),
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
fn encode_quoted_string() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"hello world", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"hello world\"");
}

#[test]
fn encode_literal_for_binary() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"line1\r\nline2", LiteralMode::Synchronizing);
    // Should use literal form because of CRLF.
    assert_eq!(&buf[..], b"{12}\r\nline1\r\nline2");
}

#[test]
fn encode_select() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A002", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A002 SELECT \"INBOX\"\r\n");
}

#[test]
fn encode_select_qresync() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 67890,
            mod_seq: 12345,
            known_uids: None,
            seq_match_data: None,
        }),
    };
    encode_command_to_buf(&mut buf, "A005", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A005 SELECT \"INBOX\" (QRESYNC (67890 12345))\r\n"
    );
}

#[test]
fn encode_select_qresync_with_known_uids() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 67890,
            mod_seq: 12345,
            known_uids: Some("1:500".into()),
            seq_match_data: None,
        }),
    };
    encode_command_to_buf(&mut buf, "A006", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A006 SELECT \"INBOX\" (QRESYNC (67890 12345 1:500))\r\n"
    );
}

#[test]
fn encode_examine_qresync() {
    let mut buf = BytesMut::new();
    let cmd = Command::Examine {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 100,
            mod_seq: 50,
            known_uids: None,
            seq_match_data: None,
        }),
    };
    encode_command_to_buf(&mut buf, "A007", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A007 EXAMINE \"INBOX\" (QRESYNC (100 50))\r\n");
}

/// RFC 3501 Section 9: uidvalidity = nz-number, so 0 is invalid.
/// RFC 7162 Section 3.2.5.2: QRESYNC takes uidvalidity.
#[test]
fn encode_select_qresync_rejects_zero_uid_validity() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 0,
            mod_seq: 1,
            known_uids: None,
            seq_match_data: None,
        }),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "QRESYNC uid_validity=0 must be rejected per RFC 3501 Section 9 (nz-number)"
    );
}

/// RFC 3501 Section 9: uidvalidity = nz-number, so 0 is invalid.
/// RFC 7162 Section 3.2.5.2: QRESYNC takes uidvalidity.
#[test]
fn encode_examine_qresync_rejects_zero_uid_validity() {
    let cmd = Command::Examine {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 0,
            mod_seq: 1,
            known_uids: None,
            seq_match_data: None,
        }),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "QRESYNC uid_validity=0 must be rejected per RFC 3501 Section 9 (nz-number)"
    );
}

#[test]
fn encode_uid_fetch() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(UID FLAGS ENVELOPE)".into(),
        changed_since: None,
        vanished: false,
    };
    encode_command_to_buf(&mut buf, "A003", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A003 UID FETCH 1:* (UID FLAGS ENVELOPE)\r\n");
}

#[test]
fn encode_store_add_flags() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("1:3").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen, crate::types::Flag::Flagged],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A004", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A004 UID STORE 1:3 +FLAGS (\\Seen \\Flagged)\r\n"
    );
}

#[test]
fn encode_store_with_condstore() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("5").unwrap(),
        operation: crate::types::StoreOperation::Remove,
        flags: vec![crate::types::Flag::Deleted],
        unchanged_since: Some(12345),
    };
    encode_command_to_buf(&mut buf, "A005", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A005 UID STORE 5 (UNCHANGEDSINCE 12345) -FLAGS (\\Deleted)\r\n"
    );
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
        mechanism: String::new(),
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
fn encode_expunge() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Expunge, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 EXPUNGE\r\n");
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
fn encode_examine() {
    let mut buf = BytesMut::new();
    let cmd = Command::Examine {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 EXAMINE \"INBOX\"\r\n");
}

#[test]
fn encode_create() {
    let mut buf = BytesMut::new();
    let cmd = Command::Create {
        mailbox: MailboxName::new("Archive").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CREATE \"Archive\"\r\n");
}

#[test]
fn encode_create_special_use_single() {
    let mut buf = BytesMut::new();
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("Sent").unwrap(),
        special_use: vec![crate::types::MailboxAttribute::Sent],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CREATE \"Sent\" (USE (\\Sent))\r\n");
}

#[test]
fn encode_create_special_use_multiple() {
    let mut buf = BytesMut::new();
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("Important Sent").unwrap(),
        special_use: vec![
            crate::types::MailboxAttribute::Sent,
            crate::types::MailboxAttribute::Important,
        ],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 CREATE \"Important Sent\" (USE (\\Sent \\Important))\r\n"
    );
}

#[test]
fn encode_create_special_use_special_char_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("My Drafts").unwrap(),
        special_use: vec![crate::types::MailboxAttribute::Drafts],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CREATE \"My Drafts\" (USE (\\Drafts))\r\n");
}

#[test]
fn encode_create_special_use_empty() {
    // RFC 6154 Section 6 ABNF: `"USE" SP "(" [use-attr *(SP use-attr)] ")"`
    // Brackets mean the use-attr list is optional.
    let mut buf = BytesMut::new();
    let cmd = Command::CreateSpecialUse {
        mailbox: MailboxName::new("Test").unwrap(),
        special_use: vec![],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CREATE \"Test\" (USE ())\r\n");
}

#[test]
fn encode_delete() {
    let mut buf = BytesMut::new();
    let cmd = Command::Delete {
        mailbox: MailboxName::new("OldMail").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 DELETE \"OldMail\"\r\n");
}

#[test]
fn encode_rename() {
    let mut buf = BytesMut::new();
    let cmd = Command::Rename {
        mailbox: MailboxName::new("OldName").unwrap(),
        new_name: MailboxName::new("NewName").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 RENAME \"OldName\" \"NewName\"\r\n");
}

#[test]
fn encode_list() {
    let mut buf = BytesMut::new();
    let cmd = Command::List {
        reference: String::new().into(),
        pattern: "*".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LIST \"\" \"*\"\r\n");
}

#[test]
fn encode_list_extended_multiple_patterns_with_return_options() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: vec!["SUBSCRIBED".into(), "RECURSIVEMATCH".into()],
        reference: String::new().into(),
        patterns: vec!["*".into(), "%".into()],
        return_options: vec!["CHILDREN".into(), "STATUS (MESSAGES UNSEEN)".into()],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LIST (SUBSCRIBED RECURSIVEMATCH) \"\" (\"*\" \"%\") RETURN (CHILDREN STATUS (MESSAGES UNSEEN))\r\n"
    );
}

#[test]
fn encode_list_extended_rejects_empty_pattern_list() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: Vec::new(),
        reference: String::new().into(),
        patterns: Vec::new(),
        return_options: Vec::new(),
    };

    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        matches!(result, Err(crate::Error::InvalidInput(ref msg)) if msg.contains("RFC 5258 Section 3")),
        "empty LIST-EXTENDED patterns must be rejected per RFC 5258 Section 3 / RFC 9051 Section 6.3.9: {result:?}"
    );
}

/// RFC 5258 Section 3 / RFC 9051 Section 6.3.9: option tokens are
/// separated by protocol spaces in the command grammar, so leading and
/// trailing caller whitespace must not leak into the wire encoding.
#[test]
fn encode_list_extended_trims_outer_option_whitespace() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: vec![" SUBSCRIBED ".into(), "\tRECURSIVEMATCH\t".into()],
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: vec![" CHILDREN ".into(), " STATUS (MESSAGES) ".into()],
    };

    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LIST (SUBSCRIBED RECURSIVEMATCH) \"\" \"*\" RETURN (CHILDREN STATUS (MESSAGES))\r\n"
    );
}

/// RFC 5258 Section 3 / RFC 9051 Section 6.3.9: `RECURSIVEMATCH` is a
/// modifying selection option and MUST NOT be the only selection option
/// (or only with `REMOTE`).
#[test]
fn encode_list_extended_rejects_recursivematch_without_base_option() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: vec!["RECURSIVEMATCH".into()],
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: Vec::new(),
    };

    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        matches!(result, Err(crate::Error::InvalidInput(ref msg)) if msg.contains("RECURSIVEMATCH")),
        "LIST-EXTENDED must reject bare RECURSIVEMATCH per RFC 5258 Section 3 / RFC 9051 Section 6.3.9: {result:?}"
    );
}

/// RFC 5819 Section 4 / RFC 9051 Section 7: the reserved LIST-STATUS
/// return option is `STATUS (<items>)`, not a bare `STATUS` atom.
#[test]
fn encode_list_extended_rejects_bare_status_return_option() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: vec!["SUBSCRIBED".into()],
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: vec!["STATUS".into()],
    };

    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        matches!(result, Err(crate::Error::InvalidInput(ref msg)) if msg.contains("STATUS (") || msg.contains("STATUS (<items>)")),
        "LIST-EXTENDED must reject bare STATUS return option per RFC 5819 Section 4 / RFC 9051 Section 7: {result:?}"
    );
}

#[test]
fn encode_status() {
    let mut buf = BytesMut::new();
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "(MESSAGES UNSEEN)".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STATUS \"INBOX\" (MESSAGES UNSEEN)\r\n");
}

#[test]
fn encode_uid_search() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidSearch {
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID SEARCH ALL\r\n");
}

#[test]
fn encode_uid_move() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidMove {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("Trash").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID MOVE 1:5 \"Trash\"\r\n");
}

#[test]
fn encode_uid_copy() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidCopy {
        sequence_set: SequenceSet::new("10:20").unwrap(),
        mailbox: MailboxName::new("Archive").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID COPY 10:20 \"Archive\"\r\n");
}

#[test]
fn encode_uid_expunge() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidExpunge {
        sequence_set: SequenceSet::new("1:3").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID EXPUNGE 1:3\r\n");
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
fn encode_store_replace_flags() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("42").unwrap(),
        operation: crate::types::StoreOperation::Replace,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID STORE 42 FLAGS (\\Seen)\r\n");
}

/// STORE +FLAGS.SILENT suppresses implicit FETCH (RFC 3501 Section 6.4.6).
#[test]
fn encode_uid_store_add_silent() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("1:3").unwrap(),
        operation: crate::types::StoreOperation::AddSilent,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID STORE 1:3 +FLAGS.SILENT (\\Seen)\r\n");
}

/// STORE -FLAGS.SILENT (RFC 3501 Section 6.4.6).
#[test]
fn encode_uid_store_remove_silent() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("5").unwrap(),
        operation: crate::types::StoreOperation::RemoveSilent,
        flags: vec![crate::types::Flag::Deleted],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID STORE 5 -FLAGS.SILENT (\\Deleted)\r\n");
}

/// STORE FLAGS.SILENT (RFC 3501 Section 6.4.6).
#[test]
fn encode_store_replace_silent() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("10").unwrap(),
        operation: crate::types::StoreOperation::ReplaceSilent,
        flags: vec![crate::types::Flag::Flagged],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STORE 10 FLAGS.SILENT (\\Flagged)\r\n");
}

/// RFC 6851 Section 3.3: The MOVE fallback emulation sequence uses
/// `UID STORE +FLAGS.SILENT (\Deleted)`  -  the `.SILENT` modifier is
/// required to suppress implicit FETCH responses (RFC 3501 Section 6.4.6).
/// This test documents the wire format that the MOVE fallback path in
/// `ImapConnection::uid_move_messages` must produce.
#[test]
fn encode_uid_store_move_fallback_uses_silent() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::AddSilent,
        flags: vec![crate::types::Flag::Deleted],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 UID STORE 1:5 +FLAGS.SILENT (\\Deleted)\r\n",
        "MOVE fallback must use +FLAGS.SILENT per RFC 6851 Section 3.3"
    );
}

#[test]
fn encode_quoted_with_nul_strips_nul_and_quotes() {
    // RFC 3501 Section 9: NUL (%x00) is stripped; remaining "hasnul" is quotable.
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"has\0nul", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"hasnul\"");
}

#[test]
fn encode_non_ascii_falls_back_to_literal() {
    // Per RFC 3501 Section 9, quoted-string characters must be in %x01-7F
    // (all ASCII) minus CR, LF, and NUL. Non-ASCII bytes (>0x7F) must use literal form.
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, "café".as_bytes(), LiteralMode::Synchronizing);
    // "café" is 5 bytes in UTF-8: 63 61 66 c3 a9
    assert_eq!(&buf[..], b"{5}\r\ncaf\xc3\xa9");
}

#[test]
fn encode_mailbox_with_special_chars() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new(r#"folder"name"#).unwrap(),
        condstore: false,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SELECT \"folder\\\"name\"\r\n");
}

#[test]
fn encode_subscribe() {
    let mut buf = BytesMut::new();
    let cmd = Command::Subscribe {
        mailbox: MailboxName::new("INBOX.Sent").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SUBSCRIBE \"INBOX.Sent\"\r\n");
}

#[test]
fn encode_unsubscribe() {
    let mut buf = BytesMut::new();
    let cmd = Command::Unsubscribe {
        mailbox: MailboxName::new("INBOX.Old").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UNSUBSCRIBE \"INBOX.Old\"\r\n");
}

#[test]
fn encode_lsub() {
    let mut buf = BytesMut::new();
    let cmd = Command::Lsub {
        reference: String::new().into(),
        pattern: "*".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LSUB \"\" \"*\"\r\n");
}

#[test]
fn encode_search() {
    let mut buf = BytesMut::new();
    let cmd = Command::Search {
        criteria: "UNSEEN".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH UNSEEN\r\n");
}

/// RFC 3501 Sections 4.3 and 6.4.4: SEARCH keys such as BODY and TEXT
/// take `astring`, and `astring` may be a literal. The encoder must
/// therefore allow a literal-bearing criteria string.
#[test]
fn encode_search_allows_literal_criteria() {
    let mut buf = BytesMut::new();
    let cmd = Command::Search {
        criteria: "BODY {12}\r\nhello MODSEQ".into(),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "SEARCH criteria literals must be accepted per RFC 3501 Sections 4.3 and 6.4.4: {result:?}"
    );
    assert_eq!(&buf[..], b"A001 SEARCH BODY {12}\r\nhello MODSEQ\r\n");
}

#[test]
fn encode_fetch() {
    let mut buf = BytesMut::new();
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 FETCH 1:* (FLAGS)\r\n");
}

#[test]
fn encode_store() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:3").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STORE 1:3 +FLAGS (\\Seen)\r\n");
}

#[test]
fn encode_copy() {
    let mut buf = BytesMut::new();
    let cmd = Command::Copy {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("Archive").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 COPY 1:5 \"Archive\"\r\n");
}

#[test]
fn encode_namespace() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Namespace, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 NAMESPACE\r\n");
}

#[test]
fn encode_check() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Check, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 CHECK\r\n");
}

#[test]
fn encode_search_save() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchSave {
        criteria: "UNSEEN".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH RETURN (SAVE) UNSEEN\r\n");
}

#[test]
fn encode_uid_search_save() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidSearchSave {
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID SEARCH RETURN (SAVE) ALL\r\n");
}

#[test]
fn encode_search_return_min_max_count() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        criteria: "UNSEEN".into(),
        return_opts: vec!["MIN".into(), "MAX".into(), "COUNT".into()],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH RETURN (MIN MAX COUNT) UNSEEN\r\n");
}

#[test]
fn encode_uid_search_return_all() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidSearchReturn {
        criteria: "ALL".into(),
        return_opts: vec!["ALL".into(), "COUNT".into()],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID SEARCH RETURN (ALL COUNT) ALL\r\n");
}

#[test]
fn encode_search_return_empty_opts() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        criteria: "FLAGGED".into(),
        return_opts: vec![],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH RETURN () FLAGGED\r\n");
}

/// RFC 4731 Section 3.2: RETURN options are space-delimited command
/// tokens, so the encoder must not preserve caller-supplied outer
/// whitespace around each option atom.
#[test]
fn encode_search_return_trims_outer_option_whitespace() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        criteria: "UNSEEN".into(),
        return_opts: vec![" MIN ".into(), "\tCOUNT\t".into()],
    };

    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SEARCH RETURN (MIN COUNT) UNSEEN\r\n");
}

/// RFC 4731 Section 3.2 defines each RETURN element as a
/// `search-return-opt` token. An element containing embedded
/// whitespace would expand into multiple wire options and must be
/// rejected instead of being serialized verbatim.
#[test]
fn encode_search_return_rejects_embedded_whitespace_in_option() {
    let mut buf = BytesMut::new();
    let cmd = Command::SearchReturn {
        criteria: "UNSEEN".into(),
        return_opts: vec!["MIN COUNT".into()],
    };

    let err = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts())
        .expect_err("SEARCH RETURN option with embedded whitespace must be rejected");

    assert!(
        matches!(err, crate::Error::InvalidInput(ref message) if message.contains("SEARCH RETURN option")),
        "expected Protocol error mentioning SEARCH RETURN option, got {err:?}"
    );
}

#[test]
fn encode_list_status() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: "MESSAGES UNSEEN".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LIST \"\" \"*\" RETURN (STATUS (MESSAGES UNSEEN))\r\n"
    );
}

#[test]
fn encode_list_status_with_reference() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListStatus {
        reference: "INBOX".into(),
        pattern: "%".into(),
        status_items: "MESSAGES RECENT UNSEEN".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 LIST \"INBOX\" \"%\" RETURN (STATUS (MESSAGES RECENT UNSEEN))\r\n"
    );
}

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

// --- GETMETADATA / SETMETADATA (RFC 5464) encoder tests ---

#[test]
fn encode_getmetadata_single_entry() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into()],
        max_size: None,
        depth: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 GETMETADATA \"INBOX\" \"/private/comment\"\r\n"
    );
}

#[test]
fn encode_getmetadata_multiple_entries() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into(), "/shared/vendor/foo".into()],
        max_size: None,
        depth: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 GETMETADATA \"INBOX\" (\"/private/comment\" \"/shared/vendor/foo\")\r\n"
    );
}

#[test]
fn encode_setmetadata_with_values() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/comment".into(), Some(b"my comment".to_vec()))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/comment\" \"my comment\")\r\n"
    );
}

#[test]
fn encode_setmetadata_with_nil_delete() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/comment".into(), None)],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/comment\" NIL)\r\n"
    );
}

#[test]
fn encode_setmetadata_multiple_entries() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![
            ("/private/comment".into(), Some(b"hello".to_vec())),
            ("/shared/vendor/x".into(), None),
        ],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/comment\" \"hello\" \"/shared/vendor/x\" NIL)\r\n"
    );
}

/// RFC 5464 Section 3.2: entry names MUST NOT contain `*`, `%`,
/// consecutive slashes, a trailing slash, non-ASCII, or octets in the
/// range 0x00..=0x19. Invalid names should be rejected client-side.
#[test]
fn encode_setmetadata_invalid_entry_names_return_error() {
    for entry in [
        "comment",
        "/public/comment",
        "/shared/%bad",
        "/shared/*bad",
        "/shared//bad",
        "/shared/bad/",
        "/shared/r\u{00E9}sum\u{00E9}",
        "/shared/\u{0019}control",
    ] {
        let mut buf = BytesMut::new();
        let cmd = Command::SetMetadata {
            mailbox: MailboxName::new("INBOX").unwrap(),
            entries: vec![(entry.into(), Some(b"value".to_vec()))],
        };
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "SETMETADATA entry name {entry:?} must be rejected per RFC 5464 Section 3.2"
        );
    }
}

/// SETMETADATA values containing NUL bytes must be preserved,
/// and binary values must use literal8 syntax (`~{N}\r\n<data>`), not
/// standard literal (`{N}\r\n<data>`).
///
/// RFC 5464 Section 5: `value = nstring / literal8`
/// RFC 3516: `literal8 = "~{" number "}" CRLF *OCTET`  -  *OCTET includes NUL (%x00).
#[test]
fn encode_setmetadata_literal8_preserves_nul_bytes() {
    let mut buf = BytesMut::new();
    // Binary value with NUL bytes  -  must not be stripped.
    let value = b"\x00\x01\x02\x03".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = &buf[..];

    // NUL bytes must be preserved in the output (not stripped).
    assert!(
        output.windows(4).any(|w| w == b"\x00\x01\x02\x03"),
        "NUL bytes must be preserved in SETMETADATA values per RFC 3516 literal8 *OCTET"
    );

    // Must use literal8 syntax ~{N}\r\n, not standard literal {N}\r\n.
    assert!(
        output.windows(5).any(|w| w == b"~{4}\r"),
        "Binary SETMETADATA value must use literal8 ~{{N}} syntax per RFC 5464 Section 5 / RFC 3516, got: {:?}",
        String::from_utf8_lossy(output)
    );
}

/// SETMETADATA values that are ASCII-safe should still use
/// quoted string form (nstring), not literal8.
///
/// RFC 5464 Section 5: `value = nstring / literal8`  -  nstring is preferred
/// when the data is quotable.
#[test]
fn encode_setmetadata_ascii_value_uses_quoted_form() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/comment".into(), Some(b"hello".to_vec()))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // ASCII-safe value should still be quoted, not literal8.
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/comment\" \"hello\")\r\n"
    );
}

/// SETMETADATA values with high bytes (>0x7F) must use classic IMAP
/// literal syntax, not literal8.
///
/// RFC 5464 Section 5: `value = nstring / literal8`
/// RFC 3501 Section 9 / RFC 9051 Section 9: `nstring = string / nil`
/// and `string` includes `literal = "{" number "}" CRLF *CHAR8`,
/// with `CHAR8 = %x01-ff`. High bytes are therefore valid in ordinary
/// literals; only NUL (`%x00`) requires literal8.
#[test]
fn encode_setmetadata_high_bytes_use_standard_literal() {
    let mut buf = BytesMut::new();
    let value = b"\x80\x81\xff".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = &buf[..];

    // Must use classic literal syntax, not literal8.
    assert!(
        output.windows(4).any(|w| w == b"{3}\r"),
        "High-byte SETMETADATA value must use classic literal {{N}} syntax per RFC 3501 Section 9 / RFC 9051 Section 9, got: {:?}",
        String::from_utf8_lossy(output)
    );
    assert!(
        !output.windows(5).any(|w| w == b"~{3}\r"),
        "High-byte SETMETADATA value must not use literal8 when no NUL octet is present, got: {:?}",
        String::from_utf8_lossy(output)
    );

    // High bytes must be preserved.
    assert!(
        output.windows(3).any(|w| w == b"\x80\x81\xff"),
        "High bytes must be preserved in SETMETADATA literal values"
    );
}

// --- encode_metadata_value direct tests ---

/// ASCII data with backslash and double-quote must be escaped in quoted form.
/// RFC 3501 Section 9: quoted-specials = `"` / `\` are escaped with `\`.
#[test]
fn metadata_value_escapes_backslash_and_quote() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"a\\b\"c", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"a\\\\b\\\"c\"");
}

/// ASCII data without special chars uses quoted form.
#[test]
fn metadata_value_ascii_uses_quoted() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"hello", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"hello\"");
}

/// Data with CR/LF uses classic literal syntax because IMAP literals
/// allow all CHAR8 octets except NUL.
#[test]
fn metadata_value_crlf_uses_standard_literal() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"line1\r\nline2", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"{12}\r\nline1\r\nline2");
}

/// Data with NUL byte uses literal8 (NUL is preserved, not stripped).
/// RFC 3516: literal8 uses `*OCTET` (%x00-FF).
#[test]
fn metadata_value_nul_uses_literal8() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"\x00data", LiteralMode::Synchronizing);
    // NUL preserved; literal8 form.
    assert_eq!(&buf[..], b"~{5}\r\n\x00data");
}

/// Empty data uses empty quoted string.
#[test]
fn metadata_value_empty() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"\"");
}

/// Data with high bytes (>0x7F) uses classic literal syntax because
/// `literal` permits `*CHAR8` and `CHAR8 = %x01-ff`.
#[test]
fn metadata_value_high_bytes_uses_standard_literal() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"\x80\xff", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"{2}\r\n\x80\xff");
}

/// RFC 9051 Section 9: DEL (0x7F) is excluded from quoted CHAR, so it
/// must fall back to literal form. Classic literals still allow DEL via
/// `CHAR8 = %x01-ff`, so literal8 is not required unless a NUL is present.
#[test]
fn metadata_value_del_byte_uses_standard_literal() {
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"hello\x7Fworld", LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"{"),
        "DEL byte (0x7F) must trigger classic literal encoding per RFC 3501/9051 literal CHAR8 rules, got: {:?}",
        std::str::from_utf8(&buf)
    );
}

/// ASCII control characters (0x01-0x1F) must trigger classic literal
/// encoding, not quoted form. `literal8` is still reserved for NUL.
#[test]
fn metadata_value_control_char_uses_standard_literal() {
    // TAB (0x09) is a control character that must NOT be quoted.
    let mut buf = BytesMut::new();
    push_metadata_value(&mut buf, b"hello\tworld", LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"{"),
        "TAB (0x09) must trigger classic literal encoding, got: {:?}",
        std::str::from_utf8(&buf)
    );

    // Printable ASCII (0x20-0x7E) should use quoted form.
    buf.clear();
    push_metadata_value(&mut buf, b"hello world", LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"\""),
        "Printable ASCII should use quoted encoding, got: {:?}",
        std::str::from_utf8(&buf)
    );

    // NUL (0x00) must trigger literal8.
    buf.clear();
    push_metadata_value(&mut buf, b"\x00", LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"~{"),
        "NUL (0x00) must trigger literal8 encoding, got: {:?}",
        std::str::from_utf8(&buf)
    );
}

/// Non-NUL METADATA literals should honor LITERAL+ / LITERAL- when the
/// caller requests non-synchronizing classic literals.
#[test]
fn encode_setmetadata_high_bytes_use_literal_plus_when_available() {
    let mut buf = BytesMut::new();
    let value = b"\x80\x81\xff".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(
        &mut buf,
        "A001",
        &cmd,
        &opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    assert_eq!(
        &buf[..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/binary\" {3+}\r\n\x80\x81\xff)\r\n"
    );
}

// --- THREAD (RFC 5256) encoder tests ---

#[test]
fn encode_thread_command() {
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 THREAD REFERENCES UTF-8 ALL\r\n");
}

/// RFC 5256 Sections 3 and 5: THREAD reuses IMAP SEARCH criteria, so its
/// criteria argument must permit IMAP literals too.
#[test]
fn encode_thread_allows_literal_criteria() {
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "BODY {12}\r\nhello MODSEQ".into(),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "THREAD criteria literals must be accepted per RFC 3501 Section 4.3 and RFC 5256 Section 3: {result:?}"
    );
    assert_eq!(
        &buf[..],
        b"A001 THREAD REFERENCES UTF-8 BODY {12}\r\nhello MODSEQ\r\n"
    );
}

#[test]
fn encode_uid_thread_command() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidThread {
        algorithm: "ORDEREDSUBJECT".into(),
        charset: "US-ASCII".into(),
        criteria: "SINCE 1-Jan-2025".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 UID THREAD ORDEREDSUBJECT US-ASCII SINCE 1-Jan-2025\r\n"
    );
}

// --- SORT (RFC 5256) encoder tests ---

#[test]
fn encode_sort_command() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SORT (DATE) UTF-8 ALL\r\n");
}

#[test]
fn encode_uid_sort_command() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidSort {
        sort_criteria: "SUBJECT".into(),
        charset: "US-ASCII".into(),
        criteria: "SINCE 1-Jan-2025".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 UID SORT (SUBJECT) US-ASCII SINCE 1-Jan-2025\r\n"
    );
}

/// RFC 5256 Section 5: `charset = atom / quoted`.
/// Quoted charsets are legal for THREAD and must not be rejected.
#[test]
fn encode_thread_allows_quoted_charset() {
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        algorithm: "REFERENCES".into(),
        charset: "\"UTF-8\"".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts())
        .expect("THREAD must accept quoted charset per RFC 5256 Section 5");
    assert_eq!(&buf[..], b"A001 THREAD REFERENCES \"UTF-8\" ALL\r\n");
}

/// RFC 5256 Section 5: `charset = atom / quoted`.
/// Quoted charsets are legal for SORT and must not be rejected.
#[test]
fn encode_sort_allows_quoted_charset() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        sort_criteria: "DATE".into(),
        charset: "\"UTF-8\"".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts())
        .expect("SORT must accept quoted charset per RFC 5256 Section 5");
    assert_eq!(&buf[..], b"A001 SORT (DATE) \"UTF-8\" ALL\r\n");
}

#[test]
fn sort_command_uses_sort_criteria_field() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A1", &cmd, &default_opts()).unwrap();
    assert_eq!(
        std::str::from_utf8(&buf).unwrap(),
        "A1 SORT (DATE) UTF-8 ALL\r\n",
        "SORT must encode sort_criteria in parentheses (RFC 5256 Section 2)"
    );
}

// --- THREAD/SORT atom validation (RFC 5256 Sections 2-3, RFC 3501 Section 9) ---

#[test]
fn encode_thread_rejects_invalid_algorithm() {
    // RFC 5256 Section 3: thread-alg is an atom (RFC 3501 Section 9).
    // An algorithm containing spaces is not a valid atom and must be rejected.
    let mut buf = BytesMut::new();
    let cmd = Command::Thread {
        algorithm: "ORDEREDSUBJECT INVALID".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "THREAD algorithm with spaces must be rejected (RFC 3501 Section 9: atom)"
    );
}

#[test]
fn encode_sort_rejects_invalid_charset() {
    // RFC 5256 Section 5: charset = atom / quoted. Parentheses are
    // atom-specials, so an unquoted charset containing them must be rejected.
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        sort_criteria: "DATE".into(),
        charset: "UTF(8)".into(),
        criteria: "ALL".into(),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SORT charset with parens must be rejected (RFC 3501 Section 9: atom)"
    );
}

// --- COMPRESS (RFC 4978) encoder test ---

#[test]
fn encode_compress() {
    let mut buf = BytesMut::new();
    encode_command_to_buf(&mut buf, "A001", &Command::Compress, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 COMPRESS DEFLATE\r\n");
}

// --- MULTIAPPEND (RFC 3502) encoder tests ---

#[test]
fn encode_multi_append_first_message_no_flags() {
    // First message with no flags and no date (RFC 3502 Section 3).
    let msgs = [append_msg(&[], None, 100)];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" {100}\r\n<100>\r\n"
    );
}

#[test]
fn encode_multi_append_first_message_with_flags() {
    // First message with flags (RFC 3502 Section 3 / RFC 3501 Section 6.3.11).
    let msgs = [append_msg(
        &[crate::types::Flag::Seen, crate::types::Flag::Flagged],
        None,
        200,
    )];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" (\\Seen \\Flagged) {200}\r\n<200>\r\n"
    );
}

#[test]
fn encode_multi_append_first_message_with_date() {
    // First message with internal date (RFC 3502 Section 3 / RFC 3501 Section 6.3.11).
    let msgs = [append_msg(&[], Some("17-Jul-1996 02:44:25 -0700"), 50)];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" \"17-Jul-1996 02:44:25 -0700\" {50}\r\n<50>\r\n"
    );
}

#[test]
fn encode_multi_append_first_message_with_flags_and_date() {
    // First message with both flags and date (RFC 3502 Section 3).
    let msgs = [append_msg(
        &[crate::types::Flag::Seen],
        Some(" 1-Jan-2024 00:00:00 +0000"),
        300,
    )];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" (\\Seen) \" 1-Jan-2024 00:00:00 +0000\" {300}\r\n<300>\r\n"
    );
}

#[test]
fn encode_multi_append_subsequent_message() {
    // Subsequent message  -  no tag/APPEND/mailbox prefix, and it follows the
    // previous body directly (RFC 3502 Section 3).
    let msgs = [
        append_msg(&[], None, 10),
        append_msg(&[crate::types::Flag::Draft], None, 75),
    ];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" {10}\r\n<10> (\\Draft) {75}\r\n<75>\r\n"
    );
}

#[test]
fn encode_multi_append_with_literal_plus() {
    // LITERAL+ (RFC 7888)  -  non-synchronizing literal.
    let msgs = [append_msg(&[], None, 42)];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::LiteralPlus, false)
        ),
        "A001 APPEND \"INBOX\" {42+}\r\n<42>\r\n"
    );
}

#[test]
fn encode_multi_append_subsequent_with_literal_plus() {
    // Subsequent message with LITERAL+ (RFC 7888 / RFC 3502 Section 3).
    let msgs = [append_msg(&[], None, 1), append_msg(&[], None, 99)];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::LiteralPlus, false)
        ),
        "A001 APPEND \"INBOX\" {1+}\r\n<1> {99+}\r\n<99>\r\n"
    );
}

#[test]
fn encode_multi_append_two_messages_mixed_flags() {
    // 2-message MULTIAPPEND: first with flags and date, second with neither
    // (RFC 3502). Synchronizing, so each body is preceded by a boundary.
    let msgs = [
        append_msg(
            &[crate::types::Flag::Seen, crate::types::Flag::Answered],
            Some("15-Mar-2026 10:00:00 +0000"),
            50,
        ),
        append_msg(&[], None, 30),
    ];
    let cmd = append_command(
        "A010",
        "INBOX",
        &msgs,
        true,
        &append_opts(LiteralMode::Synchronizing, false),
    )
    .unwrap();
    assert_eq!(
        append_segments(&cmd, &msgs),
        vec![
            "A010 APPEND \"INBOX\" (\\Seen \\Answered) \"15-Mar-2026 10:00:00 +0000\" {50}\r\n"
                .to_owned(),
            "<50> {30}\r\n".to_owned(),
            "<30>\r\n".to_owned(),
        ],
        "N synchronizing literals must yield N+1 segments"
    );
}

#[test]
fn encode_multi_append_three_messages_literal_plus() {
    // 3-message MULTIAPPEND with LITERAL+ (RFC 3502 / RFC 7888 Section 4).
    let msgs = [
        append_msg(&[crate::types::Flag::Seen], None, 100),
        append_msg(&[], Some(" 1-Jan-2025 00:00:00 +0000"), 200),
        append_msg(&[crate::types::Flag::Flagged], None, 300),
    ];
    let cmd = append_command(
        "A020",
        "Archive",
        &msgs,
        true,
        &append_opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    assert_eq!(
        append_segments(&cmd, &msgs),
        vec![
            "A020 APPEND \"Archive\" (\\Seen) {100+}\r\n<100> \" 1-Jan-2025 00:00:00 +0000\" \
             {200+}\r\n<200> (\\Flagged) {300+}\r\n<300>\r\n"
                .to_owned()
        ],
        "with LITERAL+ nothing waits, so the whole command is one segment"
    );
}

#[test]
fn encode_multi_append_subsequent_no_flags_with_date() {
    // Subsequent message with date but no flags (RFC 3502 Section 3).
    let msgs = [
        append_msg(&[], None, 10),
        append_msg(&[], Some("25-Dec-2025 12:00:00 +0000"), 500),
    ];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" {10}\r\n<10> \"25-Dec-2025 12:00:00 +0000\" {500}\r\n<500>\r\n"
    );
}

#[test]
fn encode_multi_append_special_mailbox() {
    // Mailbox name with special characters uses quoting (RFC 3501 Section 9).
    let msgs = [append_msg(&[], None, 10)];
    assert_eq!(
        append_wire(
            "A001",
            r#"folder"name"#,
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"folder\\\"name\" {10}\r\n<10>\r\n"
    );
}

/// The encoder must not copy a body: the chunk it emits is the caller's own
/// allocation, and both a synchronizing and a non-synchronizing literal keep
/// that property.
#[test]
fn encode_append_bodies_are_carried_by_reference() {
    let msgs = [append_msg(&[], None, 8192), append_msg(&[], None, 4)];
    for mode in [LiteralMode::Synchronizing, LiteralMode::LiteralPlus] {
        let cmd = append_command("A001", "INBOX", &msgs, true, &append_opts(mode, false)).unwrap();
        let carried: Vec<&bytes::Bytes> = cmd
            .segments()
            .iter()
            .flatten()
            .filter(|chunk| is_body_chunk(chunk, &msgs))
            .collect();
        assert_eq!(
            carried.len(),
            2,
            "both bodies present, once each ({mode:?})"
        );
        for (chunk, msg) in carried.iter().zip(&msgs) {
            assert_eq!(chunk.as_ptr(), msg.data.as_ptr(), "no copy ({mode:?})");
        }
    }
}

/// A mailbox that cannot be a quoted string (here, one carrying CRLF) becomes a
/// literal of its own. Under synchronizing literals that is a boundary in the
/// middle of the header, so the message body is the THIRD segment.
///
/// Only in UTF-8 mode: outside it the mailbox is modified UTF-7, which encodes
/// CR and LF into ASCII, so no mailbox name ever needs a literal there. The
/// second half pins that too.
#[test]
fn encode_append_splits_at_a_synchronizing_mailbox_literal() {
    let msgs = [append_msg(&[], None, 6)];
    let cmd = append_command(
        "A001",
        "a\r\nb",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, true),
    )
    .unwrap();
    assert_eq!(
        append_segments(&cmd, &msgs),
        vec![
            "A001 APPEND {4}\r\n".to_owned(),
            "a\r\nb UTF8 (~{6}\r\n".to_owned(),
            "<6>)\r\n".to_owned(),
        ]
    );

    let cmd = append_command(
        "A001",
        "a\r\nb",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    )
    .unwrap();
    assert_eq!(
        append_segments(&cmd, &msgs),
        vec![
            "A001 APPEND \"a&AA0ACg-b\" {6}\r\n".to_owned(),
            "<6>\r\n".to_owned(),
        ],
        "modified UTF-7 turns CRLF into ASCII, so the mailbox stays quoted"
    );
}

/// Non-empty segments: an empty message body is a legal zero-length literal and
/// still gets its own continuation under synchronizing literals.
#[test]
fn encode_append_zero_length_body_is_still_a_segment() {
    let msgs = [crate::types::AppendMessage::new(bytes::Bytes::new())];
    let cmd = append_command(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    )
    .unwrap();
    assert_eq!(cmd.segments().len(), 2);
    assert!(cmd.segments().iter().all(|segment| !segment.is_empty()));
}

// ===== Negative / edge-case encoding tests =====

/// NUL byte in data is stripped per RFC 3501 Section 9 (CHAR8 = %x01-ff).
#[test]
fn encode_data_with_nul_byte_strips_nul() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"hello\x00world", LiteralMode::Synchronizing);
    // NUL is stripped; "helloworld" is quotable ASCII.
    assert_eq!(&buf[..], b"\"helloworld\"");
    assert!(!buf.contains(&0x00), "output must not contain NUL bytes");
}

/// Non-ASCII bytes force literal encoding.
#[test]
fn encode_data_with_non_ascii_uses_literal() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, "café".as_bytes(), LiteralMode::Synchronizing);
    // Non-ASCII (0x80+) is not quotable
    assert!(buf.starts_with(b"{5}\r\n"));
}

/// Empty data encodes as empty quoted string.
#[test]
fn encode_empty_data_quoted() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"", LiteralMode::Synchronizing);
    assert_eq!(&buf[..], b"\"\"");
}

/// Mailbox name with spaces gets properly quoted in SELECT.
#[test]
fn encode_select_mailbox_with_spaces() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("my folder").unwrap(),
        condstore: false,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 SELECT \"my folder\"\r\n");
}

/// A LOGIN credential containing CR/LF cannot escape its literal framing.
///
/// `password` is an `astring`, and RFC 9051 Section 4.3 lets a literal carry
/// any CHAR8. The injection this would have to become is a credential whose
/// bytes leave the literal early and are read as a command line; that is
/// impossible because the emitted octet count is taken from exactly the bytes
/// written, so the CRLF is payload the server consumes inside the literal.
#[test]
fn encode_login_crlf_credential_stays_inside_its_literal() {
    let pass = "pass\r\nword A002 DELETE INBOX\r\n";
    let mut buf = BytesMut::new();
    push_login(
        &mut buf,
        "A001",
        "user",
        pass,
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();

    let marker = format!("{{{}}}\r\n", pass.len());
    let output = String::from_utf8(buf.to_vec()).unwrap();
    assert_eq!(
        output,
        format!("A001 LOGIN \"user\" {marker}{pass}\r\n"),
        "the credential is framed as a counted literal, verbatim"
    );

    // The count is what makes this safe: everything after the marker up to
    // `pass.len()` octets is literal payload, so the embedded `A002 DELETE`
    // line is never at a command boundary.
    let body_start = output.find(&marker).unwrap() + marker.len();
    assert_eq!(&output[body_start..body_start + pass.len()], pass);
    assert_eq!(&output[body_start + pass.len()..], "\r\n");
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

// ===== DEL (0x7F) must trigger literal encoding =====
// RFC 9051 Section 9: CHAR = %x01-7E  -  DEL is excluded in IMAP4rev2.
// Using a literal for DEL is safe for both rev1 and rev2.

#[test]
fn del_byte_triggers_literal_encoding() {
    // RFC 9051 Section 9: CHAR = %x01-7E  -  DEL (0x7F) is excluded.
    // Strings containing DEL must use literal encoding, not quoted strings,
    // to be compatible with both IMAP4rev1 and IMAP4rev2.
    let mut buf = BytesMut::new();
    let data_with_del = b"hello\x7Fworld";
    push_quoted_or_literal(&mut buf, data_with_del, LiteralMode::Synchronizing);
    let result = std::str::from_utf8(&buf).unwrap();
    // Must be a literal (starts with {), not a quoted string (starts with ")
    assert!(
        result.starts_with('{'),
        "DEL byte (0x7F) must trigger literal encoding per RFC 9051 Section 9, got: {result}"
    );
}

// ===== Control chars (%x01-1F except CR/LF) must trigger literal =====
// While RFC 3501 Section 9 technically allows these as CHAR (%x01-7F),
// many servers reject control characters in quoted strings. Using literal
// form is the conservative ("be strict in what you send") approach and is
// safe for both IMAP4rev1 and IMAP4rev2.

#[test]
fn control_char_triggers_literal_encoding() {
    // Control characters (TAB, BEL, ESC, etc.) in data should produce
    // a literal rather than a quoted string for interoperability.
    for &byte in &[0x01u8, 0x07, 0x09, 0x1B, 0x1F] {
        let mut buf = BytesMut::new();
        let data = [
            b'h', b'e', b'l', b'l', b'o', byte, b'w', b'o', b'r', b'l', b'd',
        ];
        push_quoted_or_literal(&mut buf, &data, LiteralMode::Synchronizing);
        let result = std::str::from_utf8(&buf).unwrap();
        assert!(
            result.starts_with('{'),
            "Control char 0x{byte:02X} must trigger literal encoding for interoperability, got: {result}"
        );
    }
}

// ===== non-UID MOVE command (RFC 6851 Section 3) =====
// RFC 6851 Section 5: move = "MOVE" SP sequence-set SP mailbox
// Both UID MOVE and non-UID MOVE must be supported.

/// Non-UID MOVE command encoding (RFC 6851 Section 3).
#[test]
fn encode_move() {
    let mut buf = BytesMut::new();
    let cmd = Command::Move {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("Trash").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 MOVE 1:5 \"Trash\"\r\n");
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

// M11: QRESYNC seq-match-data encoding (RFC 7162 Section 3.2.5.2)
#[test]
fn encode_select_qresync_seq_match_data() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 67890,
            mod_seq: 12345,
            known_uids: Some("1:500".into()),
            seq_match_data: Some(("1:3".into(), "100:102".into())),
        }),
    };
    encode_command_to_buf(&mut buf, "A008", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A008 SELECT \"INBOX\" (QRESYNC (67890 12345 1:500 (1:3 100:102)))\r\n"
    );
}

// M13: CONDSTORE SELECT parameter (RFC 7162 Section 3.1.1)
#[test]
fn encode_select_condstore() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: true,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A009", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A009 SELECT \"INBOX\" (CONDSTORE)\r\n");
}

#[test]
fn encode_examine_condstore() {
    let mut buf = BytesMut::new();
    let cmd = Command::Examine {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: true,
        qresync: None,
    };
    encode_command_to_buf(&mut buf, "A010", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A010 EXAMINE \"INBOX\" (CONDSTORE)\r\n");
}

// L14: FETCH CHANGEDSINCE modifier (RFC 7162 Section 3.1.4.1)

#[test]
fn encode_fetch_changedsince() {
    let mut buf = BytesMut::new();
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(12345),
    };
    encode_command_to_buf(&mut buf, "A011", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A011 FETCH 1:* (FLAGS) (CHANGEDSINCE 12345)\r\n");
}

#[test]
fn encode_fetch_no_changedsince() {
    let mut buf = BytesMut::new();
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:10").unwrap(),
        items: "(UID)".into(),
        changed_since: None,
    };
    encode_command_to_buf(&mut buf, "A012", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A012 FETCH 1:10 (UID)\r\n");
}

#[test]
fn encode_uid_fetch_changedsince() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:500").unwrap(),
        items: "(FLAGS ENVELOPE)".into(),
        changed_since: Some(67890),
        vanished: false,
    };
    encode_command_to_buf(&mut buf, "A013", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A013 UID FETCH 1:500 (FLAGS ENVELOPE) (CHANGEDSINCE 67890)\r\n"
    );
}

// RFC 7162 Section 3.2.6: UID FETCH CHANGEDSINCE VANISHED modifier

#[test]
fn encode_uid_fetch_changedsince_vanished() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(12345),
        vanished: true,
    };
    encode_command_to_buf(&mut buf, "A014", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A014 UID FETCH 1:* (FLAGS) (CHANGEDSINCE 12345 VANISHED)\r\n"
    );
}

#[test]
fn encode_uid_fetch_vanished_without_changedsince_rejected() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: None,
        vanished: true,
    };
    let result = encode_command_to_buf(&mut buf, "A015", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "VANISHED without CHANGEDSINCE must be rejected per RFC 7162 Section 3.2.6"
    );
}

#[test]
fn encode_uid_fetch_changedsince_without_vanished_unchanged() {
    // Existing behavior: CHANGEDSINCE alone, vanished=false
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:500").unwrap(),
        items: "(FLAGS ENVELOPE)".into(),
        changed_since: Some(67890),
        vanished: false,
    };
    encode_command_to_buf(&mut buf, "A016", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A016 UID FETCH 1:500 (FLAGS ENVELOPE) (CHANGEDSINCE 67890)\r\n"
    );
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

// ===== Spec audit: failing tests for known deviations =====

/// M4: NUL bytes in literal encoder.
///
/// RFC 3501 Section 9 defines CHAR8 = %x01-ff, explicitly excluding NUL (%x00).
/// Rule (3): "The ASCII NUL... MUST NOT be used at any time."
/// The encoder strips NUL bytes before encoding.
#[test]
fn spec_audit_m4_nul_bytes_in_literal() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"has\0nul", LiteralMode::Synchronizing);
    // RFC 3501 Section 9: NUL (%x00) MUST NOT appear in the output.
    // The output must not contain a NUL byte, regardless of encoding form.
    assert!(
        !buf.contains(&0x00),
        "encoder must reject or strip NUL bytes per RFC 3501 Section 9 (CHAR8 = %x01-ff), \
         but the output contains NUL: {:?}",
        &buf[..]
    );
}

/// L2: \\Recent in STORE command.
///
/// RFC 3501 Section 9: the `flag` production excludes `\Recent`.
/// STORE uses `flag` (not `flag-fetch`), so `\Recent` is not permitted.
/// The encoder silently skips `\Recent` (and `\*`) in STORE and APPEND.
#[test]
fn spec_audit_l2_recent_in_store() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen, crate::types::Flag::Recent],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    // RFC 3501 Section 9: \Recent must NOT appear in STORE output.
    assert!(
        !output.contains("\\Recent"),
        "encoder must skip \\Recent in STORE per RFC 3501 Section 9, \
         but output contains it: {output}"
    );
    // \Seen should still be present.
    assert!(
        output.contains("\\Seen"),
        "\\Seen should still be present in output: {output}"
    );
}

/// L14: Validate sequence set format.
///
/// RFC 3501 Section 9: `sequence-set = (seq-number / seq-range) *("," sequence-set)`
/// where `seq-number = nz-number / "*"` and `nz-number = digit-nz *DIGIT`.
/// Invalid sequence sets like empty strings, alphabetic text, or space-separated
/// L14: sequence-set validation  -  RFC 3501 Section 9 says non-digit sequence
/// numbers must be rejected. The `SequenceSet` newtype catches these at
/// construction time.
#[test]
fn spec_audit_l14_invalid_sequence_set() {
    // "abc" is not a valid sequence set per RFC 3501 Section 9.
    // SequenceSet::new() rejects it at construction time.
    assert!(SequenceSet::new("abc").is_err());
}

// ===== Audit finding #1: APPEND lacks RFC 6855 UTF8 data extension =====

/// RFC 6855 Section 4: when UTF8=ACCEPT has been enabled, APPEND of a
/// message with UTF-8 headers MUST use the `UTF8` APPEND data extension:
/// `APPEND <mailbox> UTF8 (~{size[+]}\r\n<message>)`.
#[test]
fn audit_finding1_append_header_with_utf8_extension() {
    let msgs = [append_msg(&[], None, 100)];
    // UTF8=ACCEPT enabled, no literal extension.
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, true),
    );
    assert_eq!(
        output, "A001 APPEND \"INBOX\" UTF8 (~{100}\r\n<100>)\r\n",
        "RFC 6855 Section 4: UTF8 APPEND data extension, closed after the body"
    );
}

/// Without UTF8 mode, the classic form is used.
#[test]
fn audit_finding1_append_header_classic_without_utf8() {
    let msgs = [append_msg(&[], None, 100)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    );
    assert_eq!(output, "A001 APPEND \"INBOX\" {100}\r\n<100>\r\n");
    assert!(!output.contains("UTF8"));
}

/// RFC 6855 Section 4 in MULTIAPPEND: each message is its own `UTF8 (...)`
/// group, closed before the next message's syntax, with one final CRLF.
#[test]
fn append_utf8_extension_wraps_every_message_of_a_multiappend() {
    let msgs = [
        append_msg(&[crate::types::Flag::Seen], None, 5),
        append_msg(&[], None, 7),
    ];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::Synchronizing, true)
        ),
        "A001 APPEND \"INBOX\" (\\Seen) UTF8 (~{5}\r\n<5>) UTF8 (~{7}\r\n<7>)\r\n"
    );
}

/// RFC 3516 Section 4.4 / RFC 9051 Section 9: APPEND data containing NUL
/// octets must use the `literal8` marker `~{size}\r\n`, not classic
/// `literal` syntax. It needs BINARY; here no literal extension is in play.
#[test]
fn audit_finding6_append_header_uses_literal8_for_binary_append() {
    let msgs = [append_nul_msg(12)];
    let mut o = append_opts(LiteralMode::Synchronizing, false);
    o.capabilities.push(Capability::Binary);
    assert_eq!(
        append_wire("A001", "INBOX", &msgs, false, &o),
        "A001 APPEND \"INBOX\" ~{12}\r\n<12>\r\n"
    );
}

/// RFC 3516 Section 4.4: a NUL-bearing body without BINARY is refused before
/// any bytes exist, as `MissingCapability`: the body is expressible, the
/// server lacks the capability that carries it, and a local refusal is never
/// a provider protocol fault.
#[test]
fn append_nul_body_without_binary_is_refused() {
    let msgs = [append_nul_msg(3)];
    let err = append_command(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::LiteralPlus, false),
    )
    .expect_err("literal8 needs BINARY");
    assert!(
        matches!(err, crate::Error::MissingCapability(ref m) if m.contains("requires BINARY literal8 support")),
        "got {err:?}"
    );
}

/// The refusal is per message: a clean first message does not excuse a NUL in
/// the second.
#[test]
fn multiappend_nul_body_without_binary_is_refused_in_any_position() {
    let msgs = [append_msg(&[], None, 3), append_nul_msg(3)];
    assert!(
        append_command(
            "A001",
            "INBOX",
            &msgs,
            true,
            &append_opts(LiteralMode::Synchronizing, false),
        )
        .is_err()
    );
}

/// RFC 7888 Section 6 matrix for literal8: the `+` marker needs BINARY AND a
/// literal extension, with the LITERAL- size cap, and is never used on pure
/// `IMAP4rev2` (RFC 9051 Section 9).
#[test]
fn append_literal8_plus_marker_matrix() {
    let binary = |mode: LiteralMode| {
        let mut o = append_opts(mode, false);
        o.capabilities.push(Capability::Binary);
        o
    };
    let small = [append_nul_msg(100)];
    let large = [append_nul_msg(5000)];

    assert_eq!(
        append_wire(
            "A",
            "INBOX",
            &small,
            false,
            &binary(LiteralMode::LiteralPlus)
        ),
        "A APPEND \"INBOX\" ~{100+}\r\n<100>\r\n",
        "BINARY + LITERAL+: non-synchronizing literal8"
    );
    assert_eq!(
        append_wire(
            "A",
            "INBOX",
            &small,
            false,
            &binary(LiteralMode::LiteralMinus)
        ),
        "A APPEND \"INBOX\" ~{100+}\r\n<100>\r\n",
        "BINARY + LITERAL- within 4096 octets"
    );
    assert_eq!(
        append_wire(
            "A",
            "INBOX",
            &large,
            false,
            &binary(LiteralMode::LiteralMinus)
        ),
        "A APPEND \"INBOX\" ~{5000}\r\n<5000>\r\n",
        "BINARY + LITERAL- over 4096 octets stays synchronizing"
    );
    assert_eq!(
        append_wire(
            "A",
            "INBOX",
            &small,
            false,
            &binary(LiteralMode::Synchronizing)
        ),
        "A APPEND \"INBOX\" ~{100}\r\n<100>\r\n",
        "BINARY without a literal extension"
    );

    // Pure rev2 with BINARY advertised: literal8 is always synchronizing.
    let rev2_binary = EncodeOptions {
        utf8_mode: true,
        literal_mode: LiteralMode::LiteralMinus,
        capabilities: vec![
            Capability::Imap4Rev2,
            Capability::MultiAppend,
            Capability::Binary,
        ],
        enabled: Vec::new(),
    };
    assert_eq!(
        append_wire("A", "INBOX", &small, false, &rev2_binary),
        "A APPEND \"INBOX\" ~{100}\r\n<100>\r\n",
        "rev2 literal8 has no `+` modifier"
    );
}

/// RFC 9051 Appendix B: IMAP4rev2 folds in the BINARY FETCH items but not
/// RFC 3516's APPEND extension, so a pure rev2 server that does not advertise
/// BINARY is owed no `literal8` APPEND body. A NUL-bearing message is refused
/// before any byte exists, exactly as on rev1.
#[test]
fn rev2_does_not_imply_binary_for_append() {
    let rev2 = EncodeOptions {
        utf8_mode: true,
        literal_mode: LiteralMode::LiteralMinus,
        capabilities: vec![Capability::Imap4Rev2],
        enabled: Vec::new(),
    };
    let err = append_command("A", "INBOX", &[append_nul_msg(100)], false, &rev2)
        .expect_err("pure rev2 without BINARY cannot carry a NUL body");
    assert!(
        matches!(err, crate::Error::MissingCapability(ref m) if m.contains("requires BINARY literal8 support")),
        "got {err:?}"
    );
}

// ===== Audit finding #3: QRESYNC encoder fabricates known_uids =====

/// RFC 7162 Section 3.2.5.2 ABNF:
/// `"QRESYNC" SP "(" uidvalidity SP mod-sequence-value
///   [SP known-uids] [SP seq-match-data] ")"`
///
/// `seq-match-data` is only valid after `known-uids`. The encoder must
/// NOT silently inject `1:*` as a placeholder when `seq_match_data` is
/// provided without `known_uids`  -  it must return an error.
#[test]
fn audit_finding3_qresync_rejects_seq_match_without_known_uids() {
    use crate::types::response::QresyncParams;

    let params = QresyncParams {
        uid_validity: 67890,
        mod_seq: 12345,
        known_uids: None,
        seq_match_data: Some(("1:100".into(), "1:100".into())),
    };
    let mut buf = BytesMut::new();
    // Must return Err because seq_match_data is present without known_uids.
    let result = push_select_or_examine(
        &mut buf,
        "A001",
        "SELECT",
        "INBOX",
        false,
        Some(&params),
        false,
        LiteralMode::Synchronizing,
    );
    assert!(
        result.is_err(),
        "seq-match-data without known-uids must return Err (RFC 7162 Section 3.2.5.2)"
    );
}

/// QRESYNC with valid `known_uids` + `seq_match_data` encodes correctly.
#[test]
fn audit_finding3_qresync_valid_known_uids_with_seq_match() {
    use crate::types::response::QresyncParams;

    let params = QresyncParams {
        uid_validity: 67890,
        mod_seq: 12345,
        known_uids: Some("1:500".into()),
        seq_match_data: Some(("1:100".into(), "1:100".into())),
    };
    let mut buf = BytesMut::new();
    push_select_or_examine(
        &mut buf,
        "A001",
        "SELECT",
        "INBOX",
        false,
        Some(&params),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();

    // Valid case: known_uids is present, so seq_match_data is legal.
    assert!(
        output.contains("1:500"),
        "known_uids should appear in output: {output}"
    );
    assert!(
        output.contains("(1:100 1:100)"),
        "seq_match_data should appear in output: {output}"
    );
    assert!(
        !output.contains("1:*"),
        "should NOT contain fabricated 1:* when known_uids is provided: {output}"
    );
}

/// QRESYNC with `known_uids` but no `seq_match_data` encodes correctly.
#[test]
fn audit_finding3_qresync_known_uids_without_seq_match() {
    use crate::types::response::QresyncParams;

    let params = QresyncParams {
        uid_validity: 67890,
        mod_seq: 12345,
        known_uids: Some("1:500".into()),
        seq_match_data: None,
    };
    let mut buf = BytesMut::new();
    push_select_or_examine(
        &mut buf,
        "A001",
        "SELECT",
        "INBOX",
        false,
        Some(&params),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 SELECT \"INBOX\" (QRESYNC (67890 12345 1:500))\r\n",
        "QRESYNC with known_uids but no seq_match_data"
    );
}

// ===== Audit finding #7: METADATA encoding lacks MAXSIZE/DEPTH options =====

/// RFC 5464 Section 5 allows GETMETADATA with options:
/// `GETMETADATA [options] <mailbox> <entries>`
/// where options include `MAXSIZE n` and `DEPTH ("0"|"1"|"infinity")`.
///
/// Regression coverage for option encoding and wire ordering.
/// GETMETADATA without options remains backward compatible.
#[test]
fn audit_finding7_getmetadata_without_options() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        None,
        None,
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output,
        "A001 GETMETADATA \"INBOX\" \"/private/comment\"\r\n",
    );
}

/// GETMETADATA with MAXSIZE and DEPTH options (RFC 5464 Section 4.2.2).
#[test]
fn audit_finding7_getmetadata_with_maxsize_and_depth() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(1024),
        Some("infinity"),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output,
        "A001 GETMETADATA (MAXSIZE 1024 DEPTH infinity) \"INBOX\" \"/private/comment\"\r\n",
    );
}

/// GETMETADATA with only MAXSIZE option.
#[test]
fn audit_finding7_getmetadata_with_maxsize_only() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(2048),
        None,
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output,
        "A001 GETMETADATA (MAXSIZE 2048) \"INBOX\" \"/private/comment\"\r\n",
    );
}

/// GETMETADATA with only DEPTH option.
#[test]
fn audit_finding7_getmetadata_with_depth_only() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        None,
        Some("1"),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output,
        "A001 GETMETADATA (DEPTH 1) \"INBOX\" \"/private/comment\"\r\n",
    );
}

/// RFC 5464 Section 5 plus verified errata 2785/2786 require
/// GETMETADATA options to precede the mailbox name.
#[test]
fn audit_getmetadata_options_before_mailbox_per_verified_errata() {
    let mut buf = BytesMut::new();
    push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(1024),
        Some("infinity"),
        false,
        LiteralMode::Synchronizing,
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 GETMETADATA (MAXSIZE 1024 DEPTH infinity) \"INBOX\" \"/private/comment\"\r\n",
        "RFC 5464 Section 5 / errata 2785 and 2786: GETMETADATA options must precede the mailbox",
    );
}

// ===== Audit finding #8: SORT/SETQUOTA commands encode correctly =====

/// The SORT command encoder exists internally even though the public
/// API does not expose it. Verify encoding is correct so that when
/// finding #8 is fixed (public API exposure), the wire format is ready.
///
/// Note: the encoder wraps the `sort_criteria` field in parentheses, so
/// the `sort_criteria` field should contain just the sort keys (e.g. `"DATE"`),
/// not the parens themselves.
#[test]
fn audit_finding8_sort_command_encodes_correctly() {
    let mut buf = BytesMut::new();
    let cmd = Command::Sort {
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: "ALL".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 SORT (DATE) UTF-8 ALL\r\n",
        "SORT command should encode correctly (RFC 5256 Section 2)"
    );
}

/// UID SORT command encoding.
#[test]
fn audit_finding8_uid_sort_command_encodes_correctly() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidSort {
        sort_criteria: "REVERSE DATE".into(),
        charset: "UTF-8".into(),
        criteria: "SINCE 1-Jan-2024".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert_eq!(
        output, "A001 UID SORT (REVERSE DATE) UTF-8 SINCE 1-Jan-2024\r\n",
        "UID SORT command should encode correctly (RFC 5256 Section 2)"
    );
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

// =====================================================================
// Audit tests (2026-03-16)
// =====================================================================

/// H3: Single-message APPEND must filter `\Recent` and `\*`.
/// RFC 3501 Section 9: `flag` excludes `\Recent` and `\*`.
#[test]
fn audit_h3_single_append_filters_recent_and_wildcard() {
    let flags = vec![
        crate::types::Flag::Seen,
        crate::types::Flag::Recent,
        crate::types::Flag::Wildcard,
        crate::types::Flag::Flagged,
    ];
    let msgs = [append_msg(&flags, None, 10)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    );
    assert!(
        !output.contains("\\Recent"),
        "must filter \\Recent: {output}"
    );
    assert!(!output.contains("\\*"), "must filter \\*: {output}");
    assert!(output.contains("\\Seen"), "must keep \\Seen");
    assert!(output.contains("\\Flagged"), "must keep \\Flagged");
}

/// L8: STORE with all flags filtered must return an error.
///
/// RFC 9051 Section 6.4.6 tightens the ABNF to require at least one flag:
/// `flag-list = "(" flag *(SP flag) ")"`  -  the content is no longer optional.
#[test]
fn audit_l8_store_all_flags_filtered() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Recent, crate::types::Flag::Wildcard],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE with all flags filtered out must return an error \
         per RFC 9051 Section 6.4.6"
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

/// L9: `SequenceSet::new` rejects invalid input (RFC 3501 Section 9).
#[test]
fn audit_l9_sequence_set_rejects_invalid() {
    assert!(SequenceSet::new("abc").is_err());
    assert!(SequenceSet::new("0").is_err());
    assert!(SequenceSet::new("").is_err());
    assert!(SequenceSet::new("1,,2").is_err());
    assert!(SequenceSet::new("1:*").is_ok());
    assert!(SequenceSet::new("1,2,3").is_ok());
}

/// RFC 3501 Section 9: `nz-number` is a non-zero unsigned 32-bit integer
/// (0 < n < 4,294,967,296). Values exceeding `u32::MAX` must be rejected.
#[test]
fn sequence_set_rejects_overflow_u32() {
    // 99999999999 exceeds u32::MAX (4294967295)
    assert!(SequenceSet::new("99999999999").is_err());
    // u32::MAX itself is valid
    assert!(SequenceSet::new("4294967295").is_ok());
    // u32::MAX + 1 is invalid
    assert!(SequenceSet::new("4294967296").is_err());
    // Very large number in a range
    assert!(SequenceSet::new("1:99999999999").is_err());
    assert!(SequenceSet::new("99999999999:1").is_err());
}

/// RFC 3501 Section 9: invalid sequence set "0" is rejected at
/// construction time by `SequenceSet::new()`.
#[test]
fn sequence_set_rejects_zero_via_newtype() {
    assert!(
        SequenceSet::new("0").is_err(),
        "\"0\" must be rejected per RFC 3501 Section 9 (nz-number)"
    );
}

/// RFC 3501 Section 9: empty sequence set is rejected at construction time.
#[test]
fn sequence_set_rejects_empty() {
    assert!(
        SequenceSet::new("").is_err(),
        "Empty string must be rejected per RFC 3501 Section 9"
    );
}

/// RFC 3501 Section 9: non-numeric sequence set is rejected at construction time.
#[test]
fn sequence_set_rejects_alphabetic() {
    assert!(
        SequenceSet::new("abc").is_err(),
        "Alphabetic string must be rejected per RFC 3501 Section 9"
    );
}

/// RFC 3501 Section 9: u32-overflow sequence set is rejected at construction time.
#[test]
fn sequence_set_rejects_overflow() {
    assert!(
        SequenceSet::new("99999999999").is_err(),
        "u32 overflow must be rejected per RFC 3501 Section 9"
    );
}

/// RFC 3501 Section 9: zero is not a valid nz-number.
#[test]
fn sequence_set_rejects_zero() {
    assert!(
        SequenceSet::new("0").is_err(),
        "Zero must be rejected per RFC 3501 Section 9 (nz-number)"
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

/// SETMETADATA with empty entries must return an error (RFC 5464 Section 5).
///
/// RFC 5464 Section 5 ABNF: `entry-values = "(" entry *(SP entry) ")"`  -  at
/// least one entry is REQUIRED. An empty list produces the invalid `SETMETADATA "INBOX" ()\r\n`.
#[test]
fn encode_setmetadata_empty_entries_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![],
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SETMETADATA with empty entries must fail (RFC 5464 Section 5)"
    );
}

/// GETMETADATA with empty entries must return an error (RFC 5464 Section 4.2).
///
/// RFC 5464 Section 4.2 ABNF: `entries = entry / "(" entry *(SP entry) ")"`  -  at
/// least one entry is REQUIRED. An empty list produces the invalid `GETMETADATA "INBOX" ()\r\n`.
#[test]
fn encode_getmetadata_empty_entries_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![],
        max_size: None,
        depth: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "GETMETADATA with empty entries must fail (RFC 5464 Section 4.2)"
    );
}

/// GETMETADATA with invalid DEPTH must return an error (RFC 5464 Section 4.2.2).
///
/// RFC 5464 Section 4.2.2: `scope-opt = "DEPTH" SP ("0" / "1" / "infinity")`
/// Only "0", "1", and "infinity" are valid depth values.
#[test]
fn encode_getmetadata_invalid_depth_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into()],
        max_size: None,
        depth: Some("2".into()),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "GETMETADATA with invalid DEPTH \"2\" must fail (RFC 5464 Section 4.2.2)"
    );
}

/// RFC 5464 Section 3.2 entry-name rules also apply to GETMETADATA.
#[test]
fn encode_getmetadata_invalid_entry_names_return_error() {
    for entry in [
        "comment",
        "/public/comment",
        "/private/%bad",
        "/private/*bad",
        "/private//bad",
        "/private/bad/",
        "/private/r\u{00E9}sum\u{00E9}",
        "/private/\u{0019}control",
    ] {
        let mut buf = BytesMut::new();
        let cmd = Command::GetMetadata {
            mailbox: MailboxName::new("INBOX").unwrap(),
            entries: vec![entry.into()],
            max_size: None,
            depth: None,
        };
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "GETMETADATA entry name {entry:?} must be rejected per RFC 5464 Section 3.2"
        );
    }
}

/// GETMETADATA with valid DEPTH values should succeed.
#[test]
fn encode_getmetadata_valid_depth_values() {
    for depth in &["0", "1", "infinity"] {
        let mut buf = BytesMut::new();
        let cmd = Command::GetMetadata {
            mailbox: MailboxName::new("INBOX").unwrap(),
            entries: vec!["/private/comment".into()],
            max_size: None,
            depth: Some((*depth).to_string()),
        };
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_ok(),
            "GETMETADATA with valid DEPTH \"{depth}\" must succeed"
        );
    }
}

/// custom flag keywords containing atom-special characters must
/// be rejected by the encoder (RFC 3501 Section 9: flag-keyword = atom).
#[test]
fn regression_custom_flag_with_spaces_rejected() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Custom("has space".into())],
        unchanged_since: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "custom flag with space must be rejected (RFC 3501 Section 9: ATOM-CHAR)"
    );
}

/// custom flag keywords containing parentheses must be rejected.
#[test]
fn regression_custom_flag_with_parens_rejected() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Custom("bad(flag".into())],
        unchanged_since: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "custom flag with parenthesis must be rejected (RFC 3501 Section 9: ATOM-CHAR)"
    );
}

/// empty custom flag keyword must be rejected.
#[test]
fn regression_empty_custom_flag_rejected() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Custom(String::new())],
        unchanged_since: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty custom flag must be rejected (RFC 3501 Section 9: atom = 1*ATOM-CHAR)"
    );
}

/// Valid custom flag keywords must still be accepted.
#[test]
fn regression_valid_custom_flags_accepted() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![
            crate::types::Flag::Custom("$Important".into()),
            crate::types::Flag::Custom("$Junk".into()),
            crate::types::Flag::Custom("NonJunk".into()),
        ],
        unchanged_since: None,
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "valid custom flags must be accepted; got: {result:?}"
    );
}

/// GETMETADATA with MAXSIZE option must place the option BEFORE
/// the mailbox per RFC 5464 Section 5 and verified errata 2785.
#[test]
fn regression_getmetadata_options_before_mailbox() {
    // Verified errata 2785 corrects the example to:
    //   C: a GETMETADATA (MAXSIZE 1024) "INBOX" (/shared/comment /private/comment)
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/shared/comment".into(), "/private/comment".into()],
        max_size: Some(1024),
        depth: None,
    };
    encode_command_to_buf(&mut buf, "a", &cmd, &default_opts()).unwrap();
    assert_eq!(
        std::str::from_utf8(&buf).unwrap(),
        "a GETMETADATA (MAXSIZE 1024) \"INBOX\" (\"/shared/comment\" \"/private/comment\")\r\n",
        "GETMETADATA options must come BEFORE the mailbox (RFC 5464 Section 5 / errata 2785)"
    );
}

/// GETMETADATA with DEPTH option must place the option BEFORE
/// the mailbox per RFC 5464 Section 5 and verified errata 2786.
#[test]
fn regression_getmetadata_depth_before_mailbox() {
    // Verified errata 2786 corrects the example to:
    //   C: a GETMETADATA (DEPTH 1) "INBOX" (/private/filters/values)
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/filters/values".into()],
        max_size: None,
        depth: Some("1".into()),
    };
    encode_command_to_buf(&mut buf, "a", &cmd, &default_opts()).unwrap();
    assert_eq!(
        std::str::from_utf8(&buf).unwrap(),
        "a GETMETADATA (DEPTH 1) \"INBOX\" \"/private/filters/values\"\r\n",
        "GETMETADATA DEPTH must come BEFORE the mailbox (RFC 5464 Section 5 / errata 2786)"
    );
}

/// GETMETADATA with both MAXSIZE and DEPTH options
/// must place them BEFORE the mailbox (RFC 5464 Section 5).
#[test]
fn regression_getmetadata_both_options_before_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::GetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec!["/private/comment".into()],
        max_size: Some(2048),
        depth: Some("infinity".into()),
    };
    encode_command_to_buf(&mut buf, "a", &cmd, &default_opts()).unwrap();
    assert_eq!(
        std::str::from_utf8(&buf).unwrap(),
        "a GETMETADATA (MAXSIZE 2048 DEPTH infinity) \"INBOX\" \"/private/comment\"\r\n",
        "GETMETADATA MAXSIZE + DEPTH must come BEFORE the mailbox (RFC 5464 Section 5 / errata 2785/2786)"
    );
}

/// QRESYNC seq-match-data without known-uids must return Err,
/// not silently produce malformed output (RFC 7162 Section 3.2.5.2).
///
/// The ABNF nests `[SP seq-match-data]` inside `[SP known-uids]`, meaning
/// seq-match-data can only appear when known-uids is present. The encoder
/// must return a proper error rather than relying on `debug_assert`.
#[test]
fn regression_qresync_seq_match_without_known_uids_returns_err() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 67890,
            mod_seq: 12345,
            known_uids: None,
            seq_match_data: Some(("1:100".into(), "1:100".into())),
        }),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "seq-match-data without known-uids must return Err (RFC 7162 Section 3.2.5.2)"
    );
}

/// `$` must be valid as a component in comma-separated
/// sequence sets (RFC 5182 Section 5).
///
/// RFC 5182 Section 5 extends the grammar: `sequence-set =/ seq-last-command`
/// where `seq-last-command = "$"`. Since the original `sequence-set` includes
/// `*("," sequence-set)` recursion, `$` is valid in comma-separated lists.
#[test]
fn regression_sequence_set_dollar_in_comma_list() {
    // RFC 5182 Section 5: $ as component in comma-separated lists
    assert!(
        SequenceSet::new("1:5,$").is_ok(),
        "\"1:5,$\" must be valid per RFC 5182 Section 5"
    );
    assert!(
        SequenceSet::new("$,1:*").is_ok(),
        "\"$,1:*\" must be valid per RFC 5182 Section 5"
    );
    assert!(
        SequenceSet::new("42,$").is_ok(),
        "\"42,$\" must be valid per RFC 5182 Section 5"
    );
    // Bare $ must still work
    assert!(
        SequenceSet::new("$").is_ok(),
        "bare \"$\" must still be valid per RFC 5182 Section 2"
    );
    // $ cannot be part of a range (it's not a seq-number)
    assert!(
        SequenceSet::new("$:5").is_err(),
        "\"$:5\" must be rejected ($ is not a seq-number, cannot form a range)"
    );
}

/// QRESYNC known-uids must reject `*` (RFC 7162 Section 3.2.5.2).
///
/// RFC 7162 Section 3.2.5.2 states that `*` is not allowed in
/// known-uids, known-sequence-set, and known-uid-set. The encoder
/// must return an error rather than silently producing invalid output.
#[test]
fn regression_qresync_known_uids_rejects_wildcard() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 67890,
            mod_seq: 12345,
            known_uids: Some("1:*".into()),
            seq_match_data: None,
        }),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "known-uids containing '*' must return Err (RFC 7162 Section 3.2.5.2)"
    );
}

/// QRESYNC seq-match-data known-sequence-set must reject `*`.
#[test]
fn regression_qresync_seq_match_data_rejects_wildcard_in_seq_set() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 67890,
            mod_seq: 12345,
            known_uids: Some("1:500".into()),
            seq_match_data: Some(("1:*".into(), "1:100".into())),
        }),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "known-sequence-set containing '*' must return Err (RFC 7162 Section 3.2.5.2)"
    );
}

/// QRESYNC seq-match-data known-uid-set must reject `*`.
#[test]
fn regression_qresync_seq_match_data_rejects_wildcard_in_uid_set() {
    let mut buf = BytesMut::new();
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(QresyncParams {
            uid_validity: 67890,
            mod_seq: 12345,
            known_uids: Some("1:500".into()),
            seq_match_data: Some(("1:100".into(), "1:*".into())),
        }),
    };
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "known-uid-set containing '*' must return Err (RFC 7162 Section 3.2.5.2)"
    );
}

/// STORE with all flags filtered must return an error.
///
/// RFC 9051 Section 6.4.6 tightens the ABNF to require at least one flag:
/// `flag-list = "(" flag *(SP flag) ")"`  -  the content is no longer optional.
#[test]
fn store_all_flags_filtered_returns_error_recent_only() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Recent],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE with only \\Recent (filtered out) must return an error \
         per RFC 9051 Section 6.4.6"
    );
}

// --- Flag validation and filtering tests (STORE / APPEND) ---
// These lock down the behavior of the shared flag-filter + validate logic
// in encode_store_flags and encode_append before refactoring.

/// RFC 3501 Section 9: `\*` (Wildcard) is not valid in the `flag`
/// production used by STORE; it must be silently filtered.
#[test]
fn store_filters_wildcard_flag() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![
            crate::types::Flag::Seen,
            crate::types::Flag::Wildcard,
            crate::types::Flag::Flagged,
        ],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 STORE 1 +FLAGS (\\Seen \\Flagged)\r\n",
        "Wildcard must be filtered from STORE flags"
    );
}

/// RFC 3501 Section 9: `\Recent` is not valid in the `flag` production
/// used by STORE; it must be silently filtered.
#[test]
fn store_filters_recent_flag() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("2").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![
            crate::types::Flag::Seen,
            crate::types::Flag::Recent,
            crate::types::Flag::Flagged,
        ],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 STORE 2 +FLAGS (\\Seen \\Flagged)\r\n",
        "Recent must be filtered from STORE flags"
    );
}

/// STORE accepts valid custom flag keywords (RFC 3501 Section 9: `flag-keyword`).
#[test]
fn store_accepts_valid_custom_flag() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![
            crate::types::Flag::Seen,
            crate::types::Flag::Custom("$Important".into()),
        ],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 STORE 1 +FLAGS (\\Seen $Important)\r\n");
}

/// STORE rejects custom flags containing characters outside ATOM-CHAR
/// (RFC 3501 Section 9).
#[test]
fn store_rejects_invalid_custom_flag() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Custom("bad flag".into())],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "custom flag with space must be rejected"
    );
}

/// STORE with only filtered-out flags (e.g., `\Recent`) must return an error
/// rather than emitting an empty flag-list `FLAGS ()`.
///
/// RFC 9051 Section 6.4.6 tightens the ABNF to require at least one flag:
/// `flag-list = "(" flag *(SP flag) ")"`  -  the content is no longer optional.
/// Even under RFC 3501, sending an empty flag-list is semantically useless
/// and likely to confuse servers.
#[test]
fn store_all_flags_filtered_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Recent],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE with all flags filtered out must return an error \
         per RFC 9051 Section 6.4.6 (flag-list requires at least one flag)"
    );
}

/// Regression IMAP-003: STORE Replace with empty flags must produce `FLAGS ()`.
///
/// RFC 3501 Section 9 / RFC 9051 Section 9: `flag-list = "(" [flag *(SP flag)] ")"`.
/// `FLAGS ()` is a legitimate command to clear all flags on a message. The encoder
/// must allow empty flag lists for `Replace` and `ReplaceSilent` operations.
#[test]
fn imap_003_store_replace_empty_flags_produces_flags_empty() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        operation: crate::types::StoreOperation::Replace,
        flags: vec![],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 STORE 1:5 FLAGS ()\r\n",
        "STORE Replace with empty flags must produce FLAGS () to clear all flags"
    );
}

/// Regression IMAP-003: STORE `ReplaceSilent` with empty flags must produce `FLAGS.SILENT ()`.
#[test]
fn imap_003_store_replace_silent_empty_flags_produces_flags_silent_empty() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("42").unwrap(),
        operation: crate::types::StoreOperation::ReplaceSilent,
        flags: vec![],
        unchanged_since: None,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 UID STORE 42 FLAGS.SILENT ()\r\n",
        "STORE ReplaceSilent with empty flags must produce FLAGS.SILENT () to clear all flags"
    );
}

/// Regression IMAP-003: STORE Add with empty flags must still return an error.
///
/// Adding zero flags is semantically useless and likely indicates a caller bug.
#[test]
fn imap_003_store_add_empty_flags_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE +FLAGS with empty flags must return an error"
    );
}

/// Regression IMAP-003: STORE Remove with empty flags must still return an error.
#[test]
fn imap_003_store_remove_empty_flags_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Remove,
        flags: vec![],
        unchanged_since: None,
    };
    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_err(),
        "STORE -FLAGS with empty flags must return an error"
    );
}

/// RFC 3501 Section 9: APPEND must filter `\Recent` and `\*` from flags.
#[test]
fn append_filters_recent_and_wildcard_flags() {
    let msgs = [append_msg(
        &[
            crate::types::Flag::Seen,
            crate::types::Flag::Recent,
            crate::types::Flag::Wildcard,
            crate::types::Flag::Flagged,
        ],
        None,
        42,
    )];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" (\\Seen \\Flagged) {42}\r\n<42>\r\n",
        "Recent and Wildcard must be filtered from APPEND flags"
    );
}

/// APPEND accepts valid custom flag keywords (RFC 3501 Section 9).
#[test]
fn append_accepts_valid_custom_flag() {
    let msgs = [append_msg(
        &[crate::types::Flag::Custom("$MailFlagBit0".into())],
        None,
        10,
    )];
    assert_eq!(
        append_wire(
            "A001",
            "INBOX",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A001 APPEND \"INBOX\" ($MailFlagBit0) {10}\r\n<10>\r\n"
    );
}

/// APPEND rejects custom flags containing invalid characters (RFC 3501 Section 9).
#[test]
fn append_rejects_invalid_custom_flag() {
    let msgs = [append_msg(
        &[crate::types::Flag::Custom("bad{flag}".into())],
        None,
        10,
    )];
    let result = append_command(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false),
    );
    assert!(result.is_err(), "custom flag with braces must be rejected");
}

/// MULTIAPPEND needs the capability (RFC 3502 Section 3) and at least one
/// message; a single APPEND carries exactly one.
#[test]
fn append_shape_and_capability_validation() {
    let one = [append_msg(&[], None, 1)];
    let two = [append_msg(&[], None, 1), append_msg(&[], None, 1)];
    let with_cap = append_opts(LiteralMode::Synchronizing, false);
    let without_cap = opts(LiteralMode::Synchronizing, false);

    assert!(matches!(
        append_command("A", "INBOX", &one, true, &without_cap),
        Err(crate::Error::MissingCapability(ref m)) if m == "MULTIAPPEND"
    ));
    // A plain APPEND never needs it.
    assert!(append_command("A", "INBOX", &one, false, &without_cap).is_ok());
    assert!(matches!(
        append_command("A", "INBOX", &[], true, &with_cap),
        Err(crate::Error::InvalidInput(_))
    ));
    assert!(matches!(
        append_command("A", "INBOX", &two, false, &with_cap),
        Err(crate::Error::InvalidInput(_))
    ));
}

/// Under active `IMAP4rev2` the mailbox goes out as raw UTF-8 like every other
/// command's mailbox argument, not as modified UTF-7 (RFC 9051 Section 5.1);
/// `UTF8=ACCEPT` is what selects the `UTF8 (` wrapper, and rev2 alone does not.
#[test]
fn append_rev2_uses_utf8_mailbox_without_the_utf8_wrapper() {
    let rev2 = EncodeOptions {
        utf8_mode: true,
        literal_mode: LiteralMode::Synchronizing,
        capabilities: vec![Capability::Imap4Rev2],
        enabled: Vec::new(),
    };
    let msgs = [append_msg(&[], None, 3)];
    assert_eq!(
        append_wire("A", "café", &msgs, false, &rev2),
        "A APPEND \"café\" {3}\r\n<3>\r\n"
    );
    // The same mailbox on rev1 without UTF8=ACCEPT is modified UTF-7.
    assert_eq!(
        append_wire(
            "A",
            "café",
            &msgs,
            false,
            &append_opts(LiteralMode::Synchronizing, false)
        ),
        "A APPEND \"caf&AOk-\" {3}\r\n<3>\r\n"
    );
}

// --- CHANGEDSINCE modifier boundary tests ---

/// RFC 7162 Section 7: minimum valid mod-sequence-value is 1.
#[test]
fn encode_fetch_changedsince_minimum_valid() {
    let mut buf = BytesMut::new();
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(1),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 FETCH 1:* (FLAGS) (CHANGEDSINCE 1)\r\n");
}

/// RFC 7162 Section 7: minimum valid mod-sequence-value is 1 (UID FETCH variant).
#[test]
fn encode_uid_fetch_changedsince_minimum_valid() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS)".into(),
        changed_since: Some(1),
        vanished: false,
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 UID FETCH 1:* (FLAGS) (CHANGEDSINCE 1)\r\n");
}

// --- RFC 6855 UTF8=ACCEPT quoted string encoding tests ---

/// RFC 6855 Section 3: ASCII-only input should produce quoted form regardless of `utf8_mode`.
#[test]
fn spec_audit_utf8_mode_ascii_produces_quoted() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, b"Brouillons", true, LiteralMode::Synchronizing);
    assert_eq!(
        &buf[..],
        b"\"Brouillons\"",
        "ASCII mailbox must be quoted in UTF-8 mode per RFC 6855 Section 3"
    );
}

/// RFC 6855 Section 3 / RFC 9051 Section 9: when UTF8=ACCEPT is active,
/// valid UTF-8 non-ASCII data must use quoted form, not literal.
#[test]
fn spec_audit_utf8_mode_non_ascii_produces_quoted() {
    let mut buf = BytesMut::new();
    // "日本語" = 9 bytes in UTF-8: e6 97 a5 e6 9c ac e8 aa 9e
    push_quoted_or_literal_utf8(
        &mut buf,
        "日本語".as_bytes(),
        true,
        LiteralMode::Synchronizing,
    );
    assert!(
        buf.starts_with(b"\""),
        "UTF-8 mailbox name must be quoted when UTF8=ACCEPT is active \
         per RFC 6855 Section 3, got literal form instead"
    );
    // Verify the full output: quoted "日本語"
    let expected_bytes = b"\"\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e\"";
    assert_eq!(
        &buf[..],
        &expected_bytes[..],
        "UTF-8 mailbox name must be quoted per RFC 6855 Section 3"
    );
}

/// RFC 3501 Section 9: without UTF8=ACCEPT, non-ASCII bytes must use literal form.
#[test]
fn spec_audit_no_utf8_mode_non_ascii_produces_literal() {
    let mut buf = BytesMut::new();
    // "日本語" = 9 bytes in UTF-8
    push_quoted_or_literal_utf8(
        &mut buf,
        "日本語".as_bytes(),
        false,
        LiteralMode::Synchronizing,
    );
    assert!(
        buf.starts_with(b"{"),
        "Non-ASCII mailbox name must use literal form when UTF8=ACCEPT is not active \
         per RFC 3501 Section 9, got quoted form instead"
    );
    assert_eq!(
        &buf[..],
        b"{9}\r\n\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e",
        "Non-ASCII mailbox name must use literal form per RFC 3501 Section 9"
    );
}

/// RFC 9051 Section 9: CR and LF are never quotable, even in UTF-8 mode.
/// TEXT-CHAR excludes CR (%x0D) and LF (%x0A).
#[test]
fn spec_audit_utf8_mode_crlf_produces_literal() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        b"line1\r\nline2",
        true,
        LiteralMode::Synchronizing,
    );
    assert!(
        buf.starts_with(b"{"),
        "Data with CR/LF must use literal form even when UTF8=ACCEPT is active \
         per RFC 9051 Section 9 TEXT-CHAR definition"
    );
    assert_eq!(
        &buf[..],
        b"{12}\r\nline1\r\nline2",
        "CR/LF data must produce literal form per RFC 9051 Section 9"
    );
}

/// RFC 6855 Section 3: invalid UTF-8 sequences must fall back to literal even
/// when `utf8_mode` is true, since they cannot be represented in quoted strings.
#[test]
fn spec_audit_utf8_mode_invalid_utf8_produces_literal() {
    let mut buf = BytesMut::new();
    // Invalid UTF-8: lone continuation byte
    let invalid = &[0x80, 0x81, 0x82];
    push_quoted_or_literal_utf8(&mut buf, invalid, true, LiteralMode::Synchronizing);
    assert!(
        buf.starts_with(b"{"),
        "Invalid UTF-8 must use literal form even when UTF8=ACCEPT is active \
         per RFC 6855 Section 3 (only valid UTF-8 is allowed in quoted strings)"
    );
}

/// RFC 9051 Section 9: CHAR = %x01-7E / UTF8-2 / UTF8-3 / UTF8-4.
/// DEL (0x7F) is a single-byte ASCII character NOT in %x01-7E, so it must
/// NOT appear in quoted strings even when UTF8=ACCEPT is active.
#[test]
fn spec_audit_utf8_mode_del_byte_produces_literal() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        b"hello\x7Fworld",
        true,
        LiteralMode::Synchronizing,
    );
    assert!(
        buf.starts_with(b"{"),
        "DEL byte (0x7F) must trigger literal encoding in UTF-8 mode \
         per RFC 9051 Section 9 (CHAR = %x01-7E / UTF8-2/3/4), got: {:?}",
        std::str::from_utf8(&buf)
    );
}

// --- CommandWriter::string: backslash and double-quote escaping ---

/// RFC 3501 Section 9: quoted-specials = DQUOTE / "\"
/// Both backslash and double-quote must be escaped in quoted strings.
#[test]
fn encode_quoted_escapes_backslash_and_dquote() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal(&mut buf, b"a\\b\"c", LiteralMode::Synchronizing);
    // Expected: "a\\b\"c"
    assert_eq!(&buf[..], b"\"a\\\\b\\\"c\"");
}

// --- CommandWriter::string in UTF-8 mode: NUL stripping ---

/// RFC 3501 Section 9: NUL (%x00) is forbidden and must be stripped
/// even when UTF8=ACCEPT is active (RFC 6855 Section 3).
#[test]
fn encode_utf8_mode_strips_nul_bytes() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        b"hello\x00world",
        true,
        LiteralMode::Synchronizing,
    );
    assert_eq!(
        &buf[..],
        b"\"helloworld\"",
        "NUL bytes must be stripped in UTF-8 mode per RFC 3501 Section 9"
    );
    assert!(!buf.contains(&0x00), "output must not contain NUL bytes");
}

/// RFC 3501 Section 9 / RFC 6855 Section 3: backslash and double-quote
/// must be escaped in quoted strings even in UTF-8 mode.
#[test]
fn encode_utf8_mode_escapes_backslash_and_dquote() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, b"a\\b\"c", true, LiteralMode::Synchronizing);
    assert_eq!(
        &buf[..],
        b"\"a\\\\b\\\"c\"",
        "backslash and double-quote must be escaped in UTF-8 mode"
    );
}

// --- APPEND date-time validation (RFC 3501 Section 9) ---

/// RFC 3501 Section 9: `date-day-fixed = (SP DIGIT) / 2DIGIT`.
/// The `2DIGIT` alternative matches any two ASCII digits, so zero-padded
/// single-digit days like `07` are syntactically valid and semantically
/// correct (day 7 of the month). The validator must accept them.
#[test]
fn test_append_accepts_zero_padded_day() {
    let result = append_with_date("07-Jul-1996 02:44:25 -0700");
    assert!(
        result.is_ok(),
        "zero-padded day '07' is valid per RFC 3501 Section 9 \
         (date-day-fixed = (SP DIGIT) / 2DIGIT): {result:?}"
    );
}

/// Zero-padded days 01 through 09 must all be accepted per RFC 3501 Section 9.
#[test]
fn test_append_accepts_all_zero_padded_days() {
    for day in 1..=9u8 {
        let date = format!("0{day}-Jan-2024 12:00:00 +0000");
        let result = validate_append_datetime(&date);
        assert!(
            result.is_ok(),
            "zero-padded day '0{day}' must be accepted per RFC 3501 Section 9 \
             date-day-fixed 2DIGIT: {result:?}"
        );
    }
}

/// Day `00` must be rejected  -  it matches `2DIGIT` syntactically but is
/// not a valid calendar day.
#[test]
fn test_append_rejects_day_zero() {
    let result = validate_append_datetime("00-Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "day '00' is not a valid calendar day and must be rejected"
    );
}

/// invalid month name must be rejected (RFC 3501 Section 9).
#[test]
fn test_append_rejects_invalid_month() {
    let result = append_with_date("31-Foo-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "invalid month name should be rejected per RFC 3501 Section 9 date-month"
    );
}

/// completely malformed date string must be rejected, with the
/// `InvalidAppendDate` variant the account layer maps: the encoder returns
/// the crate's own error, so the variant survives to the driver unchanged.
#[test]
fn test_append_rejects_garbage_date() {
    let result = append_with_date("not-a-date");
    assert!(
        matches!(result, Err(crate::Error::InvalidAppendDate(_))),
        "garbage date string should be rejected per RFC 3501 Section 9: {result:?}"
    );
}

/// RFC 3501 Section 9 paragraph (1): "all alphabetic characters are
/// case-insensitive ... Implementations MUST accept these strings in a
/// case-insensitive fashion." Month names like "JAN", "jan", "jAn" are
/// valid per RFC 5234 Section 2.3 (ABNF strings are case-insensitive).
#[test]
fn test_append_accepts_case_insensitive_month() {
    // Uppercase month
    let result = append_with_date(" 7-JAN-2024 12:00:00 +0000");
    assert!(
        result.is_ok(),
        "uppercase month must be accepted per RFC 3501 Section 9 paragraph (1): {result:?}"
    );

    // Lowercase month
    let result2 = append_with_date("15-jul-2024 12:00:00 +0000");
    assert!(
        result2.is_ok(),
        "lowercase month must be accepted per RFC 3501 Section 9 paragraph (1): {result2:?}"
    );

    // Mixed case month
    let result3 = append_with_date("20-sEp-2024 12:00:00 +0000");
    assert!(
        result3.is_ok(),
        "mixed-case month must be accepted per RFC 3501 Section 9 paragraph (1): {result3:?}"
    );
}

/// Valid date-time strings per RFC 3501 Section 9 must be accepted.
#[test]
fn test_append_accepts_valid_datetime() {
    // Space-padded single-digit day.
    let result = append_with_date(" 7-Jul-1996 02:44:25 -0700");
    assert!(
        result.is_ok(),
        "valid space-padded day should be accepted: {result:?}"
    );

    // Two-digit day.
    let result2 = append_with_date("17-Jul-1996 02:44:25 -0700");
    assert!(
        result2.is_ok(),
        "valid two-digit day should be accepted: {result2:?}"
    );
}

/// RFC 3501 Section 9: the validator must reject semantically invalid time
/// components (hours > 23, minutes > 59, seconds > 60) and accept valid
/// boundary values including leap seconds (60) per RFC 5322 Section 3.3.
#[test]
fn spec_audit_time_range_validation() {
    // Invalid hour: 25 exceeds 00-23 range.
    assert!(
        validate_append_datetime("01-Jan-2024 25:00:00 +0000").is_err(),
        "hour 25 must be rejected  -  valid range is 00-23 (RFC 3501 Section 9)"
    );

    // Invalid minute: 61 exceeds 00-59 range.
    assert!(
        validate_append_datetime("01-Jan-2024 12:61:00 +0000").is_err(),
        "minute 61 must be rejected  -  valid range is 00-59 (RFC 3501 Section 9)"
    );

    // Invalid second: 61 exceeds 00-60 range.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:61 +0000").is_err(),
        "second 61 must be rejected  -  valid range is 00-60 (RFC 3501 Section 9)"
    );

    // Valid leap second: 60 is permitted per RFC 5322 Section 3.3.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:60 +0000").is_ok(),
        "second 60 (leap second) must be accepted per RFC 5322 Section 3.3"
    );

    // Maximum valid time: 23:59:59.
    assert!(
        validate_append_datetime("01-Jan-2024 23:59:59 +0000").is_ok(),
        "23:59:59 is the maximum valid non-leap-second time"
    );

    // Minimum valid time: 00:00:00.
    assert!(
        validate_append_datetime("01-Jan-2024 00:00:00 +0000").is_ok(),
        "00:00:00 is the minimum valid time"
    );
}

/// RFC 3501 Section 9: the validator must reject impossible calendar dates
/// where the day exceeds the maximum for the given month.  February is
/// capped at 29 (we accept Feb 29 universally since a full leap-year check
/// would require parsing the 4-digit year; this is a practical compromise).
/// 30-day months (Apr, Jun, Sep, Nov) must reject day 31.
#[test]
fn spec_audit_day_month_cross_check() {
    // Feb 31  -  impossible, must be rejected.
    assert!(
        validate_append_datetime("31-Feb-2024 00:00:00 +0000").is_err(),
        "31-Feb must be rejected  -  February has at most 29 days (RFC 3501 Section 9)"
    );

    // Feb 30  -  impossible, must be rejected.
    assert!(
        validate_append_datetime("30-Feb-2024 00:00:00 +0000").is_err(),
        "30-Feb must be rejected  -  February has at most 29 days (RFC 3501 Section 9)"
    );

    // Feb 29 in a leap year  -  must be accepted.
    assert!(
        validate_append_datetime("29-Feb-2024 00:00:00 +0000").is_ok(),
        "29-Feb-2024 must be accepted  -  2024 is a leap year (RFC 3501 Section 9)"
    );

    // Feb 29 in a non-leap year  -  must be rejected (Postel's law: be
    // conservative in what you send).
    assert!(
        validate_append_datetime("29-Feb-2023 00:00:00 +0000").is_err(),
        "29-Feb-2023 must be rejected  -  2023 is not a leap year (RFC 3501 Section 9)"
    );

    // Feb 29 in a century year divisible by 400  -  leap year.
    assert!(
        validate_append_datetime("29-Feb-2000 00:00:00 +0000").is_ok(),
        "29-Feb-2000 must be accepted  -  2000 is a leap year (RFC 3501 Section 9)"
    );

    // Feb 29 in a century year NOT divisible by 400  -  not a leap year.
    assert!(
        validate_append_datetime("29-Feb-1900 00:00:00 +0000").is_err(),
        "29-Feb-1900 must be rejected  -  1900 is not a leap year (RFC 3501 Section 9)"
    );

    // Apr 31  -  impossible, must be rejected (April has 30 days).
    assert!(
        validate_append_datetime("31-Apr-2024 00:00:00 +0000").is_err(),
        "31-Apr must be rejected  -  April has at most 30 days (RFC 3501 Section 9)"
    );

    // Jun 31  -  impossible, must be rejected (June has 30 days).
    assert!(
        validate_append_datetime("31-Jun-2024 00:00:00 +0000").is_err(),
        "31-Jun must be rejected  -  June has at most 30 days (RFC 3501 Section 9)"
    );

    // Apr 30  -  valid, must be accepted.
    assert!(
        validate_append_datetime("30-Apr-2024 00:00:00 +0000").is_ok(),
        "30-Apr must be accepted  -  April has 30 days (RFC 3501 Section 9)"
    );

    // Jan 31  -  valid, must be accepted.
    assert!(
        validate_append_datetime("31-Jan-2024 00:00:00 +0000").is_ok(),
        "31-Jan must be accepted  -  January has 31 days (RFC 3501 Section 9)"
    );
}

/// RFC 6855 Section 3: verify `encode_append` uses quoted form
/// for a UTF-8 mailbox name when UTF-8 mode is on.
#[test]
fn spec_audit_multi_append_utf8_mailbox_quoted() {
    let msgs = [append_msg(&[], None, 42)];
    let output = append_wire(
        "A001",
        "日本語",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, true),
    );
    let output = output.as_str();
    // The mailbox should be quoted, not a literal.
    assert!(
        output.contains("APPEND \""),
        "UTF-8 mailbox in MULTIAPPEND must be quoted when UTF8=ACCEPT is active \
         per RFC 6855 Section 3, got: '{output}'"
    );
}

// -----------------------------------------------------------------------
// RFC 7162 Section 7: mod-sequence-value / mod-sequence-valzer validation
// -----------------------------------------------------------------------

/// RFC 7162 Section 7: CHANGEDSINCE uses mod-sequence-value (>= 1).
#[test]
fn encode_fetch_changedsince_rejects_zero() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS".into(),
        changed_since: Some(0),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "CHANGEDSINCE 0 must be rejected per RFC 7162 Section 7"
    );
}

/// RFC 7162 Section 7: mod-sequence-value must be <= 2^63-1.
#[test]
fn encode_fetch_changedsince_rejects_overflow() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS".into(),
        changed_since: Some(u64::MAX),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "CHANGEDSINCE > i64::MAX must be rejected per RFC 7162 Section 7"
    );
}

/// RFC 7162 Section 7: UNCHANGEDSINCE uses mod-sequence-valzer (allows 0).
#[test]
fn encode_store_unchangedsince_allows_zero() {
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: Some(0),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "UNCHANGEDSINCE 0 must be allowed per RFC 7162 Section 7"
    );
}

/// RFC 7162 Section 7: mod-sequence-valzer must be <= 2^63-1.
#[test]
fn encode_store_unchangedsince_rejects_overflow() {
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: Some(u64::MAX),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "UNCHANGEDSINCE > i64::MAX must be rejected per RFC 7162 Section 7"
    );
}

/// RFC 7162 Section 7: UID FETCH CHANGEDSINCE also uses mod-sequence-value.
#[test]
fn encode_uid_fetch_changedsince_rejects_zero() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS".into(),
        changed_since: Some(0),
        vanished: false,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "UID FETCH CHANGEDSINCE 0 must be rejected per RFC 7162 Section 7"
    );
}

/// RFC 7162 Section 7: UID STORE UNCHANGEDSINCE must reject > `i64::MAX`.
#[test]
fn encode_uid_store_unchangedsince_rejects_overflow() {
    let cmd = Command::UidStore {
        sequence_set: SequenceSet::new("1").unwrap(),
        operation: crate::types::StoreOperation::Add,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: Some(u64::MAX),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "UID STORE UNCHANGEDSINCE > i64::MAX must be rejected per RFC 7162 Section 7"
    );
}

/// RFC 7162 Section 7: known-uids / known-sequence-set / known-uid-set
/// must be syntactically valid `sequence-set` (with `*` and `$` disallowed).
#[test]
fn known_sequence_set_accepts_valid() {
    // Simple nz-numbers and ranges
    assert!(SequenceSet::new_known("1").is_ok());
    assert!(SequenceSet::new_known("1:100").is_ok());
    assert!(SequenceSet::new_known("1,2,3").is_ok());
    assert!(SequenceSet::new_known("1:5,10:20").is_ok());
    assert!(SequenceSet::new_known("4294967295").is_ok()); // u32::MAX
}

/// RFC 7162 Section 7: known-uids / known-sequence-set / known-uid-set
/// must reject values that are not syntactically valid `sequence-set`
/// or that contain `*` or `$`.
#[test]
fn known_sequence_set_rejects_invalid() {
    // Empty string  -  not a valid sequence-set
    assert!(SequenceSet::new_known("").is_err());
    // Non-numeric content  -  not valid nz-number
    assert!(SequenceSet::new_known("abc").is_err());
    // Contains spaces  -  not valid sequence-set syntax
    assert!(SequenceSet::new_known("1 2").is_err());
    // Contains wildcard  -  disallowed per RFC 7162 Section 3.2.5.2
    assert!(SequenceSet::new_known("1:*").is_err());
    assert!(SequenceSet::new_known("*").is_err());
    // Contains $ (RFC 5182 search result reference)  -  not meaningful in QRESYNC
    assert!(SequenceSet::new_known("$").is_err());
    assert!(SequenceSet::new_known("1,$").is_err());
    // Standalone zero  -  not a valid nz-number
    assert!(SequenceSet::new_known("0").is_err());
    // Leading zeros  -  not a valid nz-number (digit-nz *DIGIT)
    assert!(SequenceSet::new_known("01").is_err());
    // Overflow  -  exceeds u32::MAX
    assert!(SequenceSet::new_known("4294967296").is_err());
    // Trailing comma  -  produces empty part
    assert!(SequenceSet::new_known("1,").is_err());
    // Double colon  -  produces empty range element
    assert!(SequenceSet::new_known("1::2").is_err());
}

/// RFC 5464 Section 5 / RFC 3501 Section 9: MAXSIZE must fit in `number` (u32).
/// A `max_size` of 0 is valid (edge case but syntactically legal).
#[test]
fn getmetadata_maxsize_zero_accepted() {
    let mut buf = BytesMut::new();
    let result = push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(0),
        None,
        false,
        LiteralMode::Synchronizing,
    );
    assert!(result.is_ok(), "max_size 0 should be accepted");
}

/// RFC 5464 Section 5 / RFC 3501 Section 9: `u32::MAX` is the largest valid `number`.
#[test]
fn getmetadata_maxsize_u32_max_accepted() {
    let mut buf = BytesMut::new();
    let result = push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(u64::from(u32::MAX)),
        None,
        false,
        LiteralMode::Synchronizing,
    );
    assert!(result.is_ok(), "max_size u32::MAX should be accepted");
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("MAXSIZE 4294967295"),
        "should contain u32::MAX value, got: {output}"
    );
}

/// RFC 5464 Section 5 / RFC 3501 Section 9: `u32::MAX` + 1 exceeds `number` range.
#[test]
fn getmetadata_maxsize_u32_max_plus_one_rejected() {
    let mut buf = BytesMut::new();
    let result = push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(u64::from(u32::MAX) + 1),
        None,
        false,
        LiteralMode::Synchronizing,
    );
    assert!(
        result.is_err(),
        "max_size u32::MAX + 1 must be rejected per RFC 5464 Section 5 / RFC 3501 Section 9"
    );
}

/// RFC 5464 Section 5 / RFC 3501 Section 9: `u64::MAX` far exceeds `number` range.
#[test]
fn getmetadata_maxsize_u64_max_rejected() {
    let mut buf = BytesMut::new();
    let result = push_getmetadata(
        &mut buf,
        "A001",
        "INBOX",
        &["/private/comment".to_owned()],
        Some(u64::MAX),
        None,
        false,
        LiteralMode::Synchronizing,
    );
    assert!(
        result.is_err(),
        "max_size u64::MAX must be rejected per RFC 5464 Section 5 / RFC 3501 Section 9"
    );
}

/// RFC 7162 Section 3.2.5.2: QRESYNC `mod_seq` is mod-sequence-value (>= 1).
#[test]
fn encode_select_qresync_rejects_zero_modseq() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("INBOX").unwrap(),
        condstore: false,
        qresync: Some(crate::types::QresyncParams {
            uid_validity: 1,
            mod_seq: 0,
            known_uids: None,
            seq_match_data: None,
        }),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "QRESYNC mod_seq 0 must be rejected per RFC 7162 Section 7"
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

// ── Pre-refactor lock-down: LIST/LSUB/RENAME with non-ASCII names ──

#[test]
fn encode_list_non_ascii_reference() {
    // RFC 3501 Section 5.1.3: non-ASCII mailbox names are MUTF-7 encoded
    // when utf8 is false. "café" -> "caf&AOk-" (all ASCII, fits in quoted).
    let mut buf = BytesMut::new();
    let cmd = Command::List {
        reference: "café".into(),
        pattern: "*".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LIST \"caf&AOk-\" \"*\"\r\n");
}

#[test]
fn encode_lsub_non_ascii_pattern() {
    // RFC 3501 Section 5.1.3: non-ASCII mailbox names are MUTF-7 encoded
    // when utf8 is false. "日本語" -> "&ZeVnLIqe-" (all ASCII, fits in quoted).
    let mut buf = BytesMut::new();
    let cmd = Command::Lsub {
        reference: String::new().into(),
        pattern: "日本語".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LSUB \"\" \"&ZeVnLIqe-\"\r\n");
}

#[test]
fn encode_rename_non_ascii_both_args() {
    // RFC 3501 Section 6.3.5 / RFC 6855: both old and new names non-ASCII.
    // encode_mailbox_name converts to modified UTF-7 when utf8=false.
    let mut buf = BytesMut::new();
    let cmd = Command::Rename {
        mailbox: MailboxName::new("Ünread").unwrap(),
        new_name: MailboxName::new("Gelöscht").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // "Ünread" -> "&ANw-nread", "Gelöscht" -> "Gel&APY-scht" in modified UTF-7
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.starts_with("A001 RENAME "),
        "command prefix mismatch: {output}"
    );
    // Both names should be ASCII after UTF-7 encoding -> quoted strings
    assert!(
        !output.contains('{'),
        "modified UTF-7 encoded names should not need literals: {output}"
    );
}

#[test]
fn encode_list_empty_reference_percent_pattern() {
    // RFC 3501 Section 6.3.8: % matches one level of hierarchy.
    let mut buf = BytesMut::new();
    let cmd = Command::List {
        reference: String::new().into(),
        pattern: "%".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LIST \"\" \"%\"\r\n");
}

#[test]
fn encode_lsub_with_reference_and_pattern() {
    // RFC 3501 Section 6.3.9: LSUB with non-empty reference.
    let mut buf = BytesMut::new();
    let cmd = Command::Lsub {
        reference: "INBOX.".into(),
        pattern: "*".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 LSUB \"INBOX.\" \"*\"\r\n");
}

// ---------------------------------------------------------------
// Pre-refactor coverage: sequence-set validation via SequenceSet newtype
// RFC 3501 Section 9: sequence-set = (seq-number / seq-range) *("," ...)
// where seq-number = nz-number / "*", nz-number = digit-nz *DIGIT.
// "0", "abc", and "" are all invalid per the ABNF and are rejected at
// SequenceSet construction time.
// ---------------------------------------------------------------

/// RFC 3501 Section 9: invalid sequence sets are rejected at construction.
#[test]
fn sequence_set_newtype_rejects_invalid_values() {
    // "0"  -  nz-number does not allow zero
    assert!(SequenceSet::new("0").is_err());
    // "abc"  -  not a valid nz-number or seq-range
    assert!(SequenceSet::new("abc").is_err());
    // ""  -  empty string is not a valid sequence-set
    assert!(SequenceSet::new("").is_err());
}

// ---------------------------------------------------------------
// Pre-refactor coverage: STORE with CONDSTORE (typical value)
// RFC 7162 Section 3.1.3: UNCHANGEDSINCE mod-sequence-valzer
// Existing tests cover edge cases (0, u64::MAX); this covers a
// representative mid-range value.
// ---------------------------------------------------------------

/// RFC 7162 Section 3.1.3: non-UID STORE with CONDSTORE modifier at a
/// typical mod-sequence-valzer value (12345). The existing test covers
/// UID STORE; this covers the non-UID variant.
#[test]
fn encode_non_uid_store_with_condstore_typical_value() {
    let mut buf = BytesMut::new();
    let cmd = Command::Store {
        sequence_set: SequenceSet::new("1:3").unwrap(),
        operation: crate::types::StoreOperation::Remove,
        flags: vec![crate::types::Flag::Seen],
        unchanged_since: Some(12345),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 STORE 1:3 (UNCHANGEDSINCE 12345) -FLAGS (\\Seen)\r\n"
    );
}

// ---------------------------------------------------------------
// Pre-refactor coverage: COPY/MOVE with special mailbox names
// RFC 3501 Section 9: quoted strings that contain characters
// requiring escaping, or non-ASCII bytes that force literal form.
// ---------------------------------------------------------------

/// RFC 3501 Section 6.4.7 / Section 9: COPY with a mailbox name
/// containing a double-quote, which must be escaped in the quoted
/// string form (backslash-quote per quoted-specials).
#[test]
fn encode_copy_with_special_char_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::Copy {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("folder\"name").unwrap(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // Double-quote is ASCII and quotable, so it gets backslash-escaped
    // in quoted form per RFC 3501 Section 9 (quoted-specials).
    assert_eq!(
        &buf[..],
        b"A001 COPY 1:5 \"folder\\\"name\"\r\n",
        "mailbox with double-quote should use quoted form with escaping"
    );
}

/// RFC 6851 / RFC 3501 Section 9: UID MOVE with a non-ASCII mailbox
/// name forces literal encoding (bytes > 0x7F are not TEXT-CHAR per
/// RFC 3501 Section 9, so they cannot appear in a quoted string).
#[test]
fn encode_uid_move_with_non_ascii_mailbox() {
    let mut buf = BytesMut::new();
    let cmd = Command::UidMove {
        sequence_set: SequenceSet::new("1:5").unwrap(),
        mailbox: MailboxName::new("caf\u{00E9}").unwrap(), // "café"  -  5 bytes in UTF-8
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // encode_mailbox_name converts to modified UTF-7 when utf8=false:
    // "café" -> "caf&AOk-" (ASCII), which then gets quoted.
    assert_eq!(
        &buf[..],
        b"A001 UID MOVE 1:5 \"caf&AOk-\"\r\n",
        "non-ASCII mailbox must be modified UTF-7 encoded on the wire"
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

// ── RFC 9051 Section 9: literal8 must not use non-synchronizing `+` suffix ──

/// RFC 9051 Section 9: `literal8 = "~{" number64 "}" CRLF *OCTET`  -  no `["+"]`.
/// When UTF8=ACCEPT is active on a server with LITERAL+ but WITHOUT BINARY,
/// the encoder must produce `~{N}\r\n`, never `~{N+}\r\n` (RFC 7888 Section 6
/// makes the `+` form conditional on BINARY). The decoder already rejects
/// `~{N+}`  -  the encoder must match.
#[test]
fn literal8_must_not_use_non_sync_plus() {
    let msgs = [append_msg(&[], None, 100)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::LiteralPlus, true), // UTF8=ACCEPT, LITERAL+, no BINARY
    );
    let output = output.as_str();
    // Must NOT contain `+}`  -  literal8 doesn't allow non-synchronizing form.
    assert!(
        !output.contains("+}"),
        "literal8 must not contain non-synchronizing '+' suffix per RFC 9051 Section 9; got: {output}"
    );
    // Must contain the synchronizing literal8 form `~{100}\r\n`.
    assert!(
        output.contains("~{100}\r\n"),
        "literal8 must use synchronizing form ~{{100}} per RFC 9051 Section 9; got: {output}"
    );
}

/// RFC 7888 Section 6: BINARY together with LITERAL+ makes the `UTF8 (~{N+}`
/// form legal on `IMAP4rev1`, and the encoder uses it (the pre-refactor
/// handle-side path did too, by patching the header afterwards).
#[test]
fn utf8_wrapped_literal8_gets_plus_only_with_binary() {
    let msgs = [append_msg(&[], None, 100)];
    let mut o = append_opts(LiteralMode::LiteralPlus, true);
    o.capabilities.push(Capability::Binary);
    assert_eq!(
        append_wire("A001", "INBOX", &msgs, false, &o),
        "A001 APPEND \"INBOX\" UTF8 (~{100+}\r\n<100>)\r\n"
    );
}

/// RFC 9051 Section 9: when UTF8=ACCEPT is NOT active, LITERAL+ `{N+}` is fine.
/// This test ensures we don't regress the normal LITERAL+ path.
#[test]
fn regular_literal_plus_still_works_without_utf8() {
    let msgs = [append_msg(&[], None, 200)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::LiteralPlus, false),
    );
    let output = output.as_str();
    // Regular literal with LITERAL+ must contain `{200+}\r\n`.
    assert!(
        output.contains("{200+}\r\n"),
        "regular literal with LITERAL+ must use non-synchronizing form {{200+}}; got: {output}"
    );
}

/// RFC 9051 Section 9: when neither LITERAL+ nor UTF8=ACCEPT, produce `{N}\r\n`.
#[test]
fn synchronizing_literal_without_utf8() {
    let msgs = [append_msg(&[], None, 300)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        false,
        &append_opts(LiteralMode::Synchronizing, false), // no literal extension
    );
    let output = output.as_str();
    // Must contain synchronizing literal `{300}\r\n`, no `+` or `~`.
    assert!(
        output.contains("{300}\r\n"),
        "synchronizing literal must use {{300}}; got: {output}"
    );
    assert!(
        !output.contains('~'),
        "non-UTF8 literal must not contain literal8 prefix '~'; got: {output}"
    );
}

// -----------------------------------------------------------------------
// date-day-fixed fallthrough (RFC 3501 Section 9)
// -----------------------------------------------------------------------

/// RFC 3501 Section 9: date-day-fixed = (SP DIGIT) / 2DIGIT.
/// A day whose first byte is outside ' ', '0'-'3' must be rejected
/// (the `_ => false` fallthrough in the day validator).
#[test]
fn validate_datetime_rejects_invalid_day_high_digit() {
    // Day "40"  -  first byte '4' falls into the _ => false branch.
    let result = validate_append_datetime("40-Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "day '40' must be rejected  -  first byte '4' is not in SP/'0'-'3' \
         per RFC 3501 Section 9 date-day-fixed"
    );
}

/// RFC 3501 Section 9: date-day-fixed rejects a first byte that is a
/// non-digit, non-space character (e.g., 'X').
#[test]
fn validate_datetime_rejects_non_digit_day() {
    let result = validate_append_datetime("X1-Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "day 'X1' must be rejected  -  first byte 'X' is not valid \
         per RFC 3501 Section 9 date-day-fixed"
    );
}

/// Day starting with '9' also hits the fallthrough branch.
#[test]
fn validate_datetime_rejects_day_starting_with_nine() {
    let result = validate_append_datetime("91-Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "day '91' must be rejected  -  first byte '9' falls through \
         per RFC 3501 Section 9 date-day-fixed"
    );
}

// -----------------------------------------------------------------------
// Date separator and field error paths (RFC 3501 Section 9)
// -----------------------------------------------------------------------

/// RFC 3501 Section 9: the first separator at position 2 must be '-'.
#[test]
fn validate_datetime_bad_separator_at_position_2() {
    // Replace '-' at position 2 with '/'.
    let result = validate_append_datetime("01/Jan-2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "non '-' at position 2 must be rejected per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("position 2"),
        "error should mention position 2, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: the second separator at position 6 must be '-'.
#[test]
fn validate_datetime_bad_separator_at_position_6() {
    // Replace '-' at position 6 with '/'.
    let result = validate_append_datetime("01-Jan/2024 12:00:00 +0000");
    assert!(
        result.is_err(),
        "non '-' at position 6 must be rejected per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("position 6"),
        "error should mention position 6, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: date-year must be 4DIGIT.
#[test]
fn validate_datetime_invalid_year() {
    // Replace year digits with non-digits.
    let result = validate_append_datetime("01-Jan-ABCD 12:00:00 +0000");
    assert!(
        result.is_err(),
        "non-digit year 'ABCD' must be rejected per RFC 3501 Section 9 date-year = 4DIGIT"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("year"),
        "error should mention year, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: there must be a SP at position 11 (between date and time).
#[test]
fn validate_datetime_bad_separator_at_position_11() {
    // Replace SP at position 11 with 'X'.
    let result = validate_append_datetime("01-Jan-2024X12:00:00 +0000");
    assert!(
        result.is_err(),
        "non SP at position 11 must be rejected per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("position 11"),
        "error should mention position 11, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: time must be 2DIGIT ":" 2DIGIT ":" 2DIGIT.
#[test]
fn validate_datetime_invalid_time_format() {
    // Replace colons in time with dashes.
    let result = validate_append_datetime("01-Jan-2024 12-00-00 +0000");
    assert!(
        result.is_err(),
        "time 'HH-MM-SS' must be rejected  -  colons are required per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("time"),
        "error should mention time, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: time with non-digit characters must be rejected.
#[test]
fn validate_datetime_invalid_time_non_digit() {
    let result = validate_append_datetime("01-Jan-2024 AB:CD:EF +0000");
    assert!(
        result.is_err(),
        "non-digit time must be rejected per RFC 3501 Section 9 time = 2DIGIT \":\" 2DIGIT \":\" 2DIGIT"
    );
}

/// RFC 3501 Section 9: there must be a SP at position 20 (between time and zone).
#[test]
fn validate_datetime_bad_separator_at_position_20() {
    // Replace SP at position 20 with 'X'.
    let result = validate_append_datetime("01-Jan-2024 12:00:00X+0000");
    assert!(
        result.is_err(),
        "non SP at position 20 must be rejected per RFC 3501 Section 9"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("position 20"),
        "error should mention position 20, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: zone must be ("+" / "-") 4DIGIT.
#[test]
fn validate_datetime_invalid_timezone_format() {
    // Zone without +/- prefix.
    let result = validate_append_datetime("01-Jan-2024 12:00:00 X0000");
    assert!(
        result.is_err(),
        "zone without +/- prefix must be rejected per RFC 3501 Section 9 zone format"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("zone"),
        "error should mention zone, got: {err_msg}"
    );
}

/// RFC 3501 Section 9: zone with non-digit HHMM must be rejected.
#[test]
fn validate_datetime_invalid_timezone_non_digit() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +ABCD");
    assert!(
        result.is_err(),
        "zone with non-digit HHMM must be rejected per RFC 3501 Section 9"
    );
}

/// RFC 3501 Section 9 / RFC 5322 Section 4.3: timezone hour > 14 must be
/// rejected (maximum real UTC offset is +14:00).
#[test]
fn validate_datetime_timezone_hour_exceeds_14() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +1500");
    assert!(
        result.is_err(),
        "timezone hour 15 must be rejected  -  maximum is 14 (RFC 3501 Section 9)"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("zone hour"),
        "error should mention zone hour, got: {err_msg}"
    );
}

/// Boundary: timezone hour 14 is the maximum valid value and must be accepted.
#[test]
fn validate_datetime_timezone_hour_14_is_valid() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +1400");
    assert!(
        result.is_ok(),
        "timezone hour 14 must be accepted  -  it is the maximum valid offset: {result:?}"
    );
}

/// RFC 3501 Section 9: timezone minute > 59 must be rejected.
#[test]
fn validate_datetime_timezone_minute_exceeds_59() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +0060");
    assert!(
        result.is_err(),
        "timezone minute 60 must be rejected  -  maximum is 59 (RFC 3501 Section 9)"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("zone minute"),
        "error should mention zone minute, got: {err_msg}"
    );
}

/// Boundary: timezone minute 59 is the maximum valid value and must be accepted.
#[test]
fn validate_datetime_timezone_minute_59_is_valid() {
    let result = validate_append_datetime("01-Jan-2024 12:00:00 +0059");
    assert!(
        result.is_ok(),
        "timezone minute 59 must be accepted  -  it is the maximum valid value: {result:?}"
    );
}

/// RFC 3501 Section 9: `zone = ("+" / "-") 4DIGIT`  -  any hour 0-14
/// with any minute 0-59 is valid.  +1400 (Line Islands, Kiribati)
/// and offsets with non-zero minutes at hour 14 must all be accepted.
#[test]
fn validate_datetime_timezone_hour_14_allows_any_minute() {
    // +1400  -  valid (Line Islands, Kiribati).
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 +1400").is_ok(),
        "+1400 must be accepted  -  it is the maximum real UTC offset"
    );
    // +1430  -  valid per RFC 3501 Section 9 grammar.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 +1430").is_ok(),
        "+1430 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
    // -1400  -  valid symmetric boundary.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 -1400").is_ok(),
        "-1400 must be accepted  -  symmetric with +1400"
    );
    // -1430  -  valid per RFC 3501 Section 9 grammar.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 -1430").is_ok(),
        "-1430 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
}

/// RFC 3501 Section 9: `zone = ("+" / "-") 4DIGIT` has no semantic
/// constraint on the digit values.  The validation must allow any
/// hour 0-14 with any minute 0-59.  In particular, +1430 and +1445
/// must be accepted: although no current IANA timezone uses these
/// exact offsets at hour 14, the RFC grammar imposes no such
/// restriction, and future timezone changes could introduce them.
/// Real-world sub-hour offsets like +1245 (Chatham Islands standard)
/// and +1345 (Chatham Islands DST) demonstrate that non-zero minutes
/// with high hours are legitimate.
#[test]
fn validate_datetime_timezone_hour_14_with_nonzero_minutes() {
    // +1430  -  must be accepted per RFC 3501 Section 9 grammar.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 +1430").is_ok(),
        "+1430 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
    // +1445  -  must be accepted per RFC 3501 Section 9 grammar.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 +1445").is_ok(),
        "+1445 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
    // -1430  -  negative symmetric boundary must also be accepted.
    assert!(
        validate_append_datetime("01-Jan-2024 12:00:00 -1430").is_ok(),
        "-1430 must be accepted  -  RFC 3501 Section 9 zone grammar allows any 4DIGIT"
    );
}

// -----------------------------------------------------------------------
// Non-UTF8 output formatting via from_utf8_lossy
// -----------------------------------------------------------------------

/// Verify that `String::from_utf8_lossy` correctly handles encoded output
/// containing non-UTF8 bytes (e.g., high bytes in literal8 values).
/// This exercises the lossy conversion path used in debug/display formatting.
///
/// RFC 5464 Section 5 / RFC 3516: literal8 values can contain arbitrary
/// octets that are not valid UTF-8.
#[test]
fn from_utf8_lossy_handles_non_utf8_encoded_output() {
    let mut buf = BytesMut::new();
    // Encode a SETMETADATA command with high-byte (non-UTF8) value.
    let value = b"\x80\x81\xfe\xff".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = &buf[..];

    // The output contains non-UTF8 bytes; from_utf8_lossy must produce
    // a valid string with U+FFFD replacement characters.
    let lossy = String::from_utf8_lossy(output);
    assert!(
        lossy.contains('\u{FFFD}'),
        "from_utf8_lossy must replace non-UTF8 bytes with U+FFFD; got: {lossy}"
    );
    // The lossy string must still contain the recognizable ASCII parts.
    assert!(
        lossy.contains("SETMETADATA"),
        "lossy output must preserve ASCII command keyword; got: {lossy}"
    );
    assert!(
        lossy.contains("INBOX"),
        "lossy output must preserve ASCII mailbox name; got: {lossy}"
    );
}

/// Verify `from_utf8_lossy` with NUL bytes in encoded output.
/// NUL bytes (0x00) are valid UTF-8 but exercise the binary path in
/// `encode_metadata_value` (literal8 syntax).
///
/// RFC 3516: literal8 = "~{" number "}" CRLF *OCTET
#[test]
fn from_utf8_lossy_with_nul_bytes_in_output() {
    let mut buf = BytesMut::new();
    let value = b"\x00\x00".to_vec();
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(value))],
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = &buf[..];

    // NUL bytes are valid UTF-8, so from_utf8_lossy should not replace them.
    let lossy = String::from_utf8_lossy(output);
    assert!(
        lossy.contains("SETMETADATA"),
        "lossy output must preserve command keyword even with NUL bytes; got: {lossy}"
    );
    // NUL bytes are preserved (they are valid single-byte UTF-8 codepoints).
    let nul_count = output
        .iter()
        .fold(0usize, |acc, &b| acc + usize::from(b == 0x00));
    assert_eq!(
        nul_count, 2,
        "both NUL bytes must be preserved in the encoded output"
    );
}

// -----------------------------------------------------------------------
// LITERAL+ (RFC 7888) propagation through encode_command
// -----------------------------------------------------------------------

/// RFC 7888 Section 4: when the server advertises LITERAL+, the client
/// SHOULD use the non-synchronizing literal form `{N+}\r\n` instead of
/// the synchronizing `{N}\r\n`. A LOGIN password containing CRLF is a
/// legal `astring` that cannot be quoted, so it forces the literal form.
#[test]
fn encode_command_login_literal_plus() {
    let cmd = Command::Login {
        user: "alice".into(),
        pass: "pass\r\nword".into(),
    };
    let mut buf = BytesMut::new();
    encode_command_to_buf(
        &mut buf,
        "A001",
        &cmd,
        &opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("{10+}\r\n"),
        "with literal_plus=true, literal must use non-synchronizing form {{N+}}; got: {output}"
    );
}

/// When `literal_plus` is false, the encoder must use synchronizing
/// literals `{N}\r\n` (RFC 3501 Section 4.3).
#[test]
fn encode_command_login_synchronizing_literal() {
    let mut buf = BytesMut::new();
    let cmd = Command::Login {
        user: "alice".into(),
        pass: "pass\r\nword".into(),
    };
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("{10}\r\n"),
        "with literal_plus=false, literal must use synchronizing form {{N}}; got: {output}"
    );
    assert!(
        !output.contains("{10+}"),
        "with literal_plus=false, literal must NOT use non-synchronizing form; got: {output}"
    );
}

// -----------------------------------------------------------------------
// Synchronizing literal segmentation (RFC 3501 Section 4.3)
// -----------------------------------------------------------------------

/// RFC 3501 Section 4.3 splits a synchronizing literal without relying on an
/// invalid LOGIN credential as the literal source.
#[test]
fn synchronizing_literal_splits_into_segments() {
    let encoded = x_command(LiteralMode::Synchronizing, &[b"pass\r\nword"]);
    let segments = segment_bytes(&encoded);

    // Must have exactly 2 segments: header+marker, then literal body+CRLF.
    assert_eq!(
        segments.len(),
        2,
        "a synchronizing literal must produce 2 segments \
         (RFC 3501 Section 4.3); got {} segment(s): {:?}",
        segments.len(),
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );

    // Segment 0: command prefix + literal marker `{10}\r\n`.
    let seg0 = std::str::from_utf8(&segments[0]).unwrap();
    assert!(
        seg0.starts_with("A001 X {10}\r\n"),
        "first segment should be the command prefix ending with {{10}}\\r\\n; \
         got: {seg0:?}"
    );

    // Segment 1: literal body (10 bytes = "pass\r\nword") + command CRLF.
    assert_eq!(
        &segments[1][..],
        b"pass\r\nword\r\n",
        "second segment should be the literal body followed by command CRLF"
    );
}

/// Non-synchronizing markers stay in one segment because they require no `+`
/// continuation pause (RFC 7888 Section 4).
#[test]
fn literal_plus_marker_stays_in_one_segment() {
    let encoded = x_command(LiteralMode::LiteralPlus, &[b"pass\r\nword"]);
    assert_eq!(encoded.to_vec(), b"A001 X {10+}\r\npass\r\nword\r\n");
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        1,
        "a non-synchronizing literal must be a single segment \
         (RFC 7888 Section 4); got {} segments",
        segments.len()
    );
}

/// RFC 3501 Section 4.3: each synchronizing literal requires a separate
/// continuation exchange.
#[test]
fn two_synchronizing_literals_produce_three_segments() {
    let encoded = x_command(LiteralMode::Synchronizing, &[b"us\r\ner", b"pass\r\nword"]);
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        3,
        "two synchronizing literals must produce 3 segments \
         (RFC 3501 Section 4.3); got {} segment(s): {:?}",
        segments.len(),
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );

    // Segment 0: command prefix + first literal marker.
    let seg0 = std::str::from_utf8(&segments[0]).unwrap();
    assert!(
        seg0.ends_with("{6}\r\n"),
        "first segment should end with the user literal marker {{6}}\\r\\n; \
         got: {seg0:?}"
    );

    // Segment 1: first literal body + space + second literal marker.
    let seg1_bytes = &segments[1][..];
    // First 6 bytes: literal body "us\r\ner"
    assert_eq!(
        &seg1_bytes[..6],
        b"us\r\ner",
        "second segment should start with the user literal body"
    );
    // Then " {10}\r\n"
    let seg1_rest = std::str::from_utf8(&seg1_bytes[6..]).unwrap();
    assert_eq!(
        seg1_rest, " {10}\r\n",
        "second segment should end with space + password literal marker"
    );

    // Segment 2: second literal body + command CRLF.
    assert_eq!(
        &segments[2][..],
        b"pass\r\nword\r\n",
        "third segment should be the password literal body + command CRLF"
    );
}

/// SETMETADATA with a binary value produces literal8 (`~{N}\r\n`). Without a
/// literal extension it is synchronizing, so the command must split at the
/// literal8 marker and the sender wait for `+` before the body.
///
/// RFC 3516 Section 4 / RFC 9051 Section 9:
///   `literal8 = "~{" number64 "}" CRLF *OCTET`
///
/// Regression: a send-side scanner once skipped `~{N}\r\n` markers, sending
/// the whole command as one segment without waiting for the continuation.
#[test]
fn regression_setmetadata_literal8_produces_two_segments_without_literal_plus() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/binary".into(), Some(b"\x00\x01\x02".to_vec()))],
    };
    // literal_plus=false: literal8 must be synchronizing.
    let encoded = encode_command("A001", &cmd, &default_opts()).unwrap();
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        2,
        "SETMETADATA with literal8 and literal_plus=false must produce 2 segments \
         (RFC 3516 Section 4 / RFC 9051 Section 9: literal8 is synchronizing); \
         got {} segment(s): {:?}",
        segments.len(),
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );

    // Segment 0: command prefix ending with ~{3}\r\n
    let seg0 = &segments[0];
    assert!(
        seg0.ends_with(b"~{3}\r\n"),
        "first segment should end with literal8 marker ~{{3}}\\r\\n; \
         got: {:?}",
        String::from_utf8_lossy(seg0)
    );

    // Segment 1: literal body (3 bytes) + closing paren + CRLF
    assert!(
        segments[1].starts_with(b"\x00\x01\x02"),
        "second segment should start with the literal8 body"
    );
}

/// Commands without literals produce a single segment regardless of
/// `literal_plus` setting (RFC 3501 Section 4.3).
#[test]
fn encode_command_no_literal_single_segment() {
    let cmd = Command::Login {
        user: "alice".into(),
        pass: "secret".into(),
    };
    let encoded = encode_command("A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        encoded.segments().len(),
        1,
        "command without literals should be a single segment"
    );
    assert_eq!(
        &encoded.to_vec()[..],
        b"A001 LOGIN \"alice\" \"secret\"\r\n"
    );
}

// -----------------------------------------------------------------------
// CommandWriter: the one place literal framing is decided
//
// Ported from the tests of the deleted send-path patchers
// (`patch_literals_to_plus_with_binary`, `patch_small_literals_to_plus_with_binary`,
// `find_literal_boundary`): the same rules, now pinned where they live.
// -----------------------------------------------------------------------

/// Marker-shaped bytes inside a literal payload are payload. The writer
/// records the boundary of the literal it writes and never scans anything, so
/// a synchronizing payload holding `{3}\r\n` still yields exactly one split.
#[test]
fn writer_payload_holding_a_marker_is_not_framing() {
    let segments = segment_bytes(&x_command(LiteralMode::Synchronizing, &[b"abc{3}\r\ndef"]));
    assert_eq!(
        segments,
        vec![b"A001 X {11}\r\n".to_vec(), b"abc{3}\r\ndef\r\n".to_vec()]
    );
}

/// RFC 7888 Section 4: LITERAL+ makes every classic literal non-synchronizing.
#[test]
fn writer_literal_plus_marks_every_classic_literal() {
    let encoded = x_command(LiteralMode::LiteralPlus, &[b"alice\r\n", b"bob\r\n"]);
    assert_eq!(encoded.segments().len(), 1);
    assert_eq!(
        encoded.to_vec(),
        b"A001 X {7+}\r\nalice\r\n {5+}\r\nbob\r\n\r\n"
    );
}

/// RFC 7888 Section 5: LITERAL- is non-synchronizing up to 4096 octets and
/// synchronizing above.
#[test]
fn writer_literal_minus_boundary_is_4096_octets() {
    let at = vec![b'x'; 4096];
    let encoded = x_command(LiteralMode::LiteralMinus, &[&at]);
    assert_eq!(encoded.segments().len(), 1);
    assert!(encoded.to_vec().starts_with(b"A001 X {4096+}\r\n"));

    let over = vec![b'x'; 4097];
    let segments = segment_bytes(&x_command(LiteralMode::LiteralMinus, &[&over]));
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0], b"A001 X {4097}\r\n");
}

/// RFC 7888 Section 6 / RFC 9051 Section 9: a `~{n}` literal8 may take `+`
/// only with an advertised BINARY on a non-rev2 connection, and only where the
/// literal mode would allow it for a classic literal.
#[test]
fn writer_literal8_non_synchronizing_needs_binary_and_rev1() {
    let literal8 = |opts: &EncodeOptions| {
        written_segments(opts, |w| {
            w.raw(b"A001 X ");
            w.literal(b"abc", LiteralForm::Literal8);
            w.raw(b"\r\n");
        })
    };

    // LITERAL+ without BINARY: synchronizing, split at the marker.
    let no_binary = opts(LiteralMode::LiteralPlus, false);
    assert!(!no_binary.capabilities.contains(&Capability::Binary));
    assert_eq!(
        literal8(&no_binary),
        vec![b"A001 X ~{3}\r\n".to_vec(), b"abc\r\n".to_vec()]
    );

    // LITERAL+ with BINARY on rev1: non-synchronizing.
    let mut binary = opts(LiteralMode::LiteralPlus, false);
    binary.capabilities.push(Capability::Binary);
    assert_eq!(literal8(&binary), vec![b"A001 X ~{3+}\r\nabc\r\n".to_vec()]);

    // LITERAL- with BINARY: small literal8 is non-synchronizing too.
    let mut minus_binary = opts(LiteralMode::LiteralMinus, false);
    minus_binary.capabilities.push(Capability::Binary);
    assert_eq!(
        literal8(&minus_binary),
        vec![b"A001 X ~{3+}\r\nabc\r\n".to_vec()]
    );

    // BINARY on an active rev2 connection: rev2 literal8 has no `+` form.
    let rev2 = EncodeOptions {
        utf8_mode: true,
        literal_mode: LiteralMode::LiteralPlus,
        capabilities: vec![Capability::Imap4Rev2, Capability::Binary],
        enabled: Vec::new(),
    };
    assert_eq!(
        literal8(&rev2),
        vec![b"A001 X ~{3}\r\n".to_vec(), b"abc\r\n".to_vec()]
    );
}

/// A zero-length synchronizing literal still ends its segment on the marker.
#[test]
fn writer_zero_length_literal_is_its_own_boundary() {
    let segments = segment_bytes(&x_command(LiteralMode::Synchronizing, &[b""]));
    assert_eq!(segments, vec![b"A001 X {0}\r\n".to_vec(), b"\r\n".to_vec()]);
}

/// A payload handed over as `Bytes` is carried by reference: the chunk in the
/// encoded command is the caller's own allocation.
#[test]
fn writer_literal_bytes_carries_the_payload_by_reference() {
    let body = bytes::Bytes::from(vec![b'm'; 64]);
    let mut w = CommandWriter::new(&opts(LiteralMode::LiteralPlus, false));
    w.raw(b"A001 X ");
    w.literal_bytes(body.clone(), LiteralForm::Classic);
    w.raw(b"\r\n");
    let encoded = w.finish();
    assert!(
        encoded
            .segments()
            .iter()
            .flatten()
            .any(|chunk| chunk.as_ptr() == body.as_ptr() && chunk.len() == body.len()),
        "the body must be the caller's allocation, not a copy"
    );
}

/// `WireCommand`'s Debug prints the shape only: a command can carry a message
/// body or a credential literal, and the driver traces commands.
#[test]
fn wire_command_debug_never_prints_payload() {
    let encoded = x_command(LiteralMode::LiteralPlus, &[b"hunter2"]);
    let debug = format!("{encoded:?}");
    assert!(!debug.contains("hunter2"), "{debug}");
}

// -----------------------------------------------------------------------
// Caller-written literals in SEARCH-family criteria
// -----------------------------------------------------------------------

fn uid_search(criteria: &str, opts: &EncodeOptions) -> Result<WireCommand, crate::Error> {
    encode_command(
        "A001",
        &Command::UidSearch {
            criteria: criteria.to_owned(),
        },
        opts,
    )
}

/// A caller's synchronizing literal is re-emitted through the writer: under
/// LITERAL+ it takes the `+` form and needs no continuation, as the patcher
/// used to make it.
#[test]
fn criteria_literal_takes_the_live_marker_form() {
    let encoded = uid_search(
        "SUBJECT {5}\r\nhello",
        &opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    assert_eq!(encoded.segments().len(), 1);
    assert_eq!(
        encoded.to_vec(),
        b"A001 UID SEARCH SUBJECT {5+}\r\nhello\r\n"
    );
}

/// Without a literal extension the caller's literal is a real segment
/// boundary: the sender waits for `+` before its payload.
#[test]
fn criteria_literal_is_a_segment_boundary_when_synchronizing() {
    let encoded = uid_search("SUBJECT {5}\r\nhello", &default_opts()).unwrap();
    assert_eq!(
        segment_bytes(&encoded),
        vec![
            b"A001 UID SEARCH SUBJECT {5}\r\n".to_vec(),
            b"hello\r\n".to_vec()
        ]
    );
}

/// Marker-shaped bytes inside a caller literal's payload stay payload: the
/// walk skips the payload by its declared length.
#[test]
fn criteria_literal_payload_holding_a_marker_is_not_framing() {
    let encoded = uid_search("SUBJECT {7}\r\n{9}\r\nab", &default_opts()).unwrap();
    assert_eq!(
        segment_bytes(&encoded),
        vec![
            b"A001 UID SEARCH SUBJECT {7}\r\n".to_vec(),
            b"{9}\r\nab\r\n".to_vec()
        ]
    );
}

/// A caller literal8 is re-emitted as literal8, so the writer applies the
/// literal8 rule: `~{1+}` the connection may not send non-synchronizing
/// loses its `+` and becomes a real boundary, rather than being written with
/// no grant.
#[test]
fn criteria_literal8_follows_the_literal8_rule() {
    let no_binary = opts(LiteralMode::LiteralPlus, false);
    let encoded = uid_search("BODY ~{1+}\r\nx", &no_binary).unwrap();
    assert_eq!(
        segment_bytes(&encoded),
        vec![b"A001 UID SEARCH BODY ~{1}\r\n".to_vec(), b"x\r\n".to_vec()]
    );

    let mut binary = opts(LiteralMode::LiteralPlus, false);
    binary.capabilities.push(Capability::Binary);
    let encoded = uid_search("BODY ~{1}\r\nx", &binary).unwrap();
    assert_eq!(encoded.to_vec(), b"A001 UID SEARCH BODY ~{1+}\r\nx\r\n");
}

/// A count written with leading zeros is re-emitted canonically.
#[test]
fn criteria_literal_count_is_written_canonically() {
    let encoded = uid_search(
        "SUBJECT {0001}\r\nx",
        &opts(LiteralMode::LiteralPlus, false),
    )
    .unwrap();
    assert_eq!(encoded.to_vec(), b"A001 UID SEARCH SUBJECT {1+}\r\nx\r\n");
}

/// The validator judges the caller's ORIGINAL marker: a `{n+}` the connection
/// cannot take is refused, never silently rewritten to `{n}`.
#[test]
fn criteria_non_synchronizing_literal_without_an_extension_is_refused() {
    let err = uid_search("SUBJECT {5+}\r\nhello", &default_opts()).unwrap_err();
    assert!(matches!(err, crate::Error::MissingCapability(_)), "{err:?}");
}

#[test]
fn out_of_range_literal_count_is_not_a_marker_for_the_shared_parser() {
    use crate::connection::literals::{LiteralMarker, literal_marker_at};

    assert!(matches!(
        literal_marker_at(b"{9223372036854775807}\r\n", 0),
        LiteralMarker::Counted {
            size: 9_223_372_036_854_775_807,
            ..
        }
    ));
    assert!(matches!(
        literal_marker_at(b"{9223372036854775808}\r\n", 0),
        LiteralMarker::CountOutOfRange {
            size: 9_223_372_036_854_775_808,
            ..
        }
    ));
    assert!(matches!(
        literal_marker_at(b"{18446744073709551616}\r\n", 0),
        LiteralMarker::NotAMarker
    ));
}

// --- UTF-8 mode: RFC 6855 Section 3 / RFC 9051 Section 9 ---

/// When UTF8=ACCEPT is enabled (RFC 6855 Section 3), non-ASCII UTF-8
/// mailbox names must be encoded as quoted strings rather than literals.
/// RFC 6855 Section 3: "The server MUST accept UTF-8 in quoted strings."
/// RFC 9051 Section 9: extends CHAR to include UTF8-2/UTF8-3/UTF8-4.
#[test]
fn encode_select_utf8_mailbox_quoted_when_utf8_enabled() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("日本語").unwrap(),
        condstore: false,
        qresync: None,
    };
    let encoded = encode_command("A001", &cmd, &opts(LiteralMode::Synchronizing, true)).unwrap();
    let wire = encoded.to_vec();
    let wire_str = std::str::from_utf8(&wire).unwrap();

    // With utf8=true, encode_mailbox_name passes the name as raw UTF-8.
    // The per-command encoder then quotes it since it's valid UTF-8.
    assert_eq!(
        wire_str, "A001 SELECT \"日本語\"\r\n",
        "Non-ASCII mailbox should be quoted when UTF8=ACCEPT is active \
         (RFC 6855 Section 3); got literal instead"
    );
}

/// When utf8 mode is false, non-ASCII mailbox names must fall back to
/// literal encoding (RFC 3501 Section 9: CHAR = %x01-7F).
#[test]
fn encode_select_utf8_mailbox_literal_when_utf8_disabled() {
    let cmd = Command::Select {
        mailbox: MailboxName::new("日本語").unwrap(),
        condstore: false,
        qresync: None,
    };
    let encoded = encode_command("A001", &cmd, &default_opts()).unwrap();
    let wire = encoded.to_vec();
    let wire_str = std::str::from_utf8(&wire).unwrap();

    // Without utf8 mode, encode_mailbox_name converts to modified UTF-7:
    // "日本語" -> "&ZeVnLIqe-" (ASCII), which then gets quoted.
    assert!(
        !wire_str.contains('{'),
        "modified UTF-7 encoded mailbox should be quoted, not literal: {wire_str}"
    );
    assert!(
        wire_str.starts_with("A001 SELECT \""),
        "expected quoted mailbox name: {wire_str}"
    );
}

/// RFC 6855 Section 5: UTF8=ACCEPT does not extend LOGIN.
/// Non-ASCII credentials must be rejected; clients must use AUTHENTICATE.
#[test]
fn encode_login_rejects_utf8_credentials_even_when_utf8_enabled() {
    let cmd = Command::Login {
        user: "ユーザー".into(),
        pass: "пароль".into(),
    };
    let result = encode_command("A001", &cmd, &opts(LiteralMode::Synchronizing, true));

    assert!(
        matches!(result, Err(crate::Error::InvalidInput(ref msg)) if msg.contains("RFC 6855 Section 5")),
        "LOGIN with non-ASCII credentials must be rejected even when UTF8=ACCEPT is active \
         (RFC 6855 Section 5); got: {result:?}"
    );
}

/// CREATE, DELETE, SUBSCRIBE, UNSUBSCRIBE with UTF-8 mailbox names.
#[test]
fn encode_mailbox_cmds_utf8_quoted() {
    for (variant, keyword) in [
        ("create", "CREATE"),
        ("delete", "DELETE"),
        ("subscribe", "SUBSCRIBE"),
        ("unsubscribe", "UNSUBSCRIBE"),
    ] {
        let cmd = match variant {
            "create" => Command::Create {
                mailbox: MailboxName::new("Ångström").unwrap(),
            },
            "delete" => Command::Delete {
                mailbox: MailboxName::new("Ångström").unwrap(),
            },
            "subscribe" => Command::Subscribe {
                mailbox: MailboxName::new("Ångström").unwrap(),
            },
            "unsubscribe" => Command::Unsubscribe {
                mailbox: MailboxName::new("Ångström").unwrap(),
            },
            _ => unreachable!(),
        };
        let encoded = encode_command("T1", &cmd, &opts(LiteralMode::Synchronizing, true)).unwrap();
        let buf = encoded.to_vec();
        let wire = std::str::from_utf8(&buf).unwrap();
        assert_eq!(
            wire,
            format!("T1 {keyword} \"Ångström\"\r\n"),
            "{keyword} with UTF-8 mailbox should produce quoted string \
             when UTF8=ACCEPT is active (RFC 6855 Section 3)"
        );
    }
}

// -----------------------------------------------------------------------
// LITERAL- (RFC 7888 Section 5): size-limited non-synchronizing literals
// -----------------------------------------------------------------------

/// RFC 7888 Section 5: in LITERAL- mode, literals > 4096 bytes MUST use
/// synchronizing form `{N}\r\n`. Previously, the encoder had no concept
/// of LITERAL-  -  it could only produce `{N+}` for all literals or `{N}`
/// for all literals, with the connection layer post-processing the buffer.
/// This test verifies the encoder natively enforces the 4096-byte limit.
#[test]
fn literal_minus_large_literal_uses_synchronizing_form() {
    let large_data = format!("data\r\n{}", "x".repeat(4100));
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        large_data.as_bytes(),
        false,
        LiteralMode::LiteralMinus,
    );
    let output = std::str::from_utf8(&buf).unwrap();
    let expected_size = large_data.len();
    // RFC 7888 Section 5: literal > 4096 bytes MUST use synchronizing form.
    assert!(
        output.contains(&format!("{{{expected_size}}}\r\n")),
        "LITERAL- with literal > 4096 bytes must use synchronizing form \
         {{N}}\\r\\n (RFC 7888 Section 5); got: {output}"
    );
    assert!(
        !output.contains(&format!("{{{expected_size}+}}")),
        "LITERAL- with literal > 4096 bytes must NOT use non-synchronizing \
         form {{N+}} (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 5: in LITERAL- mode, literals <= 4096 bytes use
/// non-synchronizing form `{N+}\r\n`.
#[test]
fn literal_minus_small_literal_uses_non_synchronizing_form() {
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, b"pass\r\nword", false, LiteralMode::LiteralMinus);
    let output = std::str::from_utf8(&buf).unwrap();
    // RFC 7888 Section 5: literal <= 4096 bytes uses non-synchronizing form.
    assert!(
        output.contains("{10+}\r\n"),
        "LITERAL- with literal <= 4096 bytes must use non-synchronizing form \
         {{N+}}\\r\\n (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 5: boundary test  -  a literal of exactly 4096 bytes
/// should use non-synchronizing form in LITERAL- mode.
#[test]
fn literal_minus_boundary_4096_uses_non_synchronizing() {
    let data = format!("x\r\n{}", "a".repeat(4093));
    assert_eq!(data.len(), 4096);
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, data.as_bytes(), false, LiteralMode::LiteralMinus);
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("{4096+}\r\n"),
        "LITERAL- with literal of exactly 4096 bytes must use non-synchronizing \
         form (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 5: boundary test  -  a literal of 4097 bytes MUST use
/// synchronizing form in LITERAL- mode.
#[test]
fn literal_minus_boundary_4097_uses_synchronizing() {
    let data = format!("x\r\n{}", "a".repeat(4094));
    assert_eq!(data.len(), 4097);
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(&mut buf, data.as_bytes(), false, LiteralMode::LiteralMinus);
    let output = std::str::from_utf8(&buf).unwrap();
    assert!(
        output.contains("{4097}\r\n"),
        "LITERAL- with literal of 4097 bytes must use synchronizing form \
         (RFC 7888 Section 5); got: {output}"
    );
    assert!(
        !output.contains("{4097+}"),
        "LITERAL- with literal of 4097 bytes must NOT use non-synchronizing \
         form (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 4: LITERAL+ mode uses non-synchronizing `{N+}\r\n`
/// regardless of literal size  -  even for very large literals.
#[test]
fn literal_plus_large_literal_uses_non_synchronizing() {
    let large_data = format!("data\r\n{}", "x".repeat(10_000));
    let mut buf = BytesMut::new();
    push_quoted_or_literal_utf8(
        &mut buf,
        large_data.as_bytes(),
        false,
        LiteralMode::LiteralPlus,
    );
    let output = std::str::from_utf8(&buf).unwrap();
    let expected_size = large_data.len();
    assert!(
        output.contains(&format!("{{{expected_size}+}}\r\n")),
        "LITERAL+ must use non-synchronizing form {{N+}}\\r\\n for all sizes \
         (RFC 7888 Section 4); got: {output}"
    );
}

/// RFC 7888 Section 5: LITERAL- segment splitting  -  large literals must
/// produce synchronizing boundaries that split into multiple segments.
#[test]
fn literal_minus_large_literal_produces_segments() {
    let data = format!("data\r\n{}", "x".repeat(5000));
    let encoded = x_command(LiteralMode::LiteralMinus, &[data.as_bytes()]);
    let segments = segment_bytes(&encoded);
    // The large literal must produce a synchronizing boundary, resulting
    // in multiple segments (RFC 3501 Section 4.3).
    assert!(
        segments.len() > 1,
        "LITERAL- with literal > 4096 bytes must produce multiple segments \
         for synchronizing literal handling; got {} segment(s)",
        segments.len()
    );
}

/// RFC 7888 Section 5: LITERAL- with small literal produces a single
/// segment (no synchronizing boundary needed).
#[test]
fn literal_minus_small_literal_produces_single_segment() {
    let encoded = x_command(LiteralMode::LiteralMinus, &[b"pass\r\nword"]);
    assert_eq!(encoded.to_vec(), b"A001 X {10+}\r\npass\r\nword\r\n");
    let segments = segment_bytes(&encoded);
    assert_eq!(
        segments.len(),
        1,
        "LITERAL- with literal <= 4096 bytes must produce a single segment \
         (non-synchronizing, RFC 7888 Section 5); got {} segments",
        segments.len()
    );
}

/// RFC 7888 Section 5: `encode_append` must respect the
/// LITERAL- size limit for the message literal.
#[test]
fn literal_minus_multi_append_large_message_synchronizing() {
    // Message size 5000 > 4096, must use synchronizing form in LITERAL- mode.
    let msgs = [append_msg(&[], None, 5000)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        true,
        &append_opts(LiteralMode::LiteralMinus, false),
    );
    let output = output.as_str();
    assert!(
        output.contains("{5000}\r\n"),
        "LITERAL- with message > 4096 bytes must use synchronizing literal \
         in MULTIAPPEND (RFC 7888 Section 5); got: {output}"
    );
    assert!(
        !output.contains("{5000+}"),
        "LITERAL- with message > 4096 bytes must NOT use non-synchronizing \
         literal in MULTIAPPEND (RFC 7888 Section 5); got: {output}"
    );
}

/// RFC 7888 Section 5: `encode_append` uses non-synchronizing
/// form for small messages in LITERAL- mode.
#[test]
fn literal_minus_multi_append_small_message_non_synchronizing() {
    // Message size 100 <= 4096, must use non-synchronizing form.
    let msgs = [append_msg(&[], None, 100)];
    let output = append_wire(
        "A001",
        "INBOX",
        &msgs,
        true,
        &append_opts(LiteralMode::LiteralMinus, false),
    );
    let output = output.as_str();
    assert!(
        output.contains("{100+}\r\n"),
        "LITERAL- with message <= 4096 bytes must use non-synchronizing \
         literal in MULTIAPPEND (RFC 7888 Section 5); got: {output}"
    );
}

// -----------------------------------------------------------------------
// CRLF injection prevention (RFC 3501 Section 2.2)
//
// Embedding CR/LF in unquoted parameters would terminate the current
// command prematurely and allow injection of arbitrary IMAP commands.
// These tests verify that every raw-string parameter that is directly
// embedded into a command is validated for CRLF.
// -----------------------------------------------------------------------

/// SEARCH criteria containing CRLF must be rejected to prevent
/// command injection (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_search_criteria_rejected() {
    let cmd = Command::Search {
        criteria: "ALL\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SEARCH criteria with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("CR or LF"),
        "error message should mention CR or LF: {err_msg}"
    );
}

/// UID SEARCH criteria containing CRLF must be rejected.
#[test]
fn crlf_injection_uid_search_criteria_rejected() {
    let cmd = Command::UidSearch {
        criteria: "ALL\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "UID SEARCH criteria with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// SEARCH RETURN options containing CRLF must be rejected.
#[test]
fn crlf_injection_search_return_opts_rejected() {
    let cmd = Command::SearchReturn {
        criteria: "ALL".into(),
        return_opts: vec!["MIN\r\nA002 DELETE INBOX".into()],
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SEARCH RETURN option with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// STATUS items containing CRLF must be rejected to prevent
/// command injection (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_status_items_rejected() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "(MESSAGES)\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "STATUS items with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// FETCH items containing CRLF must be rejected to prevent
/// command injection (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_fetch_items_rejected() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS\r\nA002 DELETE INBOX".into(),
        changed_since: None,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "FETCH items with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// UID FETCH items containing CRLF must be rejected.
#[test]
fn crlf_injection_uid_fetch_items_rejected() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS\r\nA002 DELETE INBOX".into(),
        changed_since: None,
        vanished: false,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "UID FETCH items with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// LIST-STATUS `status_items` containing CRLF must be rejected to prevent
/// command injection (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_list_status_items_rejected() {
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: "MESSAGES\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "LIST-STATUS items with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// THREAD criteria containing CRLF must be rejected.
#[test]
fn crlf_injection_thread_criteria_rejected() {
    let cmd = Command::Thread {
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "ALL\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "THREAD criteria with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// SORT criteria containing CRLF must be rejected.
#[test]
fn crlf_injection_sort_criteria_rejected() {
    let cmd = Command::Sort {
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: "ALL\r\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "SORT criteria with CRLF must be rejected (RFC 3501 Section 2.2)"
    );
}

/// Bare LF without CR is also rejected (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_bare_lf_rejected() {
    let cmd = Command::Search {
        criteria: "ALL\nA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "bare LF in SEARCH criteria must be rejected (RFC 3501 Section 2.2)"
    );
}

/// Bare CR without LF is also rejected (RFC 3501 Section 2.2).
#[test]
fn crlf_injection_bare_cr_rejected() {
    let cmd = Command::Search {
        criteria: "ALL\rA002 DELETE INBOX".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "bare CR in SEARCH criteria must be rejected (RFC 3501 Section 2.2)"
    );
}

/// Normal SEARCH criteria without CRLF must succeed.
#[test]
fn crlf_injection_normal_search_succeeds() {
    let cmd = Command::Search {
        criteria: "ALL".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "normal SEARCH criteria must succeed: {result:?}"
    );
    assert_eq!(&buf[..], b"A001 SEARCH ALL\r\n");
}

/// RFC 7888 Section 3: a `+` literal marker is only valid when the
/// server advertised LITERAL+ / LITERAL- (or `IMAP4rev2` semantics selected
/// `LiteralMinus`). SEARCH criteria must therefore reject `{N+}` in plain
/// synchronizing-literal mode.
#[test]
fn search_rejects_non_synchronizing_literal_without_extension() {
    let cmd = Command::Search {
        criteria: "TEXT {3+}\r\nfoo".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    let err = result.expect_err(
        "SEARCH must reject non-synchronizing literals without negotiated \
         LITERAL+/LITERAL- support (RFC 7888 Section 3)",
    );
    // The refusal must arrive as a capability gap, not be flattened into
    // malformed input.
    assert!(
        matches!(err, crate::Error::MissingCapability(_)),
        "got {err:?}"
    );
}

/// A validation refusal arrives as `InvalidInput` with its message as the
/// encoder wrote it, not prefixed with another variant's display text.
#[test]
fn an_encoder_validation_refusal_arrives_as_invalid_input() {
    let cmd = Command::Search {
        criteria: String::new(),
    };
    let mut buf = BytesMut::new();
    let err = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts())
        .expect_err("empty SEARCH criteria are refused");
    let crate::Error::InvalidInput(message) = err else {
        panic!("expected InvalidInput, got {err:?}");
    };
    assert!(
        message.contains("at least one search criterion"),
        "got {message:?}"
    );
}

/// A command whose prerequisite capability is not usable is refused as
/// `MissingCapability`, worded `<cmd> requires <cap>` for every command.
#[test]
fn an_encoder_capability_refusal_names_the_command_and_the_capability() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "FLAGS".into(),
        changed_since: Some(5),
        vanished: false,
    };
    let no_condstore = EncodeOptions {
        utf8_mode: false,
        literal_mode: LiteralMode::Synchronizing,
        capabilities: vec![Capability::Imap4Rev1],
        enabled: Vec::new(),
    };
    let err = encode_command("A001", &cmd, &no_condstore).unwrap_err();
    assert_eq!(
        err,
        crate::Error::MissingCapability("UID FETCH (CHANGEDSINCE) requires CONDSTORE".into())
    );
}

/// RFC 7888 Section 3: once LITERAL+ is negotiated, SEARCH criteria may
/// include non-synchronizing literals.
#[test]
fn search_allows_non_synchronizing_literal_with_literal_plus() {
    let cmd = Command::Search {
        criteria: "TEXT {3+}\r\nfoo".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(
        &mut buf,
        "A001",
        &cmd,
        &opts(LiteralMode::LiteralPlus, false),
    );
    assert!(
        result.is_ok(),
        "SEARCH should allow non-synchronizing literals once LITERAL+ is active: {result:?}"
    );
    assert_eq!(&buf[..], b"A001 SEARCH TEXT {3+}\r\nfoo\r\n");
}

/// Normal STATUS items without CRLF must succeed.
#[test]
fn crlf_injection_normal_status_succeeds() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "(MESSAGES UNSEEN)".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "normal STATUS items must succeed: {result:?}"
    );
}

/// RFC 3501 Section 6.3.10: STATUS takes a parenthesized status-att list.
/// The encoder should accept raw item text from callers and normalize it
/// to the required on-wire `(<items>)` form.
#[test]
fn status_raw_items_are_parenthesized() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "MESSAGES UNSEEN".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "raw STATUS items should be normalized to a valid status-att-list: {result:?}"
    );
    assert_eq!(&buf[..], b"A001 STATUS \"INBOX\" (MESSAGES UNSEEN)\r\n");
}

/// RFC 3501 Section 6.3.10: STATUS requires at least one status-att in
/// the parenthesized list.
#[test]
fn status_rejects_empty_items_list() {
    let cmd = Command::Status {
        mailbox: MailboxName::new("INBOX").unwrap(),
        items: "()".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty STATUS item list must be rejected (RFC 3501 Section 6.3.10)"
    );
}

/// RFC 5819 Section 2 reuses STATUS data items and therefore also requires
/// at least one item inside `STATUS (<items>)`.
#[test]
fn list_status_rejects_empty_status_items() {
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: String::new(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "LIST-STATUS must reject an empty status data item list (RFC 5819 Section 2)"
    );
}

/// RFC 3501 Section 6.4.4: SEARCH requires one or more search keys.
#[test]
fn search_rejects_empty_criteria() {
    let cmd = Command::Search {
        criteria: String::new(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty SEARCH criteria must be rejected (RFC 3501 Section 6.4.4)"
    );
}

/// RFC 5256 Section 3 / formal syntax Section 6: THREAD requires one or
/// more search criteria after the charset.
#[test]
fn thread_rejects_empty_criteria() {
    let cmd = Command::Thread {
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: "   ".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty THREAD criteria must be rejected (RFC 5256 Section 6)"
    );
}

/// RFC 5256 Section 2 / formal syntax Section 6: SORT requires one or
/// more search criteria after the charset.
#[test]
fn sort_rejects_empty_criteria() {
    let cmd = Command::Sort {
        sort_criteria: "DATE".into(),
        charset: "UTF-8".into(),
        criteria: String::new(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty SORT criteria must be rejected (RFC 5256 Section 6)"
    );
}

/// RFC 7888 Section 5 / RFC 9051 Section 4.3: in LITERAL- style mode,
/// non-synchronizing literals must be 4096 octets or smaller. THREAD and
/// SORT reuse SEARCH criteria, so the same size cap applies there too.
#[test]
fn thread_rejects_oversized_non_synchronizing_literal_in_literal_minus_mode() {
    let oversized = "a".repeat(4097);
    let cmd = Command::Thread {
        algorithm: "REFERENCES".into(),
        charset: "UTF-8".into(),
        criteria: format!("TEXT {{4097+}}\r\n{oversized}"),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(
        &mut buf,
        "A001",
        &cmd,
        &opts(LiteralMode::LiteralMinus, false),
    );
    let err = result.expect_err(
        "THREAD must reject non-synchronizing literals larger than 4096 octets \
         in LITERAL- mode (RFC 7888 Section 5 / RFC 9051 Section 4.3)",
    );
    assert!(
        matches!(err, crate::Error::MissingCapability(_)),
        "LITERAL- without LITERAL+ is a capability gap; got {err:?}"
    );
}

/// Normal FETCH items without CRLF must succeed.
#[test]
fn crlf_injection_normal_fetch_succeeds() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1:*").unwrap(),
        items: "(FLAGS ENVELOPE)".into(),
        changed_since: None,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "normal FETCH items must succeed: {result:?}"
    );
}

/// RFC 3501 Section 6.4.5: FETCH requires at least one message data item
/// or macro.
#[test]
fn fetch_rejects_empty_items() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: "()".into(),
        changed_since: None,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty FETCH item list must be rejected (RFC 3501 Section 6.4.5)"
    );
}

/// RFC 3501 Section 6.4.5: UID FETCH uses the same fetch-att grammar as
/// FETCH and therefore also requires at least one item.
#[test]
fn uid_fetch_rejects_empty_items() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: "   ".into(),
        changed_since: None,
        vanished: false,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty UID FETCH item list must be rejected (RFC 3501 Section 6.4.5)"
    );
}

/// RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5 require `fetch-att`
/// syntax to be structurally well-formed. Unbalanced inner delimiters in
/// BODY sections must be rejected before the command is written.
#[test]
fn fetch_rejects_unbalanced_inner_delimiters() {
    let cmd = Command::Fetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: "BODY[HEADER.FIELDS (SUBJECT)".into(),
        changed_since: None,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "malformed inner FETCH delimiters must be rejected (RFC 3501 Section 6.4.5)"
    );
}

/// UID FETCH uses the same `fetch-att` grammar and must reject malformed
/// inner delimiters for the same reason.
#[test]
fn uid_fetch_rejects_unbalanced_inner_delimiters() {
    let cmd = Command::UidFetch {
        sequence_set: SequenceSet::new("1").unwrap(),
        items: "BODY.PEEK[HEADER.FIELDS (SUBJECT)".into(),
        changed_since: None,
        vanished: false,
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "malformed inner UID FETCH delimiters must be rejected \
         (RFC 3501 Section 6.4.5 / RFC 9051 Section 6.4.5)"
    );
}

/// Normal LIST-STATUS items without CRLF must succeed.
#[test]
fn crlf_injection_normal_list_status_succeeds() {
    let cmd = Command::ListStatus {
        reference: String::new().into(),
        pattern: "*".into(),
        status_items: "MESSAGES UNSEEN".into(),
    };
    let mut buf = BytesMut::new();
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_ok(),
        "normal LIST-STATUS items must succeed: {result:?}"
    );
}

/// `validate_no_crlf` rejects CR (RFC 3501 Section 2.2).
#[test]
fn validate_no_crlf_rejects_cr() {
    let result = validate_no_crlf("foo\rbar", "test");
    assert!(result.is_err());
}

/// `validate_no_crlf` rejects LF (RFC 3501 Section 2.2).
#[test]
fn validate_no_crlf_rejects_lf() {
    let result = validate_no_crlf("foo\nbar", "test");
    assert!(result.is_err());
}

/// `validate_no_crlf` rejects CRLF (RFC 3501 Section 2.2).
#[test]
fn validate_no_crlf_rejects_crlf() {
    let result = validate_no_crlf("foo\r\nbar", "test");
    assert!(result.is_err());
}

/// `validate_no_crlf` accepts clean input.
#[test]
fn validate_no_crlf_accepts_clean_input() {
    let result = validate_no_crlf("ALL UNSEEN", "test");
    assert!(result.is_ok());
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

/// RFC 6855 Section 3: once UTF8=ACCEPT is active, clients MUST NOT send
/// SEARCH with a leading CHARSET specification.
#[test]
fn encode_search_rejects_charset_when_utf8_enabled() {
    let cmd = Command::Search {
        criteria: "CHARSET UTF-8 ALL".into(),
    };
    let result = encode_command("A001", &cmd, &opts(LiteralMode::Synchronizing, true));

    assert!(
        matches!(result, Err(crate::Error::InvalidInput(ref msg)) if msg.contains("RFC 6855 Section 3")),
        "SEARCH with CHARSET must be rejected when UTF8=ACCEPT is active \
         (RFC 6855 Section 3); got: {result:?}"
    );
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

// ========================================================================
// Property-based round-trip and invariant tests
// ========================================================================

mod prop_roundtrip {
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
    fn arb_sequence_set() -> impl Strategy<Value = SequenceSet> {
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
}

// ===== NOTIFY (RFC 5465) encoder tests =====

use crate::types::notify::{MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};

/// RFC 5465 Section 3: `NOTIFY NONE` cancels all event subscriptions.
#[test]
fn encode_notify_none() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifyNone;
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 NOTIFY NONE\r\n");
}

/// RFC 5465 Section 3: basic NOTIFY SET with selected mailbox and message events.
#[test]
fn encode_notify_set_selected_basic() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Selected,
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        }],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (selected (MessageNew MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 5.2: `MessageNew` with fetch attributes for selected mailbox.
#[test]
fn encode_notify_set_with_fetch_attrs() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Selected,
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![
                        "uid".into(),
                        "body.peek[header.fields (from to subject)]".into(),
                    ],
                },
                NotifyEvent::MessageExpunge,
            ],
        }],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (selected (MessageNew \
          (uid body.peek[header.fields (from to subject)]) MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 4: STATUS indicator triggers initial STATUS responses.
#[test]
fn encode_notify_set_with_status_indicator() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: true,
        event_groups: vec![
            NotifyEventGroup {
                filter: MailboxFilter::Selected,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            },
            NotifyEventGroup {
                filter: MailboxFilter::Subtree(vec!["Lists".into()]),
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            },
        ],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    // Matches RFC 5465 Section 9.1 example (without fetch-attrs):
    // b NOTIFY SET STATUS (selected (MessageNew MessageExpunge))
    //   (subtree Lists (MessageNew MessageExpunge))
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET STATUS (selected (MessageNew MessageExpunge)) \
          (subtree \"Lists\" (MessageNew MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 6.2: selected-delayed filter.
#[test]
fn encode_notify_set_selected_delayed() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::SelectedDelayed,
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
                NotifyEvent::FlagChange,
            ],
        }],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (selected-delayed (MessageNew MessageExpunge FlagChange))\r\n"
    );
}

/// RFC 5465 Section 6: all simple mailbox filter types.
#[test]
fn encode_notify_set_simple_filters() {
    for (filter, expected) in [
        (MailboxFilter::Inboxes, "inboxes"),
        (MailboxFilter::Personal, "personal"),
        (MailboxFilter::Subscribed, "subscribed"),
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup {
                filter,
                events: vec![NotifyEvent::MailboxName],
            }],
        });
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
        let expected_str = format!("A001 NOTIFY SET ({expected} (MailboxName))\r\n");
        assert_eq!(&buf[..], expected_str.as_bytes(), "filter: {expected}");
    }
}

/// RFC 5465 Section 6.7: mailboxes filter with multiple mailbox names.
#[test]
fn encode_notify_set_mailboxes_multiple() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Mailboxes(vec!["INBOX".into(), "Sent Items".into()]),
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        }],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (mailboxes (\"INBOX\" \"Sent Items\") (MessageNew MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 8: single mailbox in subtree/mailboxes is encoded bare.
#[test]
fn encode_notify_set_subtree_single() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Subtree(vec!["Lists".into()]),
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        }],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (subtree \"Lists\" (MessageNew MessageExpunge))\r\n"
    );
}

/// RFC 5465 Section 8: events = "NONE" for an event group with no events.
#[test]
fn encode_notify_set_events_none() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Selected,
            events: vec![],
        }],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(&buf[..], b"A001 NOTIFY SET (selected NONE)\r\n");
}

/// All event types encode correctly (RFC 5465 Section 5).
#[test]
fn encode_notify_set_all_event_types() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![
            NotifyEventGroup {
                filter: MailboxFilter::Selected,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::FlagChange,
                    NotifyEvent::AnnotationChange,
                ],
            },
            NotifyEventGroup {
                filter: MailboxFilter::Personal,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::MailboxName,
                    NotifyEvent::SubscriptionChange,
                    NotifyEvent::MailboxMetadataChange,
                    NotifyEvent::ServerMetadataChange,
                ],
            },
        ],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET \
          (selected (MessageNew MessageExpunge FlagChange AnnotationChange)) \
          (personal (MessageNew MessageExpunge MailboxName SubscriptionChange \
          MailboxMetadataChange ServerMetadataChange))\r\n"
    );
}

/// Extension event types are encoded verbatim (RFC 5465 Section 8: event-ext = atom).
#[test]
fn encode_notify_set_extension_event() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Personal,
            events: vec![NotifyEvent::Other("VendorSpecificEvent".into())],
        }],
    });
    encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).unwrap();
    assert_eq!(
        &buf[..],
        b"A001 NOTIFY SET (personal (VendorSpecificEvent))\r\n"
    );
}

/// NOTIFY SET with empty `event_groups` must fail (RFC 5465 Section 8:
/// event-groups requires at least one event-group).
#[test]
fn encode_notify_set_empty_event_groups_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "NOTIFY SET with empty event_groups must fail (RFC 5465 Section 8)"
    );
}

/// NOTIFY SET subtree with empty mailbox list must fail
/// (RFC 5465 Section 8: one-or-more-mailbox).
#[test]
fn encode_notify_set_subtree_empty_mailboxes_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Subtree(vec![]),
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "subtree with empty mailbox list must fail (RFC 5465 Section 8)"
    );
}

/// NOTIFY SET mailboxes with empty mailbox list must fail
/// (RFC 5465 Section 8: one-or-more-mailbox).
#[test]
fn encode_notify_set_mailboxes_empty_returns_error() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Mailboxes(vec![]),
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::MessageExpunge,
            ],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "mailboxes with empty mailbox list must fail (RFC 5465 Section 8)"
    );
}

/// `NotifyEvent::Other` must be a valid IMAP atom (RFC 5465 Section 8: event-ext = atom).
#[test]
fn encode_notify_set_rejects_invalid_other_event() {
    for invalid in [
        "",
        "has space",
        "has\ttab",
        "(parens)",
        "cr\rlf",
        "lf\nonly",
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup {
                filter: MailboxFilter::Personal,
                events: vec![NotifyEvent::Other(invalid.into())],
            }],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "Other(\"{invalid}\") must be rejected as invalid IMAP atom"
        );
    }
}

/// CRLF in `MessageNew` fetch attributes must be rejected to prevent command
/// injection (RFC 3501 Section 2.2).
#[test]
fn encode_notify_set_rejects_crlf_in_fetch_attrs() {
    for payload in ["uid\r\nA002 LOGOUT", "flags\r", "body\n"] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup {
                filter: MailboxFilter::Selected,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![payload.into()],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            }],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "fetch-att containing CRLF must be rejected: {payload:?}"
        );
    }
}

/// `MessageNew` fetch attributes with unbalanced delimiters must be rejected
/// (RFC 3501 Section 6.4.5 fetch-att grammar).
#[test]
fn encode_notify_set_rejects_malformed_fetch_attrs() {
    for malformed in [
        "BODY[TEXT",                         // unclosed bracket
        "FLAGS)",                            // unmatched closing paren
        "BODY.PEEK]",                        // unmatched closing bracket
        "BODY.PEEK[HEADER.FIELDS (From To)", // unclosed bracket
        "(UID FLAGS",                        // unclosed paren
        "\"unterminated quote",              // unclosed quote
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup {
                filter: MailboxFilter::Selected,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![malformed.into()],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            }],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "malformed fetch-att must be rejected: {malformed:?}"
        );
    }
}

/// Valid `MessageNew` fetch attributes with balanced delimiters must be accepted.
#[test]
fn encode_notify_set_accepts_valid_fetch_attrs() {
    for valid in [
        "UID",
        "FLAGS",
        "ENVELOPE",
        "BODY.PEEK[HEADER.FIELDS (From To Subject)]",
        "BODY[TEXT]<0.100>",
        "BODYSTRUCTURE",
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup {
                filter: MailboxFilter::Selected,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![valid.into()],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            }],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_ok(),
            "valid fetch-att must be accepted: {valid:?}, got: {result:?}"
        );
    }
}

/// Non-message events on selected filter must be rejected (RFC 5465 Section 6.1).
#[test]
fn encode_notify_set_rejects_non_message_event_on_selected() {
    for (filter, filter_name) in [
        (MailboxFilter::Selected, "selected"),
        (MailboxFilter::SelectedDelayed, "selected-delayed"),
    ] {
        for non_msg_event in [
            NotifyEvent::MailboxName,
            NotifyEvent::SubscriptionChange,
            NotifyEvent::MailboxMetadataChange,
            NotifyEvent::ServerMetadataChange,
        ] {
            let mut buf = BytesMut::new();
            let cmd = crate::types::Command::NotifySet(NotifySetParams {
                status: false,
                event_groups: vec![NotifyEventGroup {
                    filter: filter.clone(),
                    events: vec![
                        NotifyEvent::MessageNew {
                            fetch_attrs: vec![],
                        },
                        NotifyEvent::MessageExpunge,
                        non_msg_event.clone(),
                    ],
                }],
            });
            let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
            assert!(
                result.is_err(),
                "{filter_name} must reject non-message events (RFC 5465 Section 6.1)"
            );
        }
    }
}

/// `MessageExpunge` without `MessageNew` must be rejected (RFC 5465 Section 5).
#[test]
fn encode_notify_set_rejects_expunge_without_new() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Personal,
            events: vec![NotifyEvent::MessageExpunge],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "MessageExpunge without MessageNew must fail (RFC 5465 Section 5)"
    );
}

/// `MessageNew` without `MessageExpunge` must be rejected on selected filters
/// (RFC 5465 Section 5).
#[test]
fn encode_notify_set_rejects_new_without_expunge_on_selected() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Selected,
            events: vec![NotifyEvent::MessageNew {
                fetch_attrs: vec![],
            }],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "MessageNew without MessageExpunge must fail on selected filter (RFC 5465 Section 5)"
    );
}

/// RFC 5465 Section 5: "If one of `MessageNew` or `MessageExpunge` is
/// specified, then both events MUST be specified"  -  this applies globally,
/// regardless of mailbox filter.  `MessageNew` alone on a non-selected filter
/// must be rejected.
#[test]
fn encode_notify_set_rejects_new_without_expunge_on_non_selected() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Personal,
            events: vec![
                NotifyEvent::MailboxName,
                NotifyEvent::SubscriptionChange,
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
            ],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "MessageNew without MessageExpunge must fail on non-selected filters (RFC 5465 Section 5)"
    );
}

/// Empty fetch-att in `MessageNew` must be rejected (RFC 3501 Section 9).
#[test]
fn encode_notify_set_rejects_empty_fetch_attr() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Selected,
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec!["UID".into(), String::new()],
                },
                NotifyEvent::MessageExpunge,
            ],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "empty fetch-att string must be rejected (RFC 3501 Section 9)"
    );
}

/// `FlagChange` without `MessageNew`+`MessageExpunge` must be rejected (RFC 5465 Section 5).
#[test]
fn encode_notify_set_rejects_flag_change_without_new_and_expunge() {
    // FlagChange with only MessageNew (missing MessageExpunge)
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Personal,
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::FlagChange,
            ],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "FlagChange without MessageExpunge must fail (RFC 5465 Section 5)"
    );

    // FlagChange alone
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Personal,
            events: vec![NotifyEvent::FlagChange],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "FlagChange alone must fail (RFC 5465 Section 5)"
    );
}

/// `AnnotationChange` without `MessageNew`+`MessageExpunge` must be rejected
/// (RFC 5465 Section 5).
#[test]
fn encode_notify_set_rejects_annotation_change_without_new_and_expunge() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![NotifyEventGroup {
            filter: MailboxFilter::Personal,
            events: vec![
                NotifyEvent::MessageNew {
                    fetch_attrs: vec![],
                },
                NotifyEvent::AnnotationChange,
            ],
        }],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "AnnotationChange without MessageExpunge must fail (RFC 5465 Section 5)"
    );
}

/// Both `selected` and `selected-delayed` in the same NOTIFY SET must be
/// rejected (RFC 5465 Section 3).
#[test]
fn encode_notify_set_rejects_both_selected_and_selected_delayed() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![
            NotifyEventGroup {
                filter: MailboxFilter::Selected,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            },
            NotifyEventGroup {
                filter: MailboxFilter::SelectedDelayed,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            },
        ],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "selected + selected-delayed must fail (RFC 5465 Section 3)"
    );
}

/// Duplicate `selected` event-groups must be rejected  -  at most one
/// selected/selected-delayed event-group is allowed (RFC 5465 Section 3).
#[test]
fn encode_notify_set_rejects_duplicate_selected() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![
            NotifyEventGroup {
                filter: MailboxFilter::Selected,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            },
            NotifyEventGroup {
                filter: MailboxFilter::Selected,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::FlagChange,
                ],
            },
        ],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "duplicate selected event-groups must fail (RFC 5465 Section 3)"
    );
}

/// Duplicate `selected-delayed` event-groups must be rejected  -  at most one
/// selected/selected-delayed event-group is allowed (RFC 5465 Section 3).
#[test]
fn encode_notify_set_rejects_duplicate_selected_delayed() {
    let mut buf = BytesMut::new();
    let cmd = crate::types::Command::NotifySet(NotifySetParams {
        status: false,
        event_groups: vec![
            NotifyEventGroup {
                filter: MailboxFilter::SelectedDelayed,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            },
            NotifyEventGroup {
                filter: MailboxFilter::SelectedDelayed,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::FlagChange,
                ],
            },
        ],
    });
    let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
    assert!(
        result.is_err(),
        "duplicate selected-delayed event-groups must fail (RFC 5465 Section 3)"
    );
}

/// `MessageNew` fetch attributes on non-selected filter must be rejected
/// (RFC 5465 Section 8 ABNF).
#[test]
fn encode_notify_set_rejects_fetch_attrs_on_non_selected_filter() {
    for filter in [
        MailboxFilter::Inboxes,
        MailboxFilter::Personal,
        MailboxFilter::Subscribed,
        MailboxFilter::Subtree(vec!["INBOX".into()]),
        MailboxFilter::Mailboxes(vec!["INBOX".into()]),
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup {
                filter,
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec!["UID".into()],
                    },
                    NotifyEvent::MessageExpunge,
                ],
            }],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "fetch-attrs on non-selected filter must fail (RFC 5465 Section 8)"
        );
    }
}

/// Extension events (`Other(...)`) must be rejected under `selected` and
/// `selected-delayed` filters because RFC 5465 Section 8 defines `event-ext`
/// as a separate ABNF production from `message-event`, and Section 6.1
/// restricts selected filters to message events only.
#[test]
fn encode_notify_set_rejects_extension_event_on_selected() {
    for (filter, filter_name) in [
        (MailboxFilter::Selected, "selected"),
        (MailboxFilter::SelectedDelayed, "selected-delayed"),
    ] {
        let mut buf = BytesMut::new();
        let cmd = crate::types::Command::NotifySet(NotifySetParams {
            status: false,
            event_groups: vec![NotifyEventGroup {
                filter: filter.clone(),
                events: vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: vec![],
                    },
                    NotifyEvent::MessageExpunge,
                    NotifyEvent::Other("X-CUSTOM".into()),
                ],
            }],
        });
        let result = encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts());
        assert!(
            result.is_err(),
            "{filter_name} must reject extension events (RFC 5465 Section 6.1, Section 8)"
        );
    }
}

// ========================================================================
// Adversarial / hostile-payload encoding
//
// These pin how the encoder behaves when a *command payload* happens to
// contain bytes that look like IMAP framing.
// ========================================================================

/// A LITERAL+ payload is opaque framing data. A sequence that resembles a
/// synchronizing marker inside the payload must not create another segment.
#[test]
fn literal_plus_body_containing_sync_marker_stays_in_one_segment() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        // 11 opaque octets. `{5}\r\n` is ordinary data here, not framing.
        entries: vec![("/private/x".into(), Some(b"a{5}\r\nbbbbb".to_vec()))],
    };
    let encoded = encode_command("A001", &cmd, &opts(LiteralMode::LiteralPlus, false)).unwrap();
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        1,
        "the `{{5}}\\r\\n` inside the LITERAL+ body is data, not a literal \
         marker (RFC 7888 Section 4). Got: {:?}",
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        &segments[0][..],
        b"A001 SETMETADATA \"INBOX\" (\"/private/x\" {11+}\r\na{5}\r\nbbbbb)\r\n"
    );
}

/// A literal-looking payload with an unrepresentable octet count is still
/// opaque LITERAL+ data. It must not be scanned, overflowed, or split.
#[test]
fn literal_plus_body_containing_overflow_marker_stays_in_one_segment() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![(
            "/private/x".into(),
            Some(b"{18446744073709551592}\r\n".to_vec()),
        )],
    };
    let encoded = encode_command("A001", &cmd, &opts(LiteralMode::LiteralPlus, false)).unwrap();

    assert_eq!(encoded.segments().len(), 1);
}

/// A *synchronizing* literal payload is skipped correctly, so the same
/// hostile bytes are harmless under `LiteralMode::Synchronizing`. This is the
/// control case for the test above: it isolates the defect to the
/// non-synchronizing marker form rather than to literal payloads generally.
#[test]
fn sync_literal_body_containing_sync_marker_is_skipped() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        entries: vec![("/private/x".into(), Some(b"a{5}\r\nbbbbb".to_vec()))],
    };
    let encoded = encode_command("A001", &cmd, &default_opts()).unwrap();
    let segments = segment_bytes(&encoded);

    assert_eq!(
        segments.len(),
        2,
        "exactly one split, at the real `{{11}}\\r\\n` marker (RFC 3501 \
         Section 4.3); got: {:?}",
        segments
            .iter()
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect::<Vec<_>>()
    );
    assert!(
        segments[0].ends_with(b"{11}\r\n"),
        "the split must be at the real marker; got: {:?}",
        String::from_utf8_lossy(&segments[0])
    );
    assert_eq!(
        &segments[1][..],
        b"a{5}\r\nbbbbb)\r\n",
        "the whole payload plus the command tail is one segment: the writer \
         records only the boundary of the literal it wrote"
    );
}

/// A `{digits}` group inside a payload that is *not* followed by CRLF is
/// payload, and the command stays one segment. The encoder never scans its
/// own output, so this pins that a value shaped like a marker cannot create a
/// boundary.
#[test]
fn brace_digits_not_abutting_crlf_is_not_a_literal_marker() {
    let cmd = Command::SetMetadata {
        mailbox: MailboxName::new("INBOX").unwrap(),
        // `{5}` appears, but is followed by `x`, not CRLF.
        entries: vec![("/private/x".into(), Some(b"a{5}x\r\nbb".to_vec()))],
    };
    let encoded = encode_command("A001", &cmd, &opts(LiteralMode::LiteralPlus, false)).unwrap();
    assert_eq!(
        encoded.segments().len(),
        1,
        "`{{5}}x` is not a literal marker: `}}` does not abut the CRLF \
         (RFC 3501 Section 4.3)"
    );
}

/// A one-character STATUS item list is valid in a LIST-EXTENDED return option.
#[test]
fn list_status_return_option_accepts_one_character_item_list() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: Vec::new(),
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: vec!["STATUS (X)".into()],
    };

    assert!(
        encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()).is_ok(),
        "the single item `X` must not be truncated before validation"
    );
    assert_eq!(&buf[..], b"A001 LIST \"\" \"*\" RETURN (STATUS (X))\r\n");
}

/// Nested parentheses are not valid in a LIST-EXTENDED STATUS item list.
#[test]
fn list_status_return_option_rejects_unbalanced_leading_paren() {
    let mut buf = BytesMut::new();
    let cmd = Command::ListExtended {
        selection_options: Vec::new(),
        reference: String::new().into(),
        patterns: vec!["*".into()],
        return_options: vec!["STATUS ((MESSAGES)".into()],
    };

    assert!(
        matches!(
            encode_command_to_buf(&mut buf, "A001", &cmd, &default_opts()),
            Err(crate::Error::InvalidInput(_))
        ),
        "an unbalanced STATUS list must not be emitted"
    );
}

#[test]
fn listed_mailbox_wire_octets_survive_select_encoding() {
    use crate::codec::decode::parse_response_utf8;
    use crate::types::response::{Response, UntaggedResponse};

    let cases: &[(&[u8], bool)] = &[
        (b"caf\xc3\xa9", false),
        (b"raw\xff", false),
        (b"&AGE-", false),
        (b"&not!base64-", false),
        (b"&unterminated", false),
        (b"&2AA-", false),
        (b"control\x01\x7f", false),
        (b"&AAoALQ-", false),
        (b"caf\xc3\xa9 &AGE-", true),
        (b"invalid\xff", true),
        (b"line\r\nbreak", true),
    ];
    for &(raw, utf8) in cases {
        let mut response = format!("* LIST () \"/\" {{{}}}\r\n", raw.len()).into_bytes();
        response.extend_from_slice(raw);
        response.extend_from_slice(b"\r\n");
        let (rest, response) = parse_response_utf8(&response, utf8).unwrap();
        assert!(rest.is_empty());
        let Response::Untagged(response) = response else {
            panic!("expected LIST")
        };
        let UntaggedResponse::List(info) = *response else {
            panic!("expected LIST")
        };
        let cmd = Command::Select {
            mailbox: info.name,
            condstore: false,
            qresync: None,
        };
        let mut encoded = BytesMut::new();
        encode_command_to_buf(
            &mut encoded,
            "A1",
            &cmd,
            &opts(LiteralMode::LiteralPlus, utf8),
        )
        .unwrap();
        // These samples contain no quote or backslash: the only legal framing
        // choices here are a quoted string or a counted non-sync literal.
        let mut quoted = b"A1 SELECT \"".to_vec();
        quoted.extend_from_slice(raw);
        quoted.extend_from_slice(b"\"\r\n");
        let mut literal = format!("A1 SELECT {{{}+}}\r\n", raw.len()).into_bytes();
        literal.extend_from_slice(raw);
        literal.extend_from_slice(b"\r\n");
        assert!(
            encoded[..] == quoted || encoded[..] == literal,
            "{raw:?}: {encoded:?}"
        );
    }
}

#[test]
fn mailbox_identity_distinguishes_equal_display_names() {
    use crate::codec::decode::parse_response_utf8;
    use crate::types::response::{Response, UntaggedResponse};
    let parse = |raw: &[u8], utf8| {
        let mut response = format!("* LIST () NIL {{{}}}\r\n", raw.len()).into_bytes();
        response.extend_from_slice(raw);
        response.extend_from_slice(b"\r\n");
        let (_, Response::Untagged(response)) = parse_response_utf8(&response, utf8).unwrap()
        else {
            panic!("expected LIST")
        };
        let UntaggedResponse::List(info) = *response else {
            panic!("expected LIST")
        };
        info.name
    };
    for (a, b, utf8) in [
        (b"&AGE-".as_slice(), b"a".as_slice(), false),
        (b"\xff".as_slice(), b"\xfe".as_slice(), true),
    ] {
        let a = parse(a, utf8);
        let b = parse(b, utf8);
        assert_eq!(a.as_str(), b.as_str());
        assert_ne!(a, b);
        let set = std::collections::HashSet::from([a, b]);
        assert_eq!(set.len(), 2);
    }
    assert_eq!(parse(b"caf&AOk-", false), MailboxName::new("café").unwrap());
    assert_eq!(
        parse("café".as_bytes(), true),
        MailboxName::new("café").unwrap()
    );
}

#[test]
fn mailbox_wire_nul_is_refused_instead_of_addressing_a_different_name() {
    let mailbox = MailboxName::from_wire("a\0b".into(), b"a\0b", true);
    let cmd = Command::Select {
        mailbox,
        condstore: false,
        qresync: None,
    };
    let mut buf = BytesMut::new();
    assert!(matches!(
        encode_command_to_buf(&mut buf, "A1", &cmd, &opts(LiteralMode::LiteralPlus, true)),
        Err(crate::Error::InvalidInput(_))
    ));
    assert!(buf.is_empty());
}

#[test]
fn append_list_patterns_and_notify_keep_retained_wire_addresses() {
    use crate::types::{MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};
    let name = MailboxName::from_wire("a".into(), b"&AGE-", false);
    let commands = [
        Command::Append {
            mailbox: (&name).into(),
            messages: vec![crate::types::AppendMessage::new(bytes::Bytes::from_static(
                b"x",
            ))],
            multi: false,
        },
        Command::List {
            reference: "".into(),
            pattern: (&name).into(),
        },
        Command::ListExtended {
            selection_options: Vec::new(),
            reference: "".into(),
            patterns: vec![(&name).into(), "*".into()],
            return_options: Vec::new(),
        },
        Command::ListStatus {
            reference: "".into(),
            pattern: (&name).into(),
            status_items: "MESSAGES".into(),
        },
        Command::NotifySet(NotifySetParams::new(
            vec![NotifyEventGroup::new(
                MailboxFilter::Mailboxes(vec![(&name).into()]),
                vec![
                    NotifyEvent::MessageNew {
                        fetch_attrs: Vec::new(),
                    },
                    NotifyEvent::MessageExpunge,
                ],
            )],
            false,
        )),
    ];
    for command in commands {
        let mut bytes = BytesMut::new();
        encode_command_to_buf(
            &mut bytes,
            "A1",
            &command,
            &opts(LiteralMode::LiteralPlus, false),
        )
        .unwrap();
        assert!(
            bytes.windows(5).any(|window| window == b"&AGE-"),
            "{command:?}: {bytes:?}"
        );
    }
}

#[test]
fn a_mode_change_refuses_noncanonical_addresses_before_writing() {
    let mailbox = MailboxName::from_wire("a".into(), b"&AGE-", false);
    let mut bytes = BytesMut::new();
    let result = encode_command_to_buf(
        &mut bytes,
        "A1",
        &Command::Select {
            mailbox,
            condstore: false,
            qresync: None,
        },
        &opts(LiteralMode::LiteralPlus, true),
    );
    assert!(matches!(result, Err(crate::Error::InvalidInput(_))));
    assert!(bytes.is_empty());
}
