use crate::types::Addressing;
use bytes::BytesMut;

use super::*;
use crate::types::response::Capability;
use crate::types::validated::{MailboxName, SequenceSet};

/// Capability assertions exercise admission; the encoder itself is pure.
fn admit_and_encode(
    tag: &str,
    cmd: &Command,
    opts: &EncodeOptions,
) -> Result<WireCommand, crate::Error> {
    crate::connection::admit_for_encoding(cmd, opts)?;
    super::encode_command(tag, cmd, opts)
}

fn admit_and_encode_to_buf(
    buf: &mut BytesMut,
    tag: &str,
    cmd: &Command,
    opts: &EncodeOptions,
) -> Result<(), crate::Error> {
    buf.extend_from_slice(&admit_and_encode(tag, cmd, opts)?.to_vec());
    Ok(())
}

/// Build an [`EncodeOptions`] with the given literal mode, utf8 flag, and a
/// generous set of capabilities that allows all commands to encode.
fn opts(literal_mode: LiteralMode, utf8_mode: bool) -> EncodeOptions {
    EncodeOptions {
        utf8_mode: utf8_mode.into(),
        literal_mode,
        // Include all commonly needed capabilities so existing tests
        // do not fail due to the new prerequisite checks.
        session: crate::types::profile::SessionCapabilities::new(
            vec![
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
                Capability::from_imap_str("THREAD=REFERENCES"),
            ],
            Vec::new(),
        ),
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
    segment_bytes(&w.finish().expect("valid command"))
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
    w.finish().expect("valid command")
}

/// Run `write` against a writer for `mode` and return every byte it wrote.
fn fragment(mode: LiteralMode, write: impl FnOnce(&mut CommandWriter)) -> Vec<u8> {
    let mut w = CommandWriter::new(&opts(mode, false));
    write(&mut w);
    w.finish().expect("valid command").to_vec()
}

fn assert_string_nul_rejected(data: &[u8]) {
    let mut w = CommandWriter::new(&default_opts());
    w.raw(b"A001 X ");
    w.string(data, crate::types::wire::WireMode::ModifiedUtf7);
    w.raw(b"\r\n");
    assert!(matches!(
        w.finish(),
        Err(crate::Error::LocalRefusal(crate::error::LocalRefusal::InvalidInput(message))) if message.contains("must not contain NUL")
    ));
}

/// As [`fragment`], for a piece that can refuse.
fn try_fragment(
    mode: LiteralMode,
    write: impl FnOnce(&mut CommandWriter) -> Result<(), crate::Error>,
) -> Result<Vec<u8>, crate::Error> {
    let mut w = CommandWriter::new(&opts(mode, false));
    write(&mut w)?;
    Ok(w.finish()?.to_vec())
}

fn push_quoted_or_literal(buf: &mut BytesMut, data: &[u8], mode: LiteralMode) {
    buf.extend_from_slice(&fragment(mode, |w| {
        w.string(data, crate::types::wire::WireMode::ModifiedUtf7);
    }));
}

fn push_quoted_or_literal_utf8(buf: &mut BytesMut, data: &[u8], utf8: bool, mode: LiteralMode) {
    buf.extend_from_slice(&fragment(mode, |w| w.string(data, (utf8).into())));
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
        encode_login(w, tag, user, pass, (utf8).into())
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
        encode_select_or_examine(w, tag, cmd, mailbox, condstore, qresync, (utf8).into())
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
        encode_getmetadata(w, tag, mailbox, entries, max_size, depth, (utf8).into())
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
    std::sync::Arc::make_mut(&mut o.session)
        .capabilities
        .push(Capability::MultiAppend);
    if utf8 {
        std::sync::Arc::make_mut(&mut o.session)
            .enabled
            .insert(Capability::from_imap_str("UTF8=ACCEPT"));
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
    admit_and_encode(
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

// -----------------------------------------------------------------------
// Caller-written literals in SEARCH-family criteria
// -----------------------------------------------------------------------

fn uid_search(criteria: &str, opts: &EncodeOptions) -> Result<WireCommand, crate::Error> {
    admit_and_encode(
        "A001",
        &Command::Search {
            addressing: Addressing::Uids,
            criteria: criteria.into(),
        },
        opts,
    )
}

// ========================================================================
// Property-based round-trip and invariant tests
// ========================================================================

mod prop_roundtrip;

// ===== NOTIFY (RFC 5465) encoder tests =====

use crate::types::notify::{MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams};

mod append;

mod fetch_store;

mod mailbox;

mod metadata;

mod notify;

mod quota_acl;

mod search;

mod session;

mod strings;

mod validation;
