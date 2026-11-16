//! Byte-level account tests over the in-memory driver pair.
//!
//! These build a real `ImapAccount` around a `Pool` primed with a
//! `connection::test_support::driver_pair` connection, so an account
//! entry point is driven end to end against a canned server transcript.
//! Hermetic: `tokio::io::duplex`, no listener, no port, no daemon.

#![allow(clippy::unwrap_used, clippy::expect_used)]

pub(super) use std::sync::Arc;
pub(super) use std::time::Duration;

pub(super) use bifrost_types::{AccountErrorKind, AccountOperation, ResourceKind};
pub(super) use bifrost_types::{ContainerId, ContainerKind};

pub(super) use crate::connection::test_support::{
    driver_pair, preauth_greeting, read_line, respond, tag_of,
};

pub(super) use crate::account::folder_registry::FolderRegistry;
pub(super) use crate::account::test_support::scripted_account_parts;
pub(super) use crate::account::{ImapAccount, ImapAccountParts};

/// Build an account whose pool allows `pool_cap` physical sessions,
/// the primed scripted connection included. Fresh dials are refused locally.
/// At a single-session cap a nested checkout waits for the held member,
/// exposing accidental nested acquisition.
pub(super) fn scripted_account(conn: crate::ImapConnection, pool_cap: usize) -> ImapAccount {
    scripted_sync_account(conn, pool_cap, false, None, FolderRegistry::default())
}

/// `scripted_account` with the two knobs the sync half needs: whether
/// QRESYNC survived negotiation, and a registry pre-seeded with the folder
/// the transcript selects (an unregistered folder makes every cursor and
/// MODSEQ cache write a silent no-op).
pub(super) fn scripted_sync_account(
    conn: crate::ImapConnection,
    pool_cap: usize,
    qresync_enabled: bool,
    qresync_negotiation_warning: Option<String>,
    folders: FolderRegistry,
) -> ImapAccount {
    scripted_dav_account(
        conn,
        pool_cap,
        qresync_enabled,
        qresync_negotiation_warning,
        folders,
        Default::default(),
    )
}

/// `scripted_sync_account` with a composed-DAV ownership index, for pinning
/// how the mail lanes treat scopes a DAV sub-account owns.
pub(super) fn scripted_dav_account(
    conn: crate::ImapConnection,
    pool_cap: usize,
    qresync_enabled: bool,
    qresync_negotiation_warning: Option<String>,
    folders: FolderRegistry,
    dav_scopes: crate::account::DavScopeIndex,
) -> ImapAccount {
    ImapAccount::new(ImapAccountParts {
        folders: Arc::new(folders),
        qresync: if qresync_enabled {
            crate::account::QresyncNegotiation::Enabled
        } else if qresync_negotiation_warning.is_some() {
            crate::account::QresyncNegotiation::Refused(
                crate::account::QresyncRefusal::EnableNotConfirmed,
            )
        } else {
            crate::account::QresyncNegotiation::NotRequested
        },
        dav_scopes,
        ..scripted_account_parts(conn, pool_cap)
    })
}

/// A registry holding a SPECIAL-USE `\Sent` folder named `Sent`.
pub(super) fn sent_registry() -> FolderRegistry {
    FolderRegistry::from_list(vec![crate::types::MailboxInfo {
        name: crate::types::MailboxName::new("Sent").unwrap(),
        delimiter: Some('/'),
        attributes: vec![crate::types::MailboxAttribute::Sent],
        ..crate::types::MailboxInfo::default()
    }])
}

/// Drive the post-send Sent APPEND against a LITERAL+ server that reads the
/// literal and then answers with `reply` (tagged with the APPEND's tag), or
/// hangs up without answering when `reply` is `None`.
pub(super) async fn sent_append_receipt(reply: Option<&'static str>) -> bifrost_types::SendReceipt {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 LITERAL+ UIDPLUS")).await;
    let account = scripted_sync_account(conn, 1, false, None, sent_registry());
    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(header.contains("APPEND"), "expected APPEND, got {header}");
        let tag = tag_of(&header).to_owned();
        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        let _trailer = read_line(&mut server).await;
        if let Some(reply) = reply {
            respond(&mut server, &format!("{tag} {reply}\r\n")).await;
            Some(server)
        } else {
            drop(server);
            None
        }
    });
    let receipt = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::compose::append_to_sent_or_fallback(
            &account,
            bytes::Bytes::from_static(b"HELLO"),
            true,
            "m1@example.com",
        ),
    )
    .await
    .expect("the Sent APPEND must finish");
    let _server = script.await.unwrap();
    receipt
}

/// The saved draft, its `Date` the save time.
pub(super) const SAVED_DRAFT: &str = "Date: Mon, 1 Jan 2001 00:00:00 +0000\r\n\
    From: user@example.com\r\n\
    To: to@example.com\r\n\
    Message-ID: <d1@example.com>\r\n\
    Subject: hi\r\n\
    \r\n\
    body\r\n";

/// The bytes SMTP must carry: the saved `Date` replaced, first, by the
/// injected send time. Spelled out rather than derived from the restamp
/// helper, so the test pins the wire and not the helper's own idea of it.
pub(super) const SENT_DRAFT: &str = "Date: Sun, 9 Sep 2001 01:46:40 +0000\r\n\
    From: user@example.com\r\n\
    To: to@example.com\r\n\
    Message-ID: <d1@example.com>\r\n\
    Subject: hi\r\n\
    \r\n\
    body\r\n";

pub(super) fn draft_send_clock() -> jiff::Timestamp {
    jiff::Timestamp::from_second(1_000_000_000).unwrap()
}

/// How the scripted IMAP peer answers `draft_send`'s discard after the STORE.
pub(super) enum DiscardTail {
    /// Nothing more: the STORE was refused.
    None,
    /// A UIDPLUS server: `UID EXPUNGE`, answered with this reply.
    UidExpunge(&'static str),
    /// A server without UIDPLUS: `UID SEARCH DELETED`, answered with these
    /// UIDs, then a plain `EXPUNGE` (answered OK) only when `expunge` says
    /// the account must send one.
    Fallback {
        deleted: &'static str,
        expunge: bool,
    },
}

/// Drive `draft_send` end to end against a scripted IMAP peer and a scripted
/// SMTP peer: EXAMINE and FETCH the draft, submit it, then discard it with a
/// read-write SELECT and `UID STORE`, answering the STORE with `store_reply`
/// and the rest of the removal as `tail` says. No Sent copy is requested, so
/// the discard is the only post-send IMAP traffic.
pub(super) async fn draft_send_with_discard(
    store_reply: &'static str,
    expunge_reply: Option<&'static str>,
) -> Result<bifrost_types::SendReceipt, bifrost_types::AccountError> {
    let tail = match expunge_reply {
        Some(reply) => DiscardTail::UidExpunge(reply),
        None => DiscardTail::None,
    };
    draft_send_scripted("IMAP4rev1 UIDPLUS", store_reply, tail).await
}

pub(super) async fn draft_send_scripted(
    greeting_caps: &'static str,
    store_reply: &'static str,
    tail: DiscardTail,
) -> Result<bifrost_types::SendReceipt, bifrost_types::AccountError> {
    use bifrost_smtp::transport::smtp::extension::ClientId;
    use bifrost_smtp::transport::smtp::{ScriptedConnector, Transcript};

    let transcripts: Arc<std::sync::Mutex<Vec<Transcript>>> = Arc::default();
    let dialed = Arc::clone(&transcripts);
    let connector = ScriptedConnector::new(move || {
        let hello = format!("EHLO {}\r\n", ClientId::default());
        let transcript = Transcript::new("220 peer\r\n")
            .expect(&hello, "250-peer\r\n250 AUTH PLAIN\r\n")
            .expect("AUTH PLAIN AHVzZXIAcGFzcw==\r\n", "235 authenticated\r\n")
            .expect(&hello, "250 peer\r\n")
            .expect("MAIL FROM:<user@example.com>\r\n", "250 sender\r\n")
            .expect("RCPT TO:<to@example.com>\r\n", "250 recipient\r\n")
            .expect("DATA\r\n", "354 send\r\n")
            .expect(SENT_DRAFT, "")
            .expect(".\r\n", "250 delivered\r\n");
        dialed.lock().unwrap().push(transcript.clone());
        transcript
    });
    let cfg = crate::account::SmtpSubmissionConfig::new(
        "smtp.invalid",
        crate::account::SubmissionTls::Plaintext,
        bifrost_types::Address::bare("user@example.com"),
    )
    .with_save_to_sent_default(false);
    let submission = crate::account::submission::SubmissionTransport::build_with_connector(
        &cfg,
        &crate::types::Credentials::password("user", "pass"),
        None,
        bifrost_net::BandwidthBudget::default(),
        Some(connector),
    )
    .unwrap();

    let (conn, mut server) = driver_pair(&preauth_greeting(greeting_caps)).await;
    let mut parts = scripted_account_parts(conn, 1);
    parts.submission = Some(Arc::new(submission));
    let account = ImapAccount::new(parts);
    let script = tokio::spawn(async move {
        let selected = |tag: &str| {
            format!(
                "* 1 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Deleted)\r\n\
                 * OK [UIDVALIDITY 7] epoch\r\n{tag} OK selected\r\n"
            )
        };
        let line = read_line(&mut server).await;
        assert!(line.contains("EXAMINE"), "expected EXAMINE, got {line}");
        respond(&mut server, &selected(tag_of(&line))).await;
        let line = read_line(&mut server).await;
        assert!(line.contains("UID FETCH"), "expected UID FETCH, got {line}");
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 1 BODY[] {{{}}}\r\n{SAVED_DRAFT})\r\n{} OK fetched\r\n",
                SAVED_DRAFT.len(),
                tag_of(&line)
            ),
        )
        .await;
        let line = read_line(&mut server).await;
        assert!(line.contains("SELECT"), "expected SELECT, got {line}");
        respond(&mut server, &selected(tag_of(&line))).await;
        let line = read_line(&mut server).await;
        assert!(line.contains("UID STORE"), "expected UID STORE, got {line}");
        respond(&mut server, &format!("{} {store_reply}\r\n", tag_of(&line))).await;
        match tail {
            DiscardTail::None => {}
            DiscardTail::UidExpunge(expunge_reply) => {
                let line = read_line(&mut server).await;
                assert!(
                    line.contains("UID EXPUNGE"),
                    "expected UID EXPUNGE, got {line}"
                );
                respond(
                    &mut server,
                    &format!("{} {expunge_reply}\r\n", tag_of(&line)),
                )
                .await;
            }
            DiscardTail::Fallback { deleted, expunge } => {
                let line = read_line(&mut server).await;
                assert!(
                    line.contains("UID SEARCH DELETED"),
                    "expected UID SEARCH DELETED, got {line}"
                );
                respond(
                    &mut server,
                    &format!("* SEARCH {deleted}\r\n{} OK searched\r\n", tag_of(&line)),
                )
                .await;
                if expunge {
                    let line = read_line(&mut server).await;
                    assert!(
                        line.ends_with(" EXPUNGE\r\n") && !line.contains("UID"),
                        "expected a plain EXPUNGE, got {line}"
                    );
                    respond(
                        &mut server,
                        &format!("* 1 EXPUNGE\r\n{} OK expunged\r\n", tag_of(&line)),
                    )
                    .await;
                }
            }
        }
        server
    });
    let draft = crate::account::encode_object_id(
        &crate::types::MailboxName::new("Drafts").unwrap(),
        crate::types::UidValidity::new(7).expect("nonzero epoch"),
        crate::types::Uid::new(1).expect("valid protocol test value"),
    );
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::compose::draft_send_with_clock(
            account,
            bifrost_types::DraftHandle(draft.0),
            draft_send_clock,
        ),
    )
    .await
    .expect("draft_send must finish");
    let _server = script.await.unwrap();
    let transcripts = transcripts.lock().unwrap();
    assert_eq!(transcripts.len(), 1, "one SMTP session");
    transcripts[0].assert_exhausted();
    outcome
}

pub(super) fn assert_discard_failure(
    receipt: &bifrost_types::SendReceipt,
) -> &bifrost_types::AccountError {
    assert!(matches!(
        receipt.sent_copy,
        bifrost_types::SentCopy::NotRequested
    ));
    let failure = receipt
        .draft_discard
        .as_ref()
        .expect("a failed discard rides the receipt");
    let draft = crate::account::encode_object_id(
        &crate::types::MailboxName::new("Drafts").unwrap(),
        crate::types::UidValidity::new(7).expect("nonzero epoch"),
        crate::types::Uid::new(1).expect("valid protocol test value"),
    );
    assert_eq!(failure.draft.0, draft.0);
    assert_eq!(
        failure.error.operation(),
        Some(AccountOperation::DraftDiscard)
    );
    &failure.error
}

/// A caller-assembled message carrying a Bcc and an old, unsigned `Date`.
pub(super) const RAW_WITH_BCC: &str = "Date: Mon, 1 Jan 2001 00:00:00 +0000\r\n\
    From: user@example.com\r\n\
    To: a@example.com\r\n\
    Bcc: b@example.com\r\n\
    Message-ID: <r1@example.com>\r\n\
    Subject: hi\r\n\
    \r\n\
    body\r\n";

/// What SMTP must carry for `RAW_WITH_BCC`: the Bcc line stripped, the
/// caller's `Date` untouched.
pub(super) const RAW_ON_THE_WIRE: &str = "Date: Mon, 1 Jan 2001 00:00:00 +0000\r\n\
    From: user@example.com\r\n\
    To: a@example.com\r\n\
    Message-ID: <r1@example.com>\r\n\
    Subject: hi\r\n\
    \r\n\
    body\r\n";

/// How the scripted IMAP peer treats the post-send Sent APPEND.
pub(super) enum SentPeer {
    /// No IMAP traffic is expected at all.
    Silent,
    /// Read the APPEND and its literal, then answer with this tagged reply.
    Reply(&'static str),
    /// Read the APPEND and its literal, then hang up without answering.
    HangUp,
}

/// Which send entry the harness drives.
pub(super) enum SendCall {
    /// `send_message` through the render seam, wrapping the real renderer.
    Message(Box<bifrost_types::SendRequest>),
    /// `send_raw_message` with these bytes, the wire body SMTP must see, and
    /// the per-call `save_to_sent`.
    Raw {
        raw: &'static str,
        wire: &'static str,
        save_to_sent: Option<bool>,
    },
}

/// What one scripted send produced.
pub(super) struct SentThrough {
    pub(super) outcome: Result<bifrost_types::SendReceipt, bifrost_types::AccountError>,
    /// The real renderer's output, for a `send_message` call.
    pub(super) rendered: Option<bifrost_types::RenderedMessage>,
    /// The APPEND literal the IMAP peer received, when it read one.
    pub(super) appended: Option<Vec<u8>>,
}

/// Drive a send entry end to end against a scripted SMTP peer answering
/// `rcpts` (address, reply) in envelope order and a scripted IMAP peer
/// treating the Sent APPEND as `sent` says. When any RCPT is accepted the
/// SMTP peer takes DATA and expects the wire body byte for byte: for
/// `send_message` that is the body the real renderer produced, recorded by
/// the render closure and read when the connector dials, which happens only
/// after render (the pool keeps no idle connections, so nothing dials at
/// build). With every RCPT refused it expects the RSET instead.
pub(super) async fn send_through_peers(
    call: SendCall,
    rcpts: &'static [(&'static str, &'static str)],
    save_to_sent_default: bool,
    sent: SentPeer,
) -> SentThrough {
    use bifrost_smtp::transport::smtp::extension::ClientId;
    use bifrost_smtp::transport::smtp::{PoolConfig, ScriptedConnector, Transcript};

    let rendered: Arc<std::sync::Mutex<Option<bifrost_types::RenderedMessage>>> = Arc::default();
    let raw_wire = match &call {
        SendCall::Message(_) => None,
        SendCall::Raw { wire, .. } => Some(*wire),
    };
    let transcripts: Arc<std::sync::Mutex<Vec<Transcript>>> = Arc::default();
    let dialed = Arc::clone(&transcripts);
    let seen = Arc::clone(&rendered);
    let connector = ScriptedConnector::new(move || {
        let hello = format!("EHLO {}\r\n", ClientId::default());
        let mut transcript = Transcript::new("220 peer\r\n")
            .expect(&hello, "250-peer\r\n250 AUTH PLAIN\r\n")
            .expect("AUTH PLAIN AHVzZXIAcGFzcw==\r\n", "235 authenticated\r\n")
            .expect(&hello, "250 peer\r\n")
            .expect("MAIL FROM:<user@example.com>\r\n", "250 sender\r\n");
        for (address, reply) in rcpts {
            transcript = transcript.expect(format!("RCPT TO:<{address}>\r\n"), *reply);
        }
        if rcpts.iter().any(|(_, reply)| reply.starts_with('2')) {
            let wire = match raw_wire {
                Some(wire) => wire.as_bytes().to_vec(),
                None => seen
                    .lock()
                    .unwrap()
                    .as_ref()
                    .expect("render completed before the SMTP dial")
                    .raw
                    .clone(),
            };
            transcript = transcript
                .expect("DATA\r\n", "354 send\r\n")
                .expect(wire, "")
                .expect(".\r\n", "250 delivered\r\n");
        } else {
            transcript = transcript.expect("RSET\r\n", "250 reset ok\r\n");
        }
        dialed.lock().unwrap().push(transcript.clone());
        transcript
    });
    let cfg = crate::account::SmtpSubmissionConfig::new(
        "smtp.invalid",
        crate::account::SubmissionTls::Plaintext,
        bifrost_types::Address::bare("user@example.com"),
    )
    .with_save_to_sent_default(save_to_sent_default)
    .with_pool(PoolConfig::new().min_idle(0));
    let submission = crate::account::submission::SubmissionTransport::build_with_connector(
        &cfg,
        &crate::types::Credentials::password("user", "pass"),
        None,
        bifrost_net::BandwidthBudget::default(),
        Some(connector),
    )
    .unwrap();

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 LITERAL+ UIDPLUS")).await;
    let mut parts = ImapAccountParts {
        folders: Arc::new(sent_registry()),
        ..scripted_account_parts(conn, 1)
    };
    parts.submission = Some(Arc::new(submission));
    let account = ImapAccount::new(parts);
    let reply = match sent {
        SentPeer::Silent => None,
        SentPeer::Reply(reply) => Some(Some(reply)),
        SentPeer::HangUp => Some(None),
    };
    let script = tokio::spawn(async move {
        let Some(reply) = reply else {
            return (Some(server), None);
        };
        let header = read_line(&mut server).await;
        assert!(
            header.contains(" APPEND \"Sent\" (\\Seen) {"),
            "expected a Seen APPEND to Sent, got {header}"
        );
        let tag = tag_of(&header).to_owned();
        let size = header
            .rsplit_once('{')
            .and_then(|(_, rest)| rest.split_once("+}"))
            .map(|(size, _)| size.parse::<usize>().unwrap())
            .expect("a LITERAL+ APPEND literal");
        let body = crate::connection::test_support::read_exact(&mut server, size).await;
        assert_eq!(
            read_line(&mut server).await,
            "\r\n",
            "the APPEND ends at its literal"
        );
        if let Some(reply) = reply {
            respond(&mut server, &format!("{tag} {reply}\r\n")).await;
            (Some(server), Some(body))
        } else {
            drop(server);
            (None, Some(body))
        }
    });
    // Held past the send, so the account's own teardown traffic cannot be
    // mistaken for, or hide, a command the send wrote.
    let held = account.clone();
    let send = match call {
        SendCall::Message(request) => {
            let record = Arc::clone(&rendered);
            crate::account::compose::send_message_with_render(
                account,
                *request,
                move |request, from| {
                    let out = bifrost_types::send_request_to_rfc5322(request, from)?;
                    *record.lock().unwrap() = Some(out.clone());
                    Ok(out)
                },
            )
        }
        SendCall::Raw {
            raw, save_to_sent, ..
        } => crate::account::compose::send_raw_message(
            account,
            bytes::Bytes::from_static(raw.as_bytes()),
            save_to_sent,
        ),
    };
    let outcome = tokio::time::timeout(Duration::from_secs(5), send)
        .await
        .expect("the send must finish");
    let (server, appended) = script.await.unwrap();
    if let Some(mut server) = server.filter(|_| appended.is_none()) {
        // The send is finished, so any IMAP command it wrote is buffered.
        let mut byte = [0u8; 1];
        let read =
            futures::FutureExt::now_or_never(tokio::io::AsyncReadExt::read(&mut server, &mut byte));
        assert!(read.is_none(), "no IMAP traffic expected, read {read:?}");
    }
    drop(held);
    let transcripts = transcripts.lock().unwrap();
    assert_eq!(transcripts.len(), 1, "one SMTP session, never re-driven");
    transcripts[0].assert_exhausted();
    let rendered = rendered.lock().unwrap().take();
    SentThrough {
        outcome,
        rendered,
        appended,
    }
}

/// The header block of a message: everything before the blank line.
pub(super) fn header_block(message: &[u8]) -> &str {
    let text = std::str::from_utf8(message).unwrap();
    text.split_once("\r\n\r\n").unwrap().0
}

/// The message's only `Date:` line, asserted to be its first header.
pub(super) fn sole_first_date(message: &[u8]) -> &str {
    let headers = header_block(message);
    let dates: Vec<&str> = headers
        .split("\r\n")
        .filter(|line| line.starts_with("Date: "))
        .collect();
    assert_eq!(dates.len(), 1, "exactly one Date header: {headers}");
    assert!(
        headers.starts_with(dates[0]),
        "Date is the first header: {headers}"
    );
    dates[0]
}

/// A plain-text request to `to` with a subject.
pub(super) fn text_request(to: &str) -> bifrost_types::SendRequest {
    let mut request = bifrost_types::SendRequest::default();
    request.to = vec![bifrost_types::Address::bare(to)];
    request.subject = Some("hi".into());
    request.body_text = Some("body".into());
    request
}

pub(super) fn assert_refused(receipt: &bifrost_types::SendReceipt, address: &str) {
    assert_eq!(receipt.recipients_not_accepted.len(), 1);
    let refused = &receipt.recipients_not_accepted[0];
    assert_eq!(refused.address.address, address);
    assert_eq!(refused.outcome, bifrost_types::RecipientOutcome::Failed);
    assert_eq!(refused.error.operation(), Some(AccountOperation::Send));
}

pub(super) fn created_name() -> crate::types::MailboxName {
    crate::types::MailboxName::new("Archive".to_owned()).unwrap()
}

/// Drain a mutation stream to `Done`, returning one entry per emitted
/// `Batch` holding that batch's item ids. Batch count and per-id
/// multiplicity are both readable from the result.
pub(super) async fn drain_mutation_batches(
    mut output: bifrost_types::AccountStream<
        bifrost_types::SyncEvent<bifrost_types::ItemOutcome<bifrost_types::MutationSuccess>>,
    >,
) -> Vec<Vec<String>> {
    use bifrost_types::SyncEvent;
    use futures::StreamExt;

    let mut batches = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("stream must make progress")
    {
        match event {
            SyncEvent::Batch(batch) => {
                batches.push(
                    batch
                        .items
                        .iter()
                        .map(|outcome| match outcome {
                            bifrost_types::ItemOutcome::Failed(item) => item.item.0.clone(),
                            bifrost_types::ItemOutcome::Uncertain(item) => item.item.0.clone(),
                            bifrost_types::ItemOutcome::Succeeded(item) => item.item.0.clone(),
                        })
                        .collect(),
                );
            }
            SyncEvent::Done(_) => break,
            unexpected => panic!("unexpected event: {unexpected:?}"),
        }
    }
    batches
}

/// Assert every id in `expected` appears in exactly one batch item across
/// `batches`, and that the emitted batch count stayed within the buffering
/// window rather than degenerating to one batch per id.
pub(super) fn assert_batched_exactly_once(
    batches: &[Vec<String>],
    expected: &[String],
    max_batches: usize,
) {
    let mut seen: Vec<String> = batches.iter().flatten().cloned().collect();
    seen.sort();
    let mut want = expected.to_vec();
    want.sort();
    assert_eq!(seen, want, "every id owes exactly one outcome");
    assert!(
        batches.len() <= max_batches,
        "expected at most {max_batches} batches for {} ids, got {}",
        expected.len(),
        batches.len()
    );
}

pub(super) fn rev2_select_ok(tag: &str, mailbox: &str) -> String {
    format!(
        "* FLAGS (\\Deleted \\Seen)\r\n\
         * 0 EXISTS\r\n\
         * LIST () \"/\" {mailbox}\r\n\
         * OK [UIDVALIDITY 1] selected\r\n\
         {tag} OK [READ-WRITE] SELECT done\r\n"
    )
}

// ---------------------------------------------------------------------------
// Sync half: strategy dispatch, VANISHED/FETCH dedup, downgrades, checkpoints.
//
// Every test below drives the real `changes_stream` over a canned transcript,
// so the strategy a cursor variant selects is proven by the bytes that reach
// the server, not by reading the dispatch match.
// ---------------------------------------------------------------------------

pub(super) use bifrost_types::{
    Change, ChangeCursor, Checkpoint, ObjectChange, ObjectChangeKind, PageBoundary, ScopeChange,
    ScopeChangeKind, SyncEvent, SyncStrategy, Warning, WarningKind,
};

pub(super) use crate::types::MailboxInfo;

pub(super) use crate::account::{FolderCursor, UidSet};

pub(super) fn inbox() -> crate::types::MailboxName {
    crate::types::MailboxName::new("INBOX").unwrap()
}

/// A registry holding just INBOX, so cursor and MODSEQ cache writes land.
/// An unregistered folder makes every registry write a silent no-op.
pub(super) fn inbox_registry() -> FolderRegistry {
    FolderRegistry::from_list(vec![MailboxInfo {
        name: inbox(),
        ..MailboxInfo::default()
    }])
}

/// The mandatory SELECT/EXAMINE preamble (RFC 3501 Sections 6.3.1-6.3.2).
///
/// `SelectConsumer` validates FLAGS + EXISTS + RECENT before it reads a
/// single response code, so a transcript that omits them fails the command
/// with `crate::error::ProviderFailure::Protocol` and the whole changes stream terminates before
/// any strategy work runs. Every select transcript below starts here.
pub(super) const SELECT_PREAMBLE: &str =
    "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* 0 RECENT\r\n";

pub(super) fn cursor_for(cursor: &FolderCursor) -> ChangeCursor {
    crate::account::encode_cursor(crate::account::folder_scope(&inbox()), cursor)
}

/// Drive `changes_stream` to completion, bounded so a regression that stalls
/// the pipeline fails fast instead of hanging the suite.
pub(super) async fn collect_changes(
    account: ImapAccount,
    cursor: ChangeCursor,
) -> Vec<SyncEvent<Change>> {
    use futures::StreamExt;

    let mut stream = crate::account::changes::changes_stream(account, cursor);
    let mut events = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("changes stream must make progress");
        let Some(event) = next else { break };
        let terminal = matches!(event, SyncEvent::Done(_) | SyncEvent::Terminated(_));
        events.push(event);
        if terminal {
            break;
        }
    }
    events
}

/// The change lanes flattened in emission order: `(object id, lane)`.
pub(super) fn change_labels(events: &[SyncEvent<Change>]) -> Vec<(String, &'static str)> {
    let mut out = Vec::new();
    for event in events {
        let SyncEvent::Batch(batch) = event else {
            continue;
        };
        for change in &batch.items {
            match change {
                Change::ScopeChange(ScopeChange {
                    id,
                    kind: ScopeChangeKind::Added,
                    ..
                }) => out.push((id.0.clone(), "added")),
                Change::ScopeChange(ScopeChange {
                    id,
                    kind: ScopeChangeKind::Removed,
                    ..
                }) => out.push((id.0.clone(), "removed")),
                Change::ObjectChange(ObjectChange {
                    id,
                    kind: ObjectChangeKind::Updated,
                }) => out.push((id.0.clone(), "updated")),
                other => panic!("unexpected change: {other:?}"),
            }
        }
    }
    out
}

pub(super) fn warnings_of(events: &[SyncEvent<Change>]) -> Vec<&Warning> {
    events
        .iter()
        .filter_map(|event| match event {
            SyncEvent::Warning(warning) => Some(warning),
            _ => None,
        })
        .collect()
}

/// The cursor carried by the terminal `Done`, decoded back to a `FolderCursor`.
pub(super) fn done_cursor(events: &[SyncEvent<Change>]) -> FolderCursor {
    let last = events.last().expect("stream emits a terminal event");
    let SyncEvent::Done(checkpoint) = last else {
        panic!("expected Done, got {last:?}");
    };
    let Some(Checkpoint::Change(cursor)) = checkpoint else {
        panic!("changes must checkpoint a change cursor");
    };
    crate::account::decode_cursor(cursor).expect("emitted checkpoint must decode")
}

pub(super) fn id(uidvalidity: crate::types::UidValidity, uid: u32) -> String {
    crate::account::encode_object_id(
        &inbox(),
        uidvalidity,
        crate::types::Uid::new(uid).expect("nonzero test UID"),
    )
    .0
}

pub(super) async fn qresync_delivery_transcript(expunge: bool, search_fails: bool) {
    let (conn, mut server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    let enable = crate::account::pool::enable_qresync(&conn, Duration::from_secs(5));
    let answer = async {
        let command = read_line(&mut server).await;
        assert!(command.contains("ENABLE QRESYNC"), "{command}");
        respond(
            &mut server,
            &format!("* ENABLED QRESYNC\r\n{} OK enabled\r\n", tag_of(&command)),
        )
        .await;
    };
    let (enabled, ()) = tokio::join!(enable, answer);
    enabled.unwrap();
    let account = scripted_sync_account(conn, 1, true, None, inbox_registry());
    let baseline = FolderCursor::QResync {
        uidvalidity: crate::types::UidValidity::new(5).expect("nonzero epoch"),
        modseq: crate::types::ModSeq::new(100).expect("valid protocol test value"),
        baseline: crate::account::envelope::UidBaseline::Complete(UidSet::from_numbers([1, 2])),
    };
    account.folders.set_cursor(&inbox(), baseline.clone());
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("QRESYNC (5 100 1:2)"), "{select}");
        let exists = if expunge { 2 } else { 3 };
        let vanished = if expunge {
            "* VANISHED (EARLIER) 1\r\n"
        } else {
            ""
        };
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * {exists} EXISTS\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 4] ok\r\n\
                 * OK [HIGHESTMODSEQ 200] ok\r\n\
                 {vanished}{} OK [READ-ONLY] selected\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let search = read_line(&mut server).await;
        assert!(search.contains("UID SEARCH ALL"), "{search}");
        let reply = if search_fails {
            format!("{} NO discovery failed\r\n", tag_of(&search))
        } else {
            let uids = if expunge { "2 3" } else { "1 2 3" };
            format!("* SEARCH {uids}\r\n{} OK searched\r\n", tag_of(&search))
        };
        respond(&mut server, &reply).await;
        server
    });
    let events = collect_changes(account.clone(), cursor_for(&baseline)).await;
    if search_fails {
        assert!(matches!(events.last(), Some(SyncEvent::Terminated(_))));
        assert!(!events.iter().any(|event| match event {
            SyncEvent::Done(_) => true,
            SyncEvent::Batch(batch) => batch.checkpoint.is_some(),
            _ => false,
        }));
        assert!(matches!(
            account.folders.get(&inbox()).unwrap().cursor(),
            Some(FolderCursor::QResync { modseq, .. }) if modseq.get() == 100
        ));
    } else {
        let expected = if expunge {
            vec![
                (
                    id(
                        crate::types::UidValidity::new(5).expect("valid protocol test value"),
                        1,
                    ),
                    "removed",
                ),
                (
                    id(
                        crate::types::UidValidity::new(5).expect("valid protocol test value"),
                        3,
                    ),
                    "added",
                ),
            ]
        } else {
            vec![(
                id(
                    crate::types::UidValidity::new(5).expect("valid protocol test value"),
                    3,
                ),
                "added",
            )]
        };
        assert_eq!(change_labels(&events), expected);
        assert!(warnings_of(&events).is_empty());
        match done_cursor(&events) {
            FolderCursor::QResync {
                modseq,
                baseline: crate::account::envelope::UidBaseline::Complete(known_uids),
                ..
            } => {
                assert_eq!(
                    modseq,
                    crate::types::ModSeq::new(200).expect("valid protocol test value")
                );
                let expected_uids = if expunge { vec![2, 3] } else { vec![1, 2, 3] };
                assert_eq!(known_uids.to_uids(), expected_uids);
            }
            other => panic!("expected QRESYNC checkpoint, got {other:?}"),
        }
    }
    let _server = script.await.unwrap();
}

/// Drive `open_raw_rfc822` for `imap2 INBOX 5:7` against one scripted
/// EXAMINE (UIDVALIDITY 5) and one scripted FETCH answer, returning every
/// event the stream yields.
pub(super) async fn raw_read_events(fetch_answer: &'static str) -> Vec<SyncEvent<bytes::Bytes>> {
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Seen)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let fetch = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("{fetch_answer}{} OK FETCH done\r\n", tag_of(&fetch)),
        )
        .await;
        server
    });
    let mut stream = crate::account::blob::open_raw_rfc822(
        account,
        crate::account::encode_object_id(
            &folder,
            crate::types::UidValidity::new(5).expect("nonzero epoch"),
            crate::types::Uid::new(7).expect("valid protocol test value"),
        ),
    );
    let mut events = Vec::new();
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the stream must finish")
    {
        events.push(event);
    }
    let _server = script.await.unwrap();
    events
}

/// Run one poller call against a scripted EXAMINE answering `select_codes`
/// (the untagged lines before the tagged OK) and then the scripted answers to
/// each further command in order, returning the command lines the server saw
/// after the EXAMINE.
pub(super) async fn poller_transcript<T, F, Fut>(
    select_codes: &'static str,
    answers: Vec<&'static str>,
    call: F,
) -> (T, Vec<String>)
where
    F: FnOnce(crate::account::ImapPoller) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let poller = crate::account::ImapPoller::new(scripted_account(conn, 1));
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("EXAMINE"), "expected EXAMINE, got {select}");
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Seen)\r\n* 0 RECENT\r\n{select_codes}{} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let mut seen = Vec::new();
        for answer in answers {
            let line = read_line(&mut server).await;
            respond(
                &mut server,
                &format!("{answer}{} OK done\r\n", tag_of(&line)),
            )
            .await;
            seen.push(line);
        }
        (server, seen)
    });
    let out = tokio::time::timeout(Duration::from_secs(5), call(poller))
        .await
        .expect("the poller call must finish");
    let (_server, seen) = script.await.unwrap();
    (out, seen)
}

pub(super) const POLL_SELECT: &str =
    "* 4 EXISTS\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 9] ok\r\n";

pub(super) fn nz<T: TryFrom<u32>>(value: u32) -> T
where
    T::Error: std::fmt::Debug,
{
    T::try_from(value).unwrap()
}

/// Drive one PIM entry point against a server whose SELECT/EXAMINE completes
/// without the mandatory `UIDVALIDITY`, and return the error it surfaces.
pub(super) async fn pim_error_on_select_without_uidvalidity<T: std::fmt::Debug>(
    call: impl FnOnce(
        ImapAccount,
    ) -> bifrost_types::AccountFuture<Result<T, bifrost_types::AccountError>>,
) -> bifrost_types::AccountError {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let mut caps = crate::account::test_support::stub_capabilities();
    caps.pim_methods.add_to_container = true;
    caps.pim_methods.message_hydrate = true;
    caps.pim_methods.search = true;
    caps.pim_methods.search_messages = true;
    caps.pim_methods.remove_from_container = true;
    caps.pim_methods.set_is_read = true;
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\"") || select.contains("SELECT \"INBOX\""),
            "expected a select of INBOX, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        server
    });

    let err = tokio::time::timeout(Duration::from_secs(5), call(account))
        .await
        .expect("the PIM call must not hang")
        .expect_err("a SELECT without UIDVALIDITY cannot yield usable ids");
    let _server = script.await.unwrap();
    err
}

/// Drive one search page call against a server that selects INBOX at
/// UIDVALIDITY 7 and answers the search command (`UID SEARCH` or
/// `UID THREAD`) with `untagged`.
pub(super) async fn search_page_against<T: std::fmt::Debug>(
    untagged: &'static str,
    call: impl FnOnce(
        ImapAccount,
    ) -> bifrost_types::AccountFuture<Result<T, bifrost_types::AccountError>>,
) -> T {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 THREAD=REFERENCES")).await;
    let mut caps = crate::account::test_support::stub_capabilities();
    caps.pim_methods.search = true;
    caps.pim_methods.search_messages = true;
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\"") || select.contains("SELECT \"INBOX\""),
            "expected a select of INBOX, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 4 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 7] ok\r\n\
                 * OK [UIDNEXT 200] ok\r\n\
                 {} OK done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH") || search.contains("UID THREAD"),
            "expected the search command, got {search}"
        );
        respond(
            &mut server,
            &format!("{untagged}\r\n{} OK done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let page = tokio::time::timeout(Duration::from_secs(5), call(account))
        .await
        .expect("the search call must not hang")
        .expect("the search page");
    let _server = script.await.unwrap();
    page
}

/// Run `message_hydrate` for INBOX uid 3 (UIDVALIDITY 5) against a server that
/// selects INBOX at UIDVALIDITY 5 and answers the UID FETCH with `fetch_data`
/// (untagged FETCH lines, possibly none) before its tagged OK.
pub(super) async fn hydrate_uid_3_against(
    fetch_data: &'static str,
) -> Result<bifrost_types::hydration::Message, bifrost_types::AccountError> {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let mut caps = crate::account::test_support::stub_capabilities();
    caps.pim_methods.message_hydrate = true;
    let account = ImapAccount::new(ImapAccountParts {
        capabilities: caps,
        ..scripted_account_parts(conn, 1)
    });

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\""),
            "expected EXAMINE, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Seen)\r\n\
                 * 9 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 10] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("UID FETCH 3"),
            "expected UID FETCH 3, got {fetch}"
        );
        respond(
            &mut server,
            &format!("{fetch_data}{} OK done\r\n", tag_of(&fetch)),
        )
        .await;
        server
    });

    let inbox = crate::types::MailboxName::new("INBOX").unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        crate::account::message::message_hydrate(
            account,
            crate::account::encode_object_id(
                &inbox,
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(3).expect("valid protocol test value"),
            ),
            bifrost_types::hydration::HydrationProjection::Headers,
        ),
    )
    .await
    .expect("message_hydrate must not hang");
    let _server = script.await.unwrap();
    result
}

pub(super) const SESSION_GREETING: &[u8] =
    b"* OK [CAPABILITY IMAP4rev1 AUTH=PLAIN SASL-IR] ready\r\n";

pub(in crate::account) async fn session_account(
    cap: usize,
    count: usize,
    notify: bool,
) -> (
    ImapAccount,
    tokio::io::DuplexStream,
    crate::account::test_support::ScriptedDials,
) {
    session_account_with_budget(cap, count, notify, 1).await
}

pub(super) async fn session_account_with_budget(
    cap: usize,
    count: usize,
    notify: bool,
    idle_connection_budget: usize,
) -> (
    ImapAccount,
    tokio::io::DuplexStream,
    crate::account::test_support::ScriptedDials,
) {
    let (conn, peer) = driver_pair(&preauth_greeting("IMAP4rev1 ACL IDLE")).await;
    let (connector, dials) =
        crate::account::test_support::scripted_connector(vec![SESSION_GREETING.to_vec(); count]);
    let config = crate::account::factory::ImapAccountConfig {
        idle_connection_budget,
        ..crate::account::test_support::scripted_account_config(cap)
    };
    let mut parts =
        crate::account::test_support::scripted_account_parts_from(conn, config, connector);
    parts.supports_notify = notify;
    (ImapAccount::new(parts), peer, dials)
}

pub(super) async fn session_auth(peer: &mut tokio::io::DuplexStream, auth: &str) {
    session_auth_with(peer, auth, "IMAP4rev1 IDLE UNSELECT BINARY").await;
}

pub(super) async fn session_auth_with(peer: &mut tokio::io::DuplexStream, auth: &str, caps: &str) {
    assert!(auth.contains("AUTHENTICATE PLAIN"), "{auth}");
    respond(
        peer,
        &format!("{} OK [CAPABILITY {caps}] done\r\n", tag_of(auth)),
    )
    .await;
}

pub(super) async fn session_select(peer: &mut tokio::io::DuplexStream, folder: &str) {
    let select = read_line(peer).await;
    assert!(
        select.contains("SELECT") && select.contains(folder),
        "{select}"
    );
    respond(peer, &format!("* FLAGS (\\Seen)\r\n* 1 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 1] valid\r\n{} OK [READ-WRITE] selected\r\n", tag_of(&select))).await;
}

pub(super) async fn session_idle(peer: &mut tokio::io::DuplexStream) {
    let idle = read_line(peer).await;
    assert!(idle.contains("IDLE"), "{idle}");
    respond(peer, "+ idling\r\n").await;
}

pub(super) fn push_folder(name: &str) -> bifrost_types::CursorScope {
    bifrost_types::CursorScope::Folder(bifrost_types::FolderId(name.into()))
}

pub(super) fn drive_push_until_shutdown(
    account: &crate::account::ImapAccount,
) -> tokio::task::JoinHandle<()> {
    use bifrost_types::Account;
    use futures::StreamExt;
    let mut session = account.push_stream(
        bifrost_types::PushLane(0),
        bifrost_types::SessionStop::never(),
        bifrost_types::PushGate::open(),
    );
    tokio::spawn(async move { while session.next().await.is_some() {} })
}

pub(super) async fn completed_push_round_and_stop(healthy: bool) {
    use bifrost_types::{
        Account, PushGate, PushLane, PushSessionEnd as End, PushSessionEvent as E, SessionStop,
    };
    use futures::StreamExt;
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let (stopper, stop) = SessionStop::pair();
    let mut stopper = Some(stopper);
    let mut session = account.push_stream(PushLane(0), stop, PushGate::open());
    let setup = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth(&mut server, &auth).await;
        session_select(&mut server, "INBOX").await;
        server
    };
    let (event, mut server) = tokio::join!(session.next(), setup);
    assert!(matches!(event, Some(E::Established)));
    let script = async {
        let idle = read_line(&mut server).await;
        assert!(idle.contains("IDLE"));
        respond(&mut server, "+ idling\r\n").await;
        if healthy {
            respond(&mut server, "* 2 EXISTS\r\n").await;
        } else {
            stopper.take().unwrap().stop();
        }
        let done = read_line(&mut server).await;
        assert!(done.contains("DONE"), "{done}");
        respond(
            &mut server,
            &format!("{} OK idle complete\r\n", tag_of(&idle)),
        )
        .await;
        if !healthy {
            let logout = read_line(&mut server).await;
            assert!(logout.contains("LOGOUT"));
            respond(
                &mut server,
                &format!("* BYE done\r\n{} OK logged out\r\n", tag_of(&logout)),
            )
            .await;
        }
    };
    let (event, ()) = tokio::join!(session.next(), script);
    if healthy {
        assert!(matches!(event, Some(E::Healthy)));
    } else {
        assert!(matches!(event, Some(E::Ended(End::Stopped))));
    }
    drop(session);
    drop(server);
    drop(peer);
    account.close().await.unwrap();
}

/// Everything the thread-default `tracing` subscriber writes, for asserting
/// on a warning the code under test emits.
#[derive(Clone, Default)]
pub(super) struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

impl CapturedLogs {
    pub(super) fn install(&self) -> tracing::subscriber::DefaultGuard {
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(self.clone())
                .with_ansi(false)
                .finish(),
        )
    }
    pub(super) fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("captured log buffer poisoned"))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

pub(super) async fn next_events_until(
    session: &mut bifrost_types::AccountStream<bifrost_types::PushSessionEvent>,
    last: fn(&bifrost_types::PushSessionEvent) -> bool,
) -> Vec<bifrost_types::PushSessionEvent> {
    use futures::StreamExt as _;
    let mut events = Vec::new();
    while let Some(event) = session.next().await {
        let done = last(&event);
        events.push(event);
        if done {
            break;
        }
    }
    events
}
