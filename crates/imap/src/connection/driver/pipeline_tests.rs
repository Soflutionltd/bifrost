//! Byte-level transcripts for the pipeline send phase.
//!
//! These all exercise one window: a pipelined batch in `Synchronizing`
//! literal mode, where the commands go out one at a time and the batch is
//! blocked mid-send waiting for a `+` continuation. Responses belonging to
//! EARLIER commands arrive in that window, and before the read-time router
//! landed the send phase had no way to reach their consumers.
//!
//! Getting a batch into that window is fiddly and the constraints are worth
//! stating, because a transcript that misses any of them exercises nothing:
//!
//! * the greeting must advertise neither `LITERAL+` nor `LITERAL-` nor
//!   `IMAP4rev2`, or the literals are non-synchronizing and the whole batch
//!   goes out in one write with no wait to interrupt;
//! * the two commands must have DISTINCT untagged routing keys
//!   (`CommandKind::untagged_routing_key`), or `group_into_sub_batches` splits
//!   them into sequential sub-batches and there is never more than one tag
//!   outstanding;
//! * the SECOND command must carry a synchronizing literal. Most pipelinable
//!   commands cannot produce one: mailbox names go through mUTF-7 and come
//!   out ASCII-quotable. `LISTRIGHTS` can, because its identifier is passed
//!   through raw, so a non-ASCII identifier forces a literal.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::connection::test_support::{
    driver_pair, preauth_greeting, read_exact, read_line, respond, select_inbox, tag_of,
};
use crate::types::validated::MailboxName;

/// A capability set with no literal extension and no `IMAP4rev2`, so
/// `build_encode_options` selects `LiteralMode::Synchronizing`.
const SYNC_LITERAL_CAPS: &str = "IMAP4rev1 ACL";

/// The identifier that forces the literal: 5 UTF-8 bytes, not ASCII.
const LITERAL_IDENTIFIER: &str = "\u{fc}ser";

/// An untagged response solicited by an EARLIER pipelined command, arriving
/// while a later command waits for its literal continuation, reaches the
/// consumer that asked for it.
///
/// This is the silent half of the defect. The send phase used to answer
/// untagged responses through the consumerless arm, which downgrades them to
/// an anonymous typed event. The batch then COMPLETED, so the caller got a
/// short result and no error at all - strictly worse than the tagged half,
/// which at least failed loudly.
///
/// The assertion is on the PAYLOAD, deliberately. `is_ok()` passed against
/// the bug: `MyRightsConsumer` finalizes happily on a tagged OK having
/// received no data at all.
#[tokio::test]
async fn an_earlier_commands_untagged_response_reaches_its_consumer_mid_literal() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .execute_dynamic(std::time::Duration::from_secs(30))
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();
    assert!(
        listrights.ends_with("{5}\r\n"),
        "LISTRIGHTS did not produce a synchronizing literal: {listrights:?}"
    );

    // ORDERING IS THE WHOLE TEST. Command #1's untagged data lands inside the
    // continuation wait, but its TAGGED completion must NOT - it comes after
    // the `+`, once the wait is over.
    //
    // Sending the tagged response inside the wait as well would make the old
    // code fail loudly on the tagged path (it answered any tagged response with
    // a protocol error), which is the OTHER half of this defect and already has
    // its own test. Then this transcript would pass for the wrong reason and
    // the silent half - batch completes, caller gets a short result, no error
    // at all - would stay unpinned. A previous version of this test made
    // exactly that mistake.
    respond(&mut server, "* MYRIGHTS INBOX lrswipkxte\r\n+ ready\r\n").await;

    // The grant must actually release the literal body.
    let body = read_exact(&mut server, 5).await;
    assert_eq!(body, LITERAL_IDENTIFIER.as_bytes());
    let _trailer = read_line(&mut server).await;

    // Both completions now, in the step 4 response loop.
    respond(
        &mut server,
        &format!("{tag1} OK myrights done\r\n{tag2} OK listrights done\r\n"),
    )
    .await;

    let results = task.await.unwrap().unwrap();
    let rights = results[0]
        .as_ref()
        .expect("command #1 must not fail")
        .downcast_ref::<String>()
        .expect("MYRIGHTS output is a String");
    assert_eq!(rights, "lrswipkxte");
}

/// An earlier command's tagged `NO` arriving mid-literal is that command's
/// own result, and the batch survives it.
///
/// Before the router, any tagged response ended the wait and became the error
/// of the command being WRITTEN: command #1's `NO` was reported as a failure
/// of command #2, command #1's real result was destroyed, and the whole batch
/// aborted on the `?` at the send site before the response loop ever ran.
#[tokio::test]
async fn an_earlier_commands_tagged_no_lands_in_its_own_result_slot() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .execute_dynamic(std::time::Duration::from_secs(30))
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();

    respond(
        &mut server,
        &format!("{tag1} NO myrights refused\r\n+ ready\r\n"),
    )
    .await;

    let body = read_exact(&mut server, 5).await;
    assert_eq!(body, LITERAL_IDENTIFIER.as_bytes());
    let _trailer = read_line(&mut server).await;

    // Command #2 gets its own LISTRIGHTS data (the identifier as a literal,
    // since it is not ASCII) and its own OK, so its slot has a definite right
    // answer: the parsed rights, not merely "some result other than #1's".
    respond(
        &mut server,
        &format!(
            "* LISTRIGHTS INBOX {{5}}\r\n{LITERAL_IDENTIFIER} lr x\r\n\
             {tag2} OK listrights done\r\n"
        ),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    let err = results[0]
        .as_ref()
        .expect_err("command #1's NO belongs to command #1");
    assert!(
        format!("{err}").contains("myrights refused"),
        "wrong error routed to command #1: {err}"
    );
    let rights = results[1]
        .as_ref()
        .expect("command #1's refusal must not surface as command #2's")
        .downcast_ref::<crate::types::ListRightsResponse>()
        .expect("LISTRIGHTS output is a ListRightsResponse");
    assert_eq!(rights.required, "lr");
    assert_eq!(rights.optional, vec!["x".to_owned()]);
}

/// A `NO` answering the command whose literal is being negotiated is that
/// command's ordinary per-command result, the rest of its bytes are never
/// written, and the batch still returns every other command's result.
///
/// The server has refused the literal, so it is no longer parsing this
/// command (RFC 3501 Section 4.3); writing the body would have it read as a
/// new command line.
#[tokio::test]
async fn the_sending_commands_own_rejection_is_its_result_not_a_batch_error() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .execute_dynamic(std::time::Duration::from_secs(30))
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();

    // Refuse the literal, then answer command #1. Note the ORDER: command #1
    // is still outstanding when its sibling is rejected, so the batch has to
    // go back to the response loop rather than give up.
    respond(
        &mut server,
        &format!(
            "{tag2} BAD literal refused\r\n* MYRIGHTS INBOX lr\r\n{tag1} OK myrights done\r\n"
        ),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    let rights = results[0]
        .as_ref()
        .expect("command #1 still completes normally")
        .downcast_ref::<String>()
        .unwrap();
    assert_eq!(rights, "lr");
    let err = results[1]
        .as_ref()
        .expect_err("the rejected command's BAD is its own result");
    assert!(
        format!("{err}").contains("literal refused"),
        "wrong error routed to the rejected command: {err}"
    );

    // The abandoned remainder must NOT have reached the wire - the server has
    // ended this command's parsing state, so those bytes would be read as a new
    // command line (RFC 3501 Section 4.3). This is the rule the whole
    // `Rejected` path exists to honour and it needs a real assertion: simply
    // dropping the server end verifies nothing, because a client that DID
    // write the body would pass that just as happily.
    let mut tail = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut tail),
    )
    .await;
    // Anything at all is not the assertion: dropping the connection at the end
    // of the test sends a graceful LOGOUT, which is ordinary teardown traffic
    // and would make a bare "no more bytes" check fail for the wrong reason.
    // The assertion is that the ABANDONED BYTES specifically never went out.
    assert!(
        !tail
            .windows(LITERAL_IDENTIFIER.len())
            .any(|w| w == LITERAL_IDENTIFIER.as_bytes()),
        "the refused literal's body reached the wire and would be parsed as a \
         command: {:?}",
        String::from_utf8_lossy(&tail)
    );
}

/// A tagged `OK` for the command whose literal has not been granted is that
/// command's own non-fatal `ProtocolMissing`, exactly as in the single-command
/// wait, and the batch carries on.
///
/// The wire is still in sync: the send phase is sequential while a literal is
/// synchronizing, so nothing of a later command has been written when the
/// early `OK` arrives, and the abandoned remainder is never written. The proof
/// is the third command: its line must be the very next thing on the wire
/// (a written literal body would arrive first and be read as a command line).
/// The `OK` is NOT finalized through the consumer, which would report success
/// for a command that never ran.
#[tokio::test]
async fn a_tagged_ok_before_the_literal_continuation_is_that_commands_missing_result() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .get_acl(MailboxName::new("INBOX").unwrap())
            .execute_dynamic(std::time::Duration::from_secs(30))
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();

    respond(&mut server, &format!("{tag2} OK not really\r\n")).await;

    let getacl = read_line(&mut server).await;
    assert!(
        getacl.contains("GETACL"),
        "the abandoned literal body reached the wire ahead of the next command: {getacl:?}"
    );
    let tag3 = tag_of(&getacl).to_owned();
    respond(
        &mut server,
        &format!("* MYRIGHTS INBOX lr\r\n{tag1} OK myrights done\r\n{tag3} OK getacl done\r\n"),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    assert_eq!(results.len(), 3);
    let err = results[1]
        .as_ref()
        .expect_err("an OK before the continuation is not a success");
    assert!(
        matches!(
            err,
            crate::error::Error::Provider(crate::error::ProviderFailure::Missing(_))
        ),
        "wrong error: {err:?}"
    );
    assert!(!err.is_connection_fatal());
    let rights = results[0]
        .as_ref()
        .expect("command #1 still completes normally")
        .downcast_ref::<String>()
        .unwrap();
    assert_eq!(rights, "lr");
}

/// A tagged response for a command the server has not been sent yet is a
/// protocol error, not a parked completion.
///
/// This is the counterpart of the unknown-tag rejection the step 4 loop
/// already performs. It matters because the alternative is a HANG: the send
/// phase would keep waiting for a continuation the server has already decided
/// not to grant.
#[tokio::test]
async fn a_completion_for_an_unsent_command_is_a_protocol_error() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        let results = conn
            .pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .get_acl(MailboxName::new("INBOX").unwrap())
            .execute_dynamic(std::time::Duration::from_secs(30))
            .await;
        (conn, results)
    });

    let _myrights = read_line(&mut server).await;
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();
    // Command #3's tag is command #2's successor; it has not been written.
    let unsent_tag = next_tag(&tag2);

    respond(&mut server, &format!("{unsent_tag} OK impossible\r\n")).await;

    // The batch was on the wire, so the failure is reported per command: no
    // command was answered, and every slot carries the protocol error.
    let (conn, results) = task.await.unwrap();
    let results = results.expect("a failure after the first byte is reported per command");
    for result in &results {
        let err = result
            .as_ref()
            .expect_err("the server cannot complete a command it has not received");
        assert!(
            format!("{err}").contains("unsent pipelined command"),
            "wrong error: {err}"
        );
    }
    assert!(
        !conn.is_alive(),
        "a desynchronized batch retires the connection"
    );
}

/// Two pipelined searches never share a sub-batch, even of different kinds:
/// a tagless `* SEARCH` is solicited by every search form, so head routing
/// could hand one search's answer to the other. They run as sequential
/// sub-batches, and the second search's line is not written until the first
/// has completed.
///
/// Reverting `untagged_routing_key` to the plain kind puts both lines on the
/// wire at once, and the script's check that the first search completes
/// before the second line arrives fails. Time is paused, so the timeout fires
/// only once every task is idle - no wall-clock wait is involved.
#[tokio::test(start_paused = true)]
async fn two_pipelined_searches_run_as_sequential_sub_batches() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 ESEARCH")).await;
    // SEARCH is a Selected-state command (RFC 3501 Section 6.4.4).
    select_inbox(&conn, &mut server).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .uid_search("ALL".to_owned())
            .uid_search_return("ALL".to_owned(), vec!["ALL".into()])
            .execute_dynamic(std::time::Duration::from_secs(30))
            .await
    });

    let search = read_line(&mut server).await;
    let tag1 = tag_of(&search).to_owned();
    // Nothing more may be on the wire until the first search completes.
    let early = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        read_line(&mut server),
    )
    .await;
    assert!(
        early.is_err(),
        "the second search was written while the first was outstanding: {early:?}"
    );
    respond(
        &mut server,
        &format!("* SEARCH 3\r\n{tag1} OK search done\r\n"),
    )
    .await;

    let search_return = read_line(&mut server).await;
    let tag2 = tag_of(&search_return).to_owned();
    respond(
        &mut server,
        &format!("* ESEARCH (TAG \"{tag2}\") UID ALL 7\r\n{tag2} OK search return done\r\n"),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    let first = results[0]
        .as_ref()
        .expect("command #1 completes normally")
        .downcast_ref::<crate::connection::SearchResult>()
        .expect("SEARCH output is a SearchResult");
    assert_eq!(first.ids, vec![3]);
    let second = results[1]
        .as_ref()
        .expect("command #2's correlated ESEARCH must reach command #2")
        .downcast_ref::<crate::types::EsearchResponse>()
        .expect("SEARCH RETURN output is an EsearchResponse");
    assert_eq!(second.tag.as_deref(), Some(tag2.as_str()));
    assert_eq!(second.all(), vec![crate::types::UidRange::single(7)]);
}

/// A tag-correlated ESEARCH naming a pipelined search that has ALREADY
/// finalized is surplus data for a completed command and is dropped, not
/// published as an event.
///
/// Without the correlator arm the ESEARCH falls through to head routing: the
/// still-active NOOP has no use for it (`classify` makes ESEARCH `Impossible`
/// outside a search), so it is forwarded as an event the classifier says
/// cannot exist. The EXISTS beside it is the control: an ordinary `Either`
/// response still reaches the event queue through the same consumer, so an
/// empty queue is not what the assertion rests on.
#[tokio::test]
async fn a_surplus_esearch_for_a_finalized_search_is_dropped_not_published() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 ESEARCH")).await;
    // SEARCH is a Selected-state command (RFC 3501 Section 6.4.4).
    select_inbox(&conn, &mut server).await;

    let task = tokio::spawn(async move {
        let results = conn
            .pipeline()
            .uid_search_return("ALL".to_owned(), vec!["ALL".into()])
            .noop()
            .execute_dynamic(std::time::Duration::from_secs(30))
            .await;
        (conn, results)
    });

    let search_return = read_line(&mut server).await;
    let tag1 = tag_of(&search_return).to_owned();
    let noop = read_line(&mut server).await;
    let tag2 = tag_of(&noop).to_owned();

    // The search completes; a second ESEARCH naming it then arrives while
    // the NOOP is still the head.
    respond(
        &mut server,
        &format!(
            "* ESEARCH (TAG \"{tag1}\") UID ALL 7\r\n{tag1} OK search return done\r\n\
             * ESEARCH (TAG \"{tag1}\") UID ALL 9\r\n* 5 EXISTS\r\n\
             {tag2} OK noop done\r\n"
        ),
    )
    .await;

    let (conn, results) = task.await.unwrap();
    let results = results.expect("the batch must not abort");
    let search = results[0]
        .as_ref()
        .expect("the search completes normally")
        .downcast_ref::<crate::types::EsearchResponse>()
        .expect("SEARCH RETURN output is an EsearchResponse");
    assert_eq!(search.all(), vec![crate::types::UidRange::single(7)]);
    assert!(results[1].is_ok(), "the NOOP completes normally");

    let events = conn.drain_events().await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, crate::connection::typed_event::TypedEvent::Exists(5))),
        "the control EXISTS did not reach the event queue: {events:?}"
    );
    assert!(
        !events.iter().any(|e| matches!(
            e,
            crate::connection::typed_event::TypedEvent::Extension(u)
                if matches!(**u, crate::types::response::UntaggedResponse::Esearch(_))
        )),
        "a surplus ESEARCH for a finalized search was published: {events:?}"
    );
}

/// A SETMETADATA value that cannot be quoted (it opens with CRLF), `len` bytes
/// long, filled with `fill`. Two of these force two classic literals.
fn metadata_value(fill: u8, len: usize) -> Vec<u8> {
    let mut v = b"\r\n".to_vec();
    v.resize(len, fill);
    v
}

/// A pipelined batch whose middle command is a two-literal SETMETADATA refused
/// with a tagged `NO` at its SECOND marker (RFC 3502 permits the refusal of a
/// later literal). The refused command's slot holds the `NO`, its neighbours
/// complete, and none of its second body reaches the wire: the next command's
/// line is the next thing the server reads.
///
/// `len` is the size of each value; the capability set decides whether the
/// batch is sent through the segment path or the patched flat-buffer path.
async fn a_pipelined_command_refused_at_its_second_marker_is_its_own_result(
    caps: &str,
    len: usize,
) {
    let (conn, mut server) = driver_pair(&preauth_greeting(caps)).await;
    let first_value = metadata_value(b'a', len);
    let second_value = metadata_value(b'b', len);
    let marker = format!("{{{len}}}\r\n");

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .set_metadata(
                MailboxName::new("INBOX").unwrap(),
                vec![
                    ("/private/a".to_owned(), Some(first_value)),
                    ("/private/b".to_owned(), Some(second_value)),
                ],
            )
            .get_acl(MailboxName::new("INBOX").unwrap())
            .execute_dynamic(std::time::Duration::from_secs(30))
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let setmeta = read_line(&mut server).await;
    assert!(
        setmeta.contains("SETMETADATA"),
        "not SETMETADATA: {setmeta:?}"
    );
    assert!(setmeta.ends_with(&marker), "first marker: {setmeta:?}");
    let tag2 = tag_of(&setmeta).to_owned();
    respond(&mut server, "+ go\r\n").await;

    let body = read_exact(&mut server, len).await;
    assert_eq!(body, metadata_value(b'a', len));
    let second = read_line(&mut server).await;
    assert!(
        second.ends_with(&marker),
        "the second marker must end the write: {second:?}"
    );
    respond(
        &mut server,
        &format!("{tag2} NO [OVERQUOTA] second refused\r\n"),
    )
    .await;

    // The second value opens with CRLF, so had any of it been written this
    // read would return a bare CRLF instead of the GETACL line.
    let getacl = read_line(&mut server).await;
    assert!(
        getacl.contains("GETACL"),
        "bytes of the refused literal reached the wire ahead of the next command: {getacl:?}"
    );
    let tag3 = tag_of(&getacl).to_owned();
    respond(
        &mut server,
        &format!(
            "* MYRIGHTS INBOX lr\r\n{tag1} OK myrights done\r\n\
             * ACL INBOX me lrswipkxte\r\n{tag3} OK getacl done\r\n"
        ),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    assert_eq!(results.len(), 3);
    let err = results[1]
        .as_ref()
        .expect_err("the refused command's NO is its own result");
    assert!(
        matches!(
            err,
            crate::error::Error::ServerStatus(crate::error::ServerStatus::No { .. })
        ),
        "wrong error: {err:?}"
    );
    assert!(!err.is_connection_fatal());
    assert!(
        format!("{err}").contains("second refused"),
        "wrong error routed to the refused command: {err}"
    );
    let rights = results[0]
        .as_ref()
        .expect("the earlier command still completes")
        .downcast_ref::<String>()
        .unwrap();
    assert_eq!(rights, "lr");
    assert!(
        results[2].is_ok(),
        "the later command must complete after the refusal: {:?}",
        results[2].as_ref().err()
    );
}

/// No literal extension: `send_wire_command` with the batch's routing.
///
/// Writing the next segment before the wait fails the GETACL-line assertion;
/// answering the own-tag `NO` as a batch error fails the results assertions.
#[tokio::test]
async fn a_pipelined_command_refused_at_its_second_marker_is_its_own_result_without_a_literal_extension()
 {
    a_pipelined_command_refused_at_its_second_marker_is_its_own_result("IMAP4rev1 ACL METADATA", 6)
        .await;
}

/// Under `LITERAL-` with literals over 4096 octets: the batch has a segment
/// boundary although the connection has a literal extension, so it is sent
/// command by command with routing rather than as one coalesced write.
///
/// Writing past the second marker before the wait fails the GETACL-line
/// assertion; answering the own-tag `NO` as a batch error fails the results
/// assertions.
#[tokio::test]
async fn a_pipelined_command_refused_at_its_second_marker_is_its_own_result_over_the_literal_minus_limit()
 {
    a_pipelined_command_refused_at_its_second_marker_is_its_own_result(
        "IMAP4rev1 ACL METADATA LITERAL-",
        4097,
    )
    .await;
}

/// A failure in a later sub-batch does not replace the results of an earlier
/// one. Two `UID COPY`s share a routing key, so they run as two sub-batches;
/// the server executes the first and vanishes, and the second sub-batch's
/// first write reaches nothing. The first COPY's acknowledged result is kept,
/// the second is `Unsent` (no byte of it was written, so a retry is safe),
/// and the connection retires.
///
/// Reverting `run_pipeline` to the all-or-error shape fails the first
/// assertion: the batch comes back as one error and the first COPY's result
/// is gone, beside a mutation the server performed.
#[tokio::test(start_paused = true)]
async fn a_later_sub_batch_failure_keeps_the_earlier_sub_batch_results() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;
    select_inbox(&conn, &mut server).await;

    let script = tokio::spawn(async move {
        let first = read_line(&mut server).await;
        assert!(first.contains("UID COPY 1 "), "{first:?}");
        let tag = tag_of(&first).to_owned();
        respond(&mut server, &format!("{tag} OK copied\r\n")).await;
        drop(server);
    });

    let results = conn
        .pipeline()
        .uid_copy(
            crate::types::SequenceSet::new("1").unwrap(),
            MailboxName::new("Archive").unwrap(),
        )
        .uid_copy(
            crate::types::SequenceSet::new("2").unwrap(),
            MailboxName::new("Archive").unwrap(),
        )
        .execute_dynamic(std::time::Duration::from_secs(30))
        .await
        .expect("a failure after the first byte is reported per command");
    script.await.unwrap();
    assert!(
        results[0].is_ok(),
        "the first COPY was acknowledged; its result must survive: {:?}",
        results[0].as_ref().err()
    );
    let err = results[1]
        .as_ref()
        .expect_err("the second sub-batch cannot be written");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::Unsent),
        "no byte of the second COPY was written; got {err:?}"
    );
    assert!(
        !conn.is_alive(),
        "a batch that ended early retires the connection"
    );
}

/// A command on the wire whose answer never arrived is `InFlight`, while the
/// command the server did answer keeps its result. One coalesced write
/// carries both; the server answers the first and vanishes.
///
/// Reverting `run_pipeline` to the all-or-error shape fails the `expect`:
/// the read failure replaced every result.
#[tokio::test]
async fn an_unanswered_command_is_in_flight_and_the_answered_one_keeps_its_result() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 ACL")).await;

    let script = tokio::spawn(async move {
        let myrights = read_line(&mut server).await;
        let tag1 = tag_of(&myrights).to_owned();
        let _getacl = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("* MYRIGHTS INBOX lr\r\n{tag1} OK myrights done\r\n"),
        )
        .await;
        drop(server);
    });

    let results = conn
        .pipeline()
        .my_rights(MailboxName::new("INBOX").unwrap())
        .get_acl(MailboxName::new("INBOX").unwrap())
        .execute_dynamic(std::time::Duration::from_secs(30))
        .await
        .expect("a failure after the first byte is reported per command");
    script.await.unwrap();
    let rights = results[0]
        .as_ref()
        .expect("the answered command keeps its result")
        .downcast_ref::<String>()
        .unwrap();
    assert_eq!(rights, "lr");
    let err = results[1]
        .as_ref()
        .expect_err("the server vanished before answering GETACL");
    assert_eq!(
        err.attempt(),
        Some(bifrost_types::TransmissionState::InFlight),
        "GETACL was written and may have executed; got {err:?}"
    );
    assert!(
        !conn.is_alive(),
        "a batch that ended early retires the connection"
    );
}

/// An encoding refusal in a LATER sub-batch refuses the whole batch before any
/// byte: the two CAPABILITYs share a routing key and so run as two
/// sub-batches, and the UID MOVE in the second cannot be encoded without MOVE.
/// The first thing the server reads afterwards is the NOOP issued next.
///
/// Encoding each sub-batch only when it runs (the old shape) fails the
/// timeout: the first CAPABILITY is written and the batch waits for its
/// answer instead of refusing. Time is paused, so the timeout fires as soon as
/// every task is idle.
#[tokio::test(start_paused = true)]
async fn an_encoding_refusal_in_a_later_sub_batch_refuses_the_batch_before_any_byte() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    select_inbox(&conn, &mut server).await;

    let refused = conn
        .pipeline()
        .capability()
        .capability()
        .uid_move_messages(
            crate::types::SequenceSet::new("1").unwrap(),
            MailboxName::new("Archive").unwrap(),
        )
        .execute_dynamic(std::time::Duration::from_secs(30));
    let err = tokio::time::timeout(std::time::Duration::from_secs(5), refused)
        .await
        .expect("the batch went on the wire instead of being refused before its first byte")
        .expect_err("UID MOVE cannot be encoded without MOVE");
    let crate::connection::pipeline::PipelineError::Driver(err) = err else {
        panic!("expected a driver refusal, got {err:?}");
    };
    assert!(
        matches!(
            err,
            crate::error::Error::LocalRefusal(crate::error::LocalRefusal::MissingCapability(_))
        ),
        "got {err:?}"
    );

    let noop = conn.noop(std::time::Duration::from_secs(5));
    let script = async {
        let line = read_line(&mut server).await;
        assert!(
            line.contains("NOOP"),
            "part of the refused batch reached the wire: {line:?}"
        );
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
    };
    let (nooped, ()) = tokio::join!(noop, script);
    nooped.expect("the connection must carry the next command");
}

/// The tag the generator will produce after `tag`.
///
/// `TagGenerator` emits `{prefix:08x}{counter:08x}` from a per-connection
/// random prefix, so the successor is derived rather than guessed at.
fn next_tag(tag: &str) -> String {
    let (prefix, counter) = tag.split_at(8);
    let n = u32::from_str_radix(counter, 16).expect("the counter is 8 hex digits");
    format!("{prefix}{:08x}", n + 1)
}

/// Exercise both send branches with the real encoder, reader and router.
/// The control without overflow proves that an ordinary OK still registers;
/// the overflow case must stay cleared after the same tagged completion.
#[tokio::test]
async fn notify_overflow_cancels_pending_registration_on_both_send_branches() {
    use super::{ConsumerErased, SubBatch, encode_command, run_pipeline_batch};
    use crate::connection::{ImapStream, dispatch, state::ProtocolState, stream, wire::WireReader};
    use crate::types::response::{GreetingResponse, GreetingStatus};
    use crate::types::{
        Capability, Command, MailboxFilter, NotifyEvent, NotifyEventGroup, NotifySetParams,
    };
    use tokio::io::AsyncWriteExt;

    for synchronizing in [false, true] {
        for overflow in [false, true] {
            let mut state = ProtocolState::new();
            state
                .apply_greeting(&GreetingResponse {
                    status: GreetingStatus::PreAuth,
                    code: Some(crate::types::ResponseCode::Capability(vec![
                        Capability::Imap4Rev1,
                        Capability::Notify,
                        Capability::Acl,
                    ])),
                    text: String::new(),
                })
                .unwrap();
            let mut commands = vec![Command::NotifySet(NotifySetParams::new(
                vec![NotifyEventGroup::new(
                    MailboxFilter::Personal,
                    vec![NotifyEvent::MailboxName],
                )],
                false,
            ))];
            let mut tags = vec!["N1".to_owned()];
            let mut consumers: Vec<Box<dyn ConsumerErased>> =
                vec![Box::new(dispatch::NotifySetConsumer::default())];
            if synchronizing {
                commands.push(Command::ListRights {
                    mailbox: MailboxName::new("INBOX").unwrap(),
                    identifier: LITERAL_IDENTIFIER.to_owned(),
                });
                tags.push("L2".to_owned());
                consumers.push(Box::new(dispatch::TaggedOkConsumer::default()));
            }
            let opts = super::super::build_encode_options(&state);
            let wires = commands
                .iter()
                .zip(&tags)
                .map(|(cmd, tag)| encode_command(tag, cmd, &opts).unwrap())
                .collect();
            let (client, mut server) = tokio::io::duplex(8192);
            let mut transcript = String::new();
            if overflow {
                transcript.push_str("* OK [NOTIFICATIONOVERFLOW] lost\r\n");
            }
            // In the literal branch N1 completes inside L2's continuation
            // wait, so the same assertion covers the other router entry.
            transcript.push_str("N1 OK registered\r\n");
            if synchronizing {
                transcript.push_str("+ ready\r\nL2 OK done\r\n");
            }
            server.write_all(transcript.as_bytes()).await.unwrap();
            let mut reader = WireReader::new_metered(
                ImapStream::Memory(stream::Tracked::new(client)),
                None,
                None,
            );
            let (tx, _rx) = tokio::sync::mpsc::channel(16);
            let mut sink = super::event_sink::DriverEventSink::new(tx, None);
            let mut results = (0..commands.len()).map(|_| None).collect::<Vec<_>>();
            let outcome = run_pipeline_batch(
                &mut reader,
                &mut state,
                &mut sink,
                SubBatch {
                    commands,
                    consumers,
                    tags,
                    wires,
                },
                &mut results,
                0,
            )
            .await;
            assert!(outcome.is_ok());
            assert!(results.iter().all(|result| matches!(result, Some(Ok(_)))));
            assert_eq!(state.notify().list, !overflow);
        }
    }
}
