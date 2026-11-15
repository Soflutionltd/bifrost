use bifrost_types::{
    AccountError, AccountOperation, Checkpoint, ObjectId, PageBoundary, Protocol, SyncEvent,
};

use super::error::raw_message_not_found;
use bytes::Bytes;

use crate::types::{FetchAttr, MailboxName};

use super::{ImapAccount, batch, decode_object_id, guarded_receiver_stream, terminated_event};

pub(super) enum BlobError {
    Account(AccountError),
    Imap(crate::Error),
    ChannelDropped,
}

impl From<AccountError> for BlobError {
    fn from(value: AccountError) -> Self {
        Self::Account(value)
    }
}

impl From<crate::Error> for BlobError {
    fn from(value: crate::Error) -> Self {
        Self::Imap(value)
    }
}

/// Client-side ceiling on one raw-message read.
///
/// Every other body path carries a budget (hydration 256 MiB, draft fetch
/// 64 MiB) because the response size is the server's to choose: a corrupt
/// or adversarial peer can answer a single `BODY.PEEK[]` with an arbitrary
/// number of octets. The budget bounds delivered body octets in total and
/// declared literal octets before parsing. It is not a peak-memory bound:
/// one complete response is buffered before delivery, and syntax, parsed
/// objects and retained backing capacity add overhead. Crossing the body
/// budget is `crate::error::ResultLimit::Fetch`.
const RAW_FETCH_BUDGET: usize = 256 * 1024 * 1024;

/// Open a message's assembled RFC822 octets (`BODY.PEEK[]`).
///
/// Decodes an `ObjectId` (folder / uidvalidity / uid, no section) and fetches
/// the whole message with no section or partial, tagging transport errors
/// `OpenRawRfc822`. It shares no code with the `Account::open_blob` entry
/// point, which IMAP answers `Unsupported`.
///
/// Each complete FETCH response's body sections are forwarded under
/// `RAW_FETCH_BUDGET`. A section is not emitted while its literal is still
/// arriving: framing and parsing finish before the response is delivered.
pub(crate) fn open_raw_rfc822(
    account: ImapAccount,
    message: ObjectId,
) -> bifrost_types::AccountStream<SyncEvent<Bytes>> {
    let (tx, rx) = tokio::sync::mpsc::channel(super::STREAM_CAPACITY);
    let scope_id = message.0.clone();
    let task = tokio::spawn(async move {
        match run_raw(account.clone(), message.clone(), &tx).await {
            Ok(()) => {
                let _ = tx.send(SyncEvent::Done(None::<Checkpoint>)).await;
            }
            Err(BlobError::ChannelDropped) => {}
            Err(BlobError::Account(err)) => {
                let _ = tx.send(terminated_event(err)).await;
            }
            Err(BlobError::Imap(err)) => {
                let error = match super::decode_object_id(&message, AccountOperation::OpenRawRfc822)
                {
                    Ok(decoded) => account.folder_error(
                        err,
                        &decoded.folder.clone(),
                        super::error::ImapErrorContext::operation(AccountOperation::OpenRawRfc822)
                            .with_message_id(scope_id),
                    ),
                    Err(_) => super::account_error_with(
                        err,
                        super::error::ImapErrorContext::operation(AccountOperation::OpenRawRfc822)
                            .with_message_id(scope_id),
                    ),
                };
                let _ = tx.send(terminated_event(error)).await;
            }
        }
    });
    guarded_receiver_stream(rx, task)
}

async fn run_raw(
    account: ImapAccount,
    message: ObjectId,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<Bytes>>,
) -> Result<(), BlobError> {
    let decoded = decode_object_id(&message, AccountOperation::OpenRawRfc822)?;
    let outcome = run_fetch(
        &account,
        &decoded.folder.clone(),
        decoded.uidvalidity,
        decoded.uid.get(),
        AccountOperation::OpenRawRfc822,
        RAW_FETCH_BUDGET,
        UidPersistence::Session,
        tx,
    )
    .await?;
    match outcome {
        RawFetchOutcome::Delivered => Ok(()),
        RawFetchOutcome::NotSticky => Err(BlobError::Imap(crate::Error::Provider(
            crate::error::ProviderFailure::Protocol(
                "raw read unexpectedly refused session-local UIDs".into(),
            ),
        ))),
        // The server completed the FETCH without answering for the UID: the
        // message was expunged after the caller learned its id. Completing
        // the stream with zero batches presented that as an empty message.
        RawFetchOutcome::Absent => Err(BlobError::Account(raw_message_not_found(&decoded.id))),
        RawFetchOutcome::GenerationChanged { .. } => Err(BlobError::Account(
            super::error::raw_uidvalidity_changed(&decoded.folder),
        )),
    }
}

/// What a completed raw read established about the requested message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RawFetchOutcome {
    /// The server answered for the UID with its whole body, which was
    /// forwarded (possibly zero octets).
    Delivered,
    /// The FETCH completed OK and no response named the UID.
    Absent,
    /// The selected folder's UIDVALIDITY is not the one the id was minted
    /// under; nothing was fetched.
    GenerationChanged { observed: crate::types::UidValidity },
    /// The folder is `UIDNOTSTICKY` and the caller asked for that to be
    /// refused before the generation is judged; nothing was fetched.
    NotSticky,
}

/// Polling retains references between selections and therefore needs sticky
/// UIDs. A raw account read can use the current selection's UID epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub(super) enum UidPersistence {
    Session,
    Sticky,
}

fn missing_body(uid: u32) -> crate::Error {
    crate::Error::Provider(crate::error::ProviderFailure::Protocol(format!(
        "FETCH answered for UID {uid} without its whole BODY[] section"
    )))
}

/// Shared selection / UIDVALIDITY recheck / streaming-FETCH core for the
/// raw-message reads (`open_raw_rfc822` and the polling surface's
/// `fetch_raw`). `op` tags the errors so the caller's operation is
/// preserved, and `budget` is the client-side ceiling on the octets one read
/// may stream before `crate::error::ResultLimit::Fetch`.
///
/// The outcome is judged over the whole command, not per response: a server
/// may answer the UID in several FETCH responses (an unsolicited FLAGS update
/// beside the body), and responses for other UIDs are ignored. The whole
/// body (`BODY[]`, no origin) is forwarded once; a repeat with identical
/// octets is tolerated, a conflicting repeat or a NIL body is a protocol
/// violation, and a UID answered without any body is too.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_fetch(
    account: &ImapAccount,
    folder: &MailboxName,
    expected_uidvalidity: crate::types::UidValidity,
    uid: u32,
    op: AccountOperation,
    budget: usize,
    persistence: UidPersistence,
    tx: &tokio::sync::mpsc::Sender<SyncEvent<Bytes>>,
) -> Result<RawFetchOutcome, BlobError> {
    let attr = FetchAttr::BodySection {
        peek: true,
        section: None,
        partial: None,
    };
    if account.shutdown.is_cancelled() {
        return Err(BlobError::Account(AccountError::account_closed(
            op,
            Protocol::Imap,
        )));
    }
    let mut conn = account.checkout_for_folder(folder).await?;
    let mut previous_bytes_in = conn.connection().bytes_in();
    let folder_entry = account.folders.get(folder);
    let shared_owner = folder_entry
        .as_ref()
        .and_then(|entry| entry.shared_owner.clone());
    let selected = match account
        .select_folder(&mut conn, folder, super::SelectPurpose::Read)
        .await
    {
        Ok(selected) => selected,
        // A permission denial on a shared folder quarantines just that
        // scope (`ScopeRevoked`) instead of escalating to account-level
        // terminal; a personal folder, or any non-permission failure,
        // flows through the normal mapping.
        Err(err) if shared_owner.is_some() => {
            return Err(BlobError::Account(super::error::shared_folder_error(
                err,
                folder,
                shared_owner.as_ref(),
                super::error::ImapErrorContext::operation(op).with_folder_scope(folder),
            )));
        }
        Err(err) => return Err(err.into()),
    };
    if persistence == UidPersistence::Sticky && selected.mailbox.uid_not_sticky {
        return Ok(RawFetchOutcome::NotSticky);
    }
    let uidvalidity = super::selected_uidvalidity(&selected.mailbox)?;
    if uidvalidity != expected_uidvalidity {
        return Ok(RawFetchOutcome::GenerationChanged {
            observed: uidvalidity,
        });
    }
    // Build the operand from the same source the batch lanes use. This lane
    // streams bytes and has no per-id outcome to report an exclusion in, so
    // a UID the operand cannot carry is refused: sending nothing and
    // completing the stream normally would present "no such body" as a
    // successful empty read of a message the server was never asked about.
    let operand = super::targets::UidOperand::build([uid]);
    let Some(uid_set) = operand.uid_set() else {
        return Err(BlobError::Account(
            super::error::excluded_from_operand_error(op, folder),
        ));
    };
    let attrs = [attr];
    let connection = conn.connection();
    // Enforce the ceiling while framing literal declarations, before decoding
    // allocates the body; the streamed-byte check below is a second boundary.
    let (mut rx, fetch_fut) = connection.uid_fetch_stream_budgeted(
        uid_set.as_sequence_set(),
        &attrs,
        super::STREAM_CAPACITY,
        Some(budget),
        account.command_timeout(),
    )?;
    // Forward sections from each fully framed and parsed response. The bounded
    // receiver applies back-pressure between responses, not within a literal:
    // a slow consumer stalls this drain, which stalls the driver's next
    // `reserve_owned`, which stalls the socket read. Dropping `rx` on the way
    // out (every exit below) is what releases the driver to read through the
    // tagged completion instead of parking on a permit that will never be
    // taken.
    let requested_uid = uid;
    let drain = async {
        let mut matched = false;
        let mut body: Option<Bytes> = None;
        let mut outcome: Result<(), BlobError> = Ok(());
        'responses: while let Some(item) = rx.recv().await {
            let fetch = match item {
                Ok(fetch) => fetch,
                Err(err) => {
                    outcome = Err(err.into());
                    break;
                }
            };
            if fetch.uid != Some(requested_uid) {
                continue;
            }
            matched = true;
            let seq = fetch.seq;
            for section in fetch.body_sections {
                if !section.section.is_empty() || section.origin.is_some() {
                    continue;
                }
                let Some(data) = section.data else {
                    outcome = Err(BlobError::Imap(missing_body(requested_uid)));
                    break 'responses;
                };
                if let Some(first) = &body {
                    if *first != data {
                        outcome = Err(BlobError::Imap(crate::Error::Provider(
                            crate::error::ProviderFailure::Protocol(format!(
                                "FETCH answered UID {requested_uid} with two different bodies"
                            )),
                        )));
                        break 'responses;
                    }
                    continue;
                }
                if data.len() > budget {
                    outcome = Err(BlobError::Imap(crate::Error::ResultLimit(
                        crate::error::ResultLimit::Fetch {
                            estimated: data.len(),
                            limit: budget,
                            seq: crate::types::Seq::new(seq),
                            uid: crate::types::Uid::new(requested_uid),
                        },
                    )));
                    break 'responses;
                }
                body = Some(data.clone());
                if tx
                    .send(batch(
                        vec![data],
                        PageBoundary::Page,
                        None::<Checkpoint>,
                        super::measured_bytes_in(connection, &mut previous_bytes_in),
                    ))
                    .await
                    .is_err()
                {
                    outcome = Err(BlobError::ChannelDropped);
                    break 'responses;
                }
            }
        }
        drop(rx);
        outcome.map(|()| match (matched, body.is_some()) {
            (false, _) => Ok(RawFetchOutcome::Absent),
            (true, true) => Ok(RawFetchOutcome::Delivered),
            (true, false) => Err(BlobError::Imap(missing_body(requested_uid))),
        })
    };
    // The command future is authoritative for the wire outcome, exactly as
    // in inventory / QRESYNC / CONDSTORE: the driver drops the streaming
    // consumer before answering its oneshot, so the receiver closing says
    // nothing about whether the FETCH succeeded. A local drain failure
    // (limit crossed, dropped consumer) still wins, since it describes this
    // caller rather than the server.
    let (fetch_result, drain_result) = tokio::join!(fetch_fut, drain);
    let judged = drain_result?;
    match fetch_result {
        Err(crate::Error::ResultLimit(crate::error::ResultLimit::FetchLiteral {
            estimated,
            limit,
        })) => {
            // The response was stopped at its literal marker, before parsing
            // could recover the server's sequence number or UID. This lane
            // requested exactly one UID, so retain that identity while
            // reporting the caller's byte limit through the established
            // FetchLimit variant.
            return Err(BlobError::Imap(crate::Error::ResultLimit(
                crate::error::ResultLimit::Fetch {
                    estimated,
                    limit,
                    seq: None,
                    uid: crate::types::Uid::new(uid),
                },
            )));
        }
        other => other?,
    }
    // Judged only after the tagged completion: bytes forwarded before a
    // failed command never make the read a success.
    judged
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    use crate::account::test_support::scripted_account_parts;
    use crate::connection::test_support::{
        driver_pair, preauth_greeting, read_line, respond, tag_of,
    };

    /// Withhold the FETCH completion so a detached reader cannot finish by itself.
    #[tokio::test]
    async fn dropping_raw_stream_releases_account_during_fetch() {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        let account = ImapAccount::new(scripted_account_parts(conn, 1));
        let pool = std::sync::Arc::downgrade(&account.pool);
        let folder = MailboxName::new("INBOX").unwrap();
        let stream = open_raw_rfc822(
            account,
            super::super::encode_object_id(
                &folder,
                crate::types::UidValidity::new(5).expect("nonzero epoch"),
                crate::types::Uid::new(7).expect("valid protocol test value"),
            ),
        );
        let select = read_line(&mut server).await;
        assert!(select.contains("EXAMINE"), "{select}");
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Seen)\r\n* 1 EXISTS\r\n* 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] valid\r\n{} OK [READ-ONLY] selected\r\n",
                tag_of(&select),
            ),
        )
        .await;
        let fetch = read_line(&mut server).await;
        assert!(fetch.contains("UID FETCH"), "{fetch}");
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pool.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the stream must destroy its pending read task");
    }
}
