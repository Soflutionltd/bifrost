//! OneDrive large-attachment hosting: resumable upload + sharing link.
//!
//! COPY-AND-ADAPTed from ratatoskr's `graph/onedrive.rs`. The pure pieces
//! (serde structs, `encode_onedrive_path`, the chunk-range math) copy cleanly;
//! the transport bodies are rewritten onto bifrost-net. The ratatoskr source
//! used `reqwest::Client` directly for the chunk PUT and threaded a
//! `db: &ReadDbState` ratatoskr handle into the session/link `client.post`
//! calls; the rewrite drops both. The session/link POSTs route through
//! `GraphClient::post` (AccountNet supplies the Bearer); the pre-authenticated
//! chunk PUTs and the session cancel go through `GraphClient::execute_aux`
//! with `AuxTarget::Anonymous`, which carries no bearer and follows no
//! redirect.
//!
//! De-brand: ratatoskr uploaded into a `"Ratatoskr Attachments"` folder. This
//! uses a neutral, consumer-agnostic `"Attachments"` folder. A future
//! consumer-configurable folder name can replace `ATTACHMENTS_FOLDER` without
//! touching the wire path.
//!
//! Mailbox routing: hosting always uploads into the *primary* user's OneDrive
//! (`account.client`, the `/me/drive` prefix), even when the draft being
//! composed belongs to a shared/delegate mailbox (`send_as`). This is
//! deliberate - a shared Exchange mailbox has no OneDrive of its own, and the
//! attachment is shared via a link rather than embedded, so the authenticated
//! user's drive is the only sensible host. The recipient sees a link, not a
//! drive location, so the hosting mailbox is invisible downstream.

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, CloudUploadMeta, HostedAttachment, ShareScope,
    TransmissionState,
};
use bifrost_types::{
    Application, CompensationOutcome, ErrorScope, PartialCommitBuilder, PrimaryLeg, StepEffect,
    StepReplay, StepTargets, TargetRef, TargetRole,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::graph_error::{GraphErrorContext, into_account_error, invalid_account_error};
use crate::account::GraphAccount;
use crate::client::{AuxTarget, GraphClient, Resource};
use crate::error::{GraphError, GraphResponseError};
use crate::origin::{Refusal, UploadUrl};

/// Minimum alignment for upload chunks (320 KiB per Graph API spec).
const CHUNK_ALIGNMENT: usize = 320 * 1024;

/// Default chunk size: 5 MiB. Must be a multiple of `CHUNK_ALIGNMENT`.
const DEFAULT_CHUNK_SIZE: usize = 5 * 1024 * 1024;

/// Neutral, de-branded folder the hosted attachments land in. A later
/// consumer-configurable folder name can replace this constant without
/// touching the wire path below.
const ATTACHMENTS_FOLDER: &str = "Attachments";

/// An active OneDrive resumable upload session.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadSession {
    upload_url: String,
    #[serde(default)]
    #[allow(dead_code)]
    expiration_date_time: String,
}

/// Request body for creating an upload session via `createUploadSession`.
#[derive(Debug, Serialize)]
struct CreateUploadSessionRequest<'a> {
    item: DriveItemUploadable<'a>,
    #[serde(rename = "@microsoft.graph.conflictBehavior")]
    conflict_behavior: &'a str,
}

/// Properties for the file being uploaded.
#[derive(Debug, Serialize)]
struct DriveItemUploadable<'a> {
    name: &'a str,
}

/// The completed drive item returned after the final upload chunk.
#[derive(Debug, Deserialize)]
struct DriveItemResponse {
    id: String,
}

/// The 202 Accepted body OneDrive returns mid-upload. `nextExpectedRanges`
/// is a list of `"start-end"` (or `"start-"`) byte ranges the server still
/// wants; the first range's start is the authoritative resume offset. The
/// server may accept fewer bytes than were sent (it is allowed to), so the
/// client must resume from the server's reported offset rather than blindly
/// advancing to the end of the chunk it just PUT.
///
/// The field is REQUIRED: a 202 without it names no offset to resume from,
/// and guessing one (the end of the chunk just sent) skips every byte the
/// server did not accept, corrupting the uploaded file without an error.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadProgress {
    next_expected_ranges: Vec<String>,
}

/// The start of a `"start-end"` or `"start-"` byte range, or `None` when the
/// text is not one. Both bounds must be plain decimal digits (no sign, no
/// whitespace inside), and a closed range must not end before it starts.
fn range_start(range: &str) -> Option<usize> {
    fn decimal(text: &str) -> Option<usize> {
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        text.parse().ok()
    }
    let (start, end) = range.trim().split_once('-')?;
    let start = decimal(start)?;
    if !end.is_empty() && decimal(end)? < start {
        return None;
    }
    Some(start)
}

/// The resume offset a mid-upload 202 names, admitted against the chunk it
/// answers (`offset..end` of `total` bytes).
///
/// A 202 carries no data of its own beyond this offset, so every way it can
/// be wrong is refused rather than guessed around:
///
/// - a body that does not parse, a missing `nextExpectedRanges`, or a first
///   range that is not a byte range is the provider's malformed response
///   (`GraphError::Json`, `Protocol(ParseFailed)`);
/// - an empty `nextExpectedRanges` (a 202 that wants nothing more), an offset
///   that does not advance (which would spin the loop), one past the end of
///   the bytes just sent (which would skip bytes the server never received),
///   or one that reaches the total (every byte acknowledged, yet no drive
///   item, the 200/201 the protocol requires) is well-formed but impossible
///   (`GraphError::ContractViolation`, `Protocol(ContractViolation)`).
///
/// An offset short of `end` is legitimate on any chunk, the final one
/// included: the server accepted part of it and wants the rest again.
fn admitted_resume_offset(
    body: &Bytes,
    offset: usize,
    end: usize,
    total: usize,
) -> Result<usize, GraphError> {
    let progress: UploadProgress =
        serde_json::from_slice(body.as_ref()).map_err(|e| GraphError::Json {
            message: format!("OneDrive 202 body: {e}"),
            body: (!body.is_empty()).then(|| body.clone()),
        })?;
    let Some(first) = progress.next_expected_ranges.first() else {
        return Err(contract_violation(format!(
            "OneDrive 202 for bytes {offset}-{} named no nextExpectedRanges",
            end - 1
        )));
    };
    let next = range_start(first).ok_or_else(|| GraphError::Json {
        message: "OneDrive 202 nextExpectedRanges entry is not a byte range".to_string(),
        body: Some(body.clone()),
    })?;
    if next <= offset {
        return Err(contract_violation(format!(
            "OneDrive 202 resume offset {next} did not advance past {offset}"
        )));
    }
    if next > end {
        return Err(contract_violation(format!(
            "OneDrive 202 resume offset {next} is past the end {end} of the bytes sent"
        )));
    }
    if next >= total {
        return Err(contract_violation(format!(
            "OneDrive 202 acknowledged all {total} bytes without returning the drive item"
        )));
    }
    Ok(next)
}

/// Admit the `uploadUrl` a `createUploadSession` answer named, at receipt and
/// before any byte is sent to it. The rule itself lives with every other
/// admission rule in `crate::origin` (`Base::admit_upload`); this is where a
/// refusal is classified.
///
/// A refusal is the provider's contract violation: Graph answered, and the
/// answer names a destination this client will not send to. The reason never
/// quotes the URL, since the URL is itself the session credential.
fn admit_upload_url(client: &GraphClient, raw: &str) -> Result<UploadUrl, GraphError> {
    client.admit_upload(raw).map_err(|refusal| {
        let reason = match refusal {
            Refusal::Target(reason) | Refusal::InvalidBase(reason) => reason,
        };
        contract_violation(format!(
            "OneDrive createUploadSession named an upload URL this client will not send \
             the attachment to: {reason}"
        ))
    })
}

/// Budget for the best-effort session cancel after a failed upload. Its own,
/// not the remainder of `UPLOAD_TOTAL_TIMEOUT`: the upload may have failed
/// precisely because that budget ran out, and a cancel handed a spent deadline
/// would never be sent.
const SESSION_CANCEL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Best-effort cancel of an upload session that will not be completed:
/// `DELETE` on the admitted session URL, anonymous like the chunk PUTs.
/// OneDrive otherwise keeps the session (and the bytes already accepted into
/// it) until it expires on its own.
///
/// Never replaces the upload's own error, whatever the cancel does. Bounded
/// by [`SESSION_CANCEL_TIMEOUT`] and abandoned the moment the account shuts
/// down, so a failed upload cannot hold up `close()`; it is not attempted at
/// all once the shutdown has already fired. Nothing is spawned: a caller that
/// drops the hosting future drops the cancel with it, and the session
/// expires on OneDrive's clock, which is the most a detached task outliving
/// the account could have promised anyway.
///
/// A cancel racing a final chunk PUT that was cut short (an `InFlight`
/// failure) is harmless either way: a session that already produced its
/// drive item is gone, and deleting a session never deletes a created file;
/// one that had not yet completed is stopped, which leaves the reconciliation
/// that `InFlight` asks for a clean answer.
///
/// `None` means no cancel was attempted (shutdown had already fired), so the
/// record carries no compensation. A shutdown that interrupts the cancel is an
/// `InFlight` failure, which becomes an `Unconfirmed` compensation. An
/// unfollowed 3xx is a contract violation with `Acknowledged` evidence, so a
/// `Failed` compensation, never `Succeeded`.
///
/// A failure comes back through [`without_session_url`]: it is kept as the
/// record's compensation outcome, which reaches support exports, and the
/// transport's error text names the session URL, which is the credential.
async fn cancel_upload_session(
    client: &GraphClient,
    upload_url: &UploadUrl,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Option<Result<(), GraphError>> {
    if shutdown.is_cancelled() {
        return None;
    }
    let cancel = client.execute_aux(
        Resource::Unmetered,
        "DELETE",
        AuxTarget::Anonymous(upload_url),
        &[],
        Bytes::new(),
        Some(SESSION_CANCEL_TIMEOUT),
        bifrost_net::ReplayPolicy::IdempotentUnlessConditional,
    );
    let outcome = tokio::select! {
        biased;
        outcome = cancel => outcome,
        () = shutdown.cancelled() => return Some(Err(shutdown_during_upload(TransmissionState::InFlight))),
    };
    Some(match outcome {
        // The DELETE is sent with redirects disabled (`AuxTarget::Anonymous`),
        // so a 3xx is the answer itself: a real response that did not accept
        // the cancel, not a cancelled session. Classified like the chunk PUT's
        // redirect, naming the status only, since a `Location` kept in the
        // compensation would reach support exports and may carry the session
        // credential. bifrost-net resolves 4xx/5xx into an `Err` before a
        // response surfaces, so a 2xx is the only acceptance.
        Ok(response) if !response.status.is_success() => Err(contract_violation(format!(
            "OneDrive answered the upload session cancel with {}, which does not \
             cancel the session",
            response.status
        ))),
        Ok(_) => Ok(()),
        Err(error) => Err(without_session_url(error)),
    })
}

/// `error` with the transport text that can quote the request URL withheld
/// (`bifrost_net::Error::redact_request_text`), classifying as before.
fn without_session_url(error: GraphError) -> GraphError {
    match error {
        GraphError::Net(net) => GraphError::Net(net.redact_request_text()),
        GraphError::MailboxThrottled { budget, source } => GraphError::MailboxThrottled {
            budget,
            source: Box::new(without_session_url(*source)),
        },
        other => other,
    }
}

/// Overall wall-clock budget for the whole chunked upload. A server that
/// keeps returning 202 (or hangs) without ever completing must not stall
/// the send pipeline indefinitely.
const UPLOAD_TOTAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// Response from creating a sharing link.
#[derive(Debug, Deserialize)]
struct CreateLinkResponse {
    link: SharingLink,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SharingLink {
    web_url: String,
}

/// Dispatch entry point wired from the `Account` impl.
pub(crate) fn host_attachment(
    account: GraphAccount,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> AccountFuture<Result<HostedAttachment, AccountError>> {
    Box::pin(async move {
        if meta.size != bytes.len() as u64 {
            return Err(invalid_account_error(
                AccountOperation::HostAttachment,
                format!(
                    "declared size {} does not match payload length {}",
                    meta.size,
                    bytes.len()
                ),
            ));
        }
        if bytes.is_empty() {
            return Err(invalid_account_error(
                AccountOperation::HostAttachment,
                "cannot host an empty attachment",
            ));
        }
        upload_and_link(&account, bytes, meta)
            .await
            .map_err(upload_failure_error)
    })
}

fn ctx() -> GraphErrorContext {
    GraphErrorContext::graph(AccountOperation::HostAttachment)
}

/// What an upload had done when it failed, for the partial-commit record.
/// Every effect is `Repeatable` relative to hosting an attachment: a replay
/// leaves at most another expiring session or unlinked file.
#[derive(Default)]
struct UploadFacts {
    session: bool,
    chunks: usize,
    final_chunk: bool,
    file: Option<String>,
    cancel: Option<Result<(), GraphError>>,
}
struct UploadFailure {
    error: GraphError,
    facts: UploadFacts,
    effect: Option<StepEffect>,
}
fn session_targets() -> StepTargets {
    StepTargets::at_least(vec![TargetRef::new(
        TargetRole::Subject,
        ErrorScope::UploadSession,
    )])
}
fn file_targets(id: Option<&str>) -> StepTargets {
    id.map_or_else(StepTargets::unknown, |id| {
        StepTargets::complete(vec![TargetRef::new(
            TargetRole::Subject,
            ErrorScope::CloudFile { id: id.to_owned() },
        )])
    })
}
/// The plain leaf when no session could exist; otherwise the leaf with the
/// session, accepted chunks, cancel compensation and any created file. An
/// admission refusal is an effectless primary with no compensation (no
/// cancel is sent to an untrusted URL); a final-chunk failure is a
/// `Created(CloudFile)` primary the session cancel does not reverse.
fn upload_failure_error(failure: UploadFailure) -> AccountError {
    let UploadFailure {
        error,
        facts,
        effect,
    } = failure;
    let error = into_account_error(error, ctx());
    let application = Application::from_leaf(&error);
    if !facts.session && application != Application::Uncertain {
        return error;
    }
    let targets = if !facts.session || effect == Some(StepEffect::Updated) {
        session_targets()
    } else {
        file_targets(facts.file.as_deref())
    };
    let primary = effect.map_or_else(
        || PrimaryLeg::effectless(targets.clone()),
        |effect| PrimaryLeg::new(effect, targets.clone(), StepReplay::Repeatable, application),
    );
    let mut record = PartialCommitBuilder::new(primary);
    if facts.session {
        let session = record.committed(
            StepEffect::Created,
            session_targets(),
            StepReplay::Repeatable,
        );
        for _ in 0..facts.chunks {
            record.committed(
                StepEffect::Updated,
                session_targets(),
                StepReplay::Repeatable,
            );
        }
        if let Some(cancel) = facts.cancel {
            record.compensation(
                StepEffect::Deleted,
                session_targets(),
                vec![session],
                match cancel {
                    Ok(()) => CompensationOutcome::Succeeded,
                    Err(e) => CompensationOutcome::from_error(into_account_error(e, ctx())),
                },
            );
        }
    }
    if let Some(file) = facts.file {
        record.committed(
            StepEffect::Created,
            file_targets(Some(&file)),
            StepReplay::Repeatable,
        );
    }
    error
        .into_builder()
        .partial_commit(record)
        .try_build()
        .expect("valid OneDrive progress")
}
async fn upload_and_link(
    account: &GraphAccount,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> Result<HostedAttachment, UploadFailure> {
    let client = &account.client;
    let mut facts = UploadFacts::default();
    let session = create_upload_session(client, &meta.file_name)
        .await
        .map_err(|error| UploadFailure {
            error,
            facts: UploadFacts::default(),
            effect: Some(StepEffect::Created),
        })?;
    facts.session = true;
    let upload_url = match admit_upload_url(client, &session.upload_url) {
        Ok(url) => url,
        Err(error) => {
            return Err(UploadFailure {
                error,
                facts,
                effect: None,
            });
        }
    };
    let uploaded = upload_file_chunked(
        client,
        &upload_url,
        bytes,
        DEFAULT_CHUNK_SIZE,
        &account.shutdown,
        &mut facts,
    )
    .await;
    let item_id = match uploaded {
        Ok(id) => id,
        Err(error) => {
            let effect = Some(if facts.final_chunk {
                StepEffect::Created
            } else {
                StepEffect::Updated
            });
            return Err(UploadFailure {
                error,
                facts,
                effect,
            });
        }
    };
    facts.file = Some(item_id.clone());
    let share_url = create_sharing_link(client, &item_id, meta.scope)
        .await
        .map_err(|error| UploadFailure {
            error,
            facts,
            effect: Some(StepEffect::Created),
        })?;
    Ok(HostedAttachment::new(share_url, item_id))
}

/// Create an upload session under the de-branded `Attachments` folder. The
/// folder is created implicitly by OneDrive on first upload; `rename` conflict
/// behavior means uploads never overwrite an existing file.
async fn create_upload_session(
    client: &GraphClient,
    file_name: &str,
) -> Result<UploadSession, GraphError> {
    let encoded = encode_onedrive_path(file_name);
    let prefix = client.api_path_prefix();
    let path = format!("{prefix}/drive/root:/{ATTACHMENTS_FOLDER}/{encoded}:/createUploadSession");

    let body = CreateUploadSessionRequest {
        item: DriveItemUploadable { name: file_name },
        conflict_behavior: "rename",
    };

    client
        .post(
            Resource::Unmetered,
            &path,
            &body,
            bifrost_net::ReplayPolicy::Never,
        )
        .await
}

/// Upload the payload in `chunk_size`-aligned chunks against the
/// pre-authenticated session URL.
///
/// 200/201 -> the final chunk was accepted; parse the drive item id. 202 ->
/// the server reports the next byte range it expects via `nextExpectedRanges`;
/// resume from that offset (the server may accept fewer bytes than were sent,
/// so blindly advancing `offset = end` can skip unaccepted bytes and corrupt
/// the upload), and refuse a 202 that names no valid offset inside the bytes
/// just sent rather than guess one (`admitted_resume_offset`). Any other
/// status -> classified error. OneDrive's 202 resume signal never enters
/// bifrost-net's redirect path, and the PUT is sent with redirects disabled,
/// so any 3xx is refused as a contract violation rather than followed. The
/// whole upload is bounded by
/// `UPLOAD_TOTAL_TIMEOUT` and aborts if the account shutdown token fires.
///
/// The budget is enforced INSIDE the transport, not by a timer around the
/// loop: each chunk PUT carries what is left of it as its bifrost-net total
/// deadline, so an expiry is classified by the stage it hit - `Unsent` while
/// waiting to dispatch, `InFlight` awaiting headers, a partial response after
/// them. A timer around the whole loop threw that stage away, and used to
/// report the expiry as a provider parse failure the provider never caused.
///
/// A failed upload cancels its session (`cancel_upload_session`) before the
/// failure is returned, unchanged. Progress for the partial-commit record
/// (accepted chunks, whether the final chunk was in flight, the cancel
/// outcome) is written into `facts`.
async fn upload_file_chunked(
    client: &GraphClient,
    upload_url: &UploadUrl,
    data: Bytes,
    chunk_size: usize,
    shutdown: &tokio_util::sync::CancellationToken,
    facts: &mut UploadFacts,
) -> Result<String, GraphError> {
    // Defensive guard over the caller-supplied chunk size; the sole caller
    // passes the `DEFAULT_CHUNK_SIZE` constant, so this is a producer-bug
    // check, not a runtime path - a `debug_assert` keeps it out of release.
    debug_assert!(
        chunk_size != 0 && chunk_size.is_multiple_of(CHUNK_ALIGNMENT),
        "chunk_size must be a positive multiple of {CHUNK_ALIGNMENT}, got {chunk_size}"
    );

    let deadline = tokio::time::Instant::now() + UPLOAD_TOTAL_TIMEOUT;
    upload_or_cancel(
        client, upload_url, data, chunk_size, shutdown, deadline, facts,
    )
    .await
}

/// [`upload_chunks`], then the session cancel if it failed, its outcome kept
/// in `facts.cancel`. The failure is returned unchanged.
async fn upload_or_cancel(
    client: &GraphClient,
    upload_url: &UploadUrl,
    data: Bytes,
    chunk_size: usize,
    shutdown: &tokio_util::sync::CancellationToken,
    deadline: tokio::time::Instant,
    facts: &mut UploadFacts,
) -> Result<String, GraphError> {
    let uploaded = upload_chunks(
        client, upload_url, data, chunk_size, shutdown, deadline, facts,
    )
    .await;
    if uploaded.is_err() {
        facts.cancel = cancel_upload_session(client, upload_url, shutdown).await;
    }
    uploaded
}

/// The chunk loop. `facts.chunks` counts non-final chunks the server
/// acknowledged with 202; `facts.final_chunk` is true while the chunk that
/// would create the drive item is in flight.
async fn upload_chunks(
    client: &GraphClient,
    upload_url: &UploadUrl,
    data: Bytes,
    chunk_size: usize,
    shutdown: &tokio_util::sync::CancellationToken,
    deadline: tokio::time::Instant,
    facts: &mut UploadFacts,
) -> Result<String, GraphError> {
    // Empty payloads are rejected up front in `host_attachment`, so by here the
    // data is non-empty and the loop always runs at least once.
    let total = data.len();

    let mut offset = 0usize;
    while offset < total {
        // Between chunks no PUT is outstanding and every earlier one got a
        // complete 202, and the file exists only once the final chunk is
        // accepted: nothing a replay could duplicate has happened, so both
        // refusals here are `Unsent`.
        if shutdown.is_cancelled() {
            return Err(shutdown_during_upload(TransmissionState::Unsent));
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(GraphError::Net(bifrost_net::Error::Timeout {
                transmission_state: TransmissionState::Unsent,
            }));
        }
        let end = (offset + chunk_size).min(total);
        facts.final_chunk = end == total;
        let chunk = data.slice(offset..end);
        let content_range = format!("bytes {offset}-{}/{total}", end - 1);

        // The session URL is pre-authenticated: sending the Graph bearer to
        // it would leak the token to whatever host OneDrive minted.
        let headers = [("Content-Range", content_range.as_str())];
        let put = client.execute_aux(
            Resource::Unmetered,
            "PUT",
            AuxTarget::Anonymous(upload_url),
            &headers,
            chunk,
            Some(remaining),
            bifrost_net::ReplayPolicy::Idempotent,
        );
        // Shutdown is watched HERE, at the one boundary where the stage is
        // known, rather than by racing the whole upload: a race around the
        // loop let the same between-chunks shutdown come out either way. Cut
        // short mid-PUT, the stage is lost and the chunk may be the final one
        // that creates the file, so it is conservatively `InFlight` (a blind
        // replay of this non-idempotent upload could make a second file). A
        // PUT result that is already ready wins over the shutdown, since it
        // is the known outcome.
        //
        // Accepted limit (owner ruling, 2026-10-03): this is `InFlight` even
        // when the cancellation lands while bifrost-net is still waiting for
        // rate-limit admission and nothing was dispatched, because dropping
        // the future loses the stage it had reached. The cost is an unneeded
        // read-back, never a blind replay. Exact classification would need
        // bifrost-net to take a cancellation signal into the request and
        // report the stage it stopped at; re-raise only with that transport
        // feature in hand.
        let response = tokio::select! {
            biased;
            response = put => response?,
            () = shutdown.cancelled() => {
                return Err(shutdown_during_upload(TransmissionState::InFlight));
            }
        };

        let status = response.status;
        match status.as_u16() {
            200 | 201 => {
                let item: DriveItemResponse = serde_json::from_slice(response.body.as_ref())
                    .map_err(|e| GraphError::Json {
                        message: format!("OneDrive upload drive item JSON body: {e}"),
                        body: if response.body.is_empty() {
                            None
                        } else {
                            Some(response.body)
                        },
                    })?;
                return Ok(item.id);
            }
            // 202 Accepted: resume from the server's authoritative offset.
            // The server is allowed to accept fewer bytes than the chunk we
            // PUT, so there is no safe default: a 202 without a valid offset
            // inside `offset..=end` is refused, never resumed at `end`.
            202 => {
                offset = admitted_resume_offset(&response.body, offset, end, total)?;
                facts.chunks += 1;
            }
            // A redirect. The chunk PUT is sent with redirects disabled (the
            // session URL was admitted under a rule no hop is checked
            // against), so every 3xx arrives here. Graph's resumable upload
            // never redirects a chunk PUT, so this is the provider breaking
            // the upload contract, not a transient server fault to retry.
            // The message names the status only: a `Location` may carry the
            // session credential.
            300..=399 => {
                return Err(contract_violation(format!(
                    "OneDrive answered an upload chunk PUT with redirect {status}, \
                     which the resumable upload protocol does not use"
                )));
            }
            // Everything else. This arm is DELIBERATELY not pinned by a test,
            // and cannot be reached by the failure statuses it reads as though
            // it handled them: bifrost-net resolves 4xx/5xx into an `Err`
            // before a response ever surfaces here, and a 3xx takes the arm
            // above, so what could arrive is a 1xx or a nonstandard 2xx. The
            // arm is kept, and kept classifying with the real response
            // headers and body rather than a synthesized error, because "the
            // transport handed us a status we did not expect" must not be
            // silently treated as success. If bifrost-net ever stops
            // pre-resolving error statuses, this becomes the live error path
            // and wants a scripted test.
            _ => {
                let err =
                    GraphResponseError::from_response(status, response.headers, response.body);
                return Err(GraphError::Response(err));
            }
        }
    }

    // Every 202 leaves `offset` short of `total` (`admitted_resume_offset`
    // refuses one that acknowledges the last byte), so the loop ends only by
    // returning, unless it never ran: an empty payload, which
    // `host_attachment` refuses before any request.
    Err(GraphError::Internal {
        message: "OneDrive chunked upload was handed an empty payload".to_string(),
    })
}

/// The account was shut down (close / reopen) while an upload was under way.
/// A network-class abort stamped with where it was observed: `Unsent`
/// between chunks (retryable), `InFlight` when it cut a PUT short (which a
/// non-idempotent upload reconciles rather than replays).
fn shutdown_during_upload(transmission_state: TransmissionState) -> GraphError {
    GraphError::Net(bifrost_net::Error::Network {
        message: "OneDrive upload aborted: account shutting down".to_string(),
        transmission_state,
        source: None,
    })
}

/// Create a sharing link (one round-trip): POST `createLink`, return
/// `link.webUrl`.
async fn create_sharing_link(
    client: &GraphClient,
    item_id: &str,
    scope: ShareScope,
) -> Result<String, GraphError> {
    let prefix = client.api_path_prefix();
    let path = format!("{prefix}/drive/items/{item_id}/createLink");
    let body = serde_json::json!({
        "type": "view",
        "scope": onedrive_scope(scope),
    });

    let response: CreateLinkResponse = client
        .post(
            Resource::Unmetered,
            &path,
            &body,
            bifrost_net::ReplayPolicy::Never,
        )
        .await?;
    Ok(response.link.web_url)
}

/// Map the uniform `ShareScope` onto OneDrive's `createLink` scope vocabulary.
fn onedrive_scope(scope: ShareScope) -> &'static str {
    match scope {
        ShareScope::Anyone => "anonymous",
        // `ShareScope` is `#[non_exhaustive]`; `Organization` plus any unknown
        // future scope map to the most restrictive tenant-only link rather
        // than a public one.
        _ => "organization",
    }
}

/// Percent-encode characters invalid in OneDrive path segments (`%`, `#`, `?`).
/// Spaces are allowed in OneDrive paths and kept as-is.
fn encode_onedrive_path(filename: &str) -> String {
    filename
        .replace('%', "%25")
        .replace('#', "%23")
        .replace('?', "%3F")
}

/// A complete OneDrive answer that breaks the resumable-upload contract
/// (well-formed, but impossible at that point), classified
/// `Protocol(ContractViolation)` with `Acknowledged` evidence. A body that
/// does not parse at all is `GraphError::Json` instead.
fn contract_violation(message: String) -> GraphError {
    GraphError::ContractViolation { message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::PushMode;
    use crate::client::ScriptedRestResponse;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

    const SESSION_URL: &str = "https://upload.example/session/abc?token=preauth";

    // The production upload path without its progress out-parameter, so the
    // tests below exercise the same composition `upload_and_link` runs.
    async fn upload_file_chunked(
        client: &GraphClient,
        upload_url: &UploadUrl,
        data: Bytes,
        chunk_size: usize,
        shutdown: &CancellationToken,
    ) -> Result<String, GraphError> {
        let mut facts = UploadFacts::default();
        super::upload_file_chunked(client, upload_url, data, chunk_size, shutdown, &mut facts).await
    }
    async fn upload_or_cancel(
        client: &GraphClient,
        upload_url: &UploadUrl,
        data: Bytes,
        chunk_size: usize,
        shutdown: &CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<String, GraphError> {
        let mut facts = UploadFacts::default();
        super::upload_or_cancel(
            client, upload_url, data, chunk_size, shutdown, deadline, &mut facts,
        )
        .await
    }
    async fn upload_chunks(
        client: &GraphClient,
        upload_url: &UploadUrl,
        data: Bytes,
        chunk_size: usize,
        shutdown: &CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<String, GraphError> {
        let mut facts = UploadFacts::default();
        super::upload_chunks(
            client, upload_url, data, chunk_size, shutdown, deadline, &mut facts,
        )
        .await
    }

    /// `SESSION_URL`, through the same admission production applies.
    fn session(client: &GraphClient) -> UploadUrl {
        admit_upload_url(client, SESSION_URL).expect("an https session URL is admitted")
    }

    fn fresh_deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + UPLOAD_TOTAL_TIMEOUT
    }

    fn classify(error: GraphError) -> bifrost_types::AccountError {
        into_account_error(error, ctx())
    }

    /// The whole-upload budget running out between chunks is this client's
    /// own timeout, never a provider parse failure (which it used to be):
    /// `Transport(Timeout)`, `Unsent`, because no PUT is outstanding and no
    /// file exists yet, so nothing is sent.
    #[tokio::test]
    async fn a_spent_budget_between_chunks_is_an_unsent_timeout() {
        let client = GraphClient::new("token");
        client.script_aux(std::iter::empty::<ScriptedRestResponse>());

        let error = upload_chunks(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            5,
            &CancellationToken::new(),
            tokio::time::Instant::now(),
        )
        .await
        .expect_err("a spent budget refuses the next chunk");
        assert!(client.take_aux_requests().is_empty());
        let error = classify(error);
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Timeout)
        );
        assert!(error.recovery().is_retryable(), "{:?}", error.recovery());
    }

    /// The budget expiring while a chunk PUT is outstanding is classified by
    /// bifrost-net at the stage it hit. A PUT that may be the final one of a
    /// non-idempotent upload is read back rather than replayed, since a
    /// replay could create a second file.
    #[tokio::test(start_paused = true)]
    async fn a_budget_expiring_mid_put_reconciles_instead_of_blaming_the_provider() {
        let client = GraphClient::new("token");
        client.script_aux_pending(1);
        // A budget shorter than bifrost-net's per-attempt wait for response
        // headers, so only the budget can end the PUT this early.
        let budget = std::time::Duration::from_secs(5);
        let started = tokio::time::Instant::now();

        let error = upload_chunks(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            CHUNK_ALIGNMENT,
            &CancellationToken::new(),
            started + budget,
        )
        .await
        .expect_err("the budget expires");
        // What is left of the upload budget rode into the transport as the
        // PUT's own deadline.
        assert_eq!(started.elapsed(), budget);
        let error = classify(error);
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Timeout)
        );
        assert!(
            error.recovery().requires_reconciliation(),
            "{:?}",
            error.recovery()
        );
    }

    /// A shutdown that cuts an outstanding PUT short cannot know whether it
    /// was the one that created the file, so it is `InFlight`.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_during_a_put_is_in_flight() {
        let client = GraphClient::new("token");
        client.script_aux_pending(1);
        let shutdown = CancellationToken::new();
        let trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            trigger.cancel();
        });

        let error = upload_file_chunked(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            CHUNK_ALIGNMENT,
            &shutdown,
        )
        .await
        .expect_err("the shutdown aborts the upload");
        assert_eq!(
            client.take_aux_requests().len(),
            1,
            "the PUT was dispatched"
        );
        assert!(
            matches!(
                &error,
                GraphError::Net(bifrost_net::Error::Network {
                    transmission_state: TransmissionState::InFlight,
                    ..
                })
            ),
            "{error:?}"
        );
    }

    /// The resumable chunk PUT, end to end through the aux wire seam.
    ///
    /// Three things this leg cannot get wrong without corrupting an upload
    /// or leaking a credential: the session URL is pre-authenticated, so the
    /// Graph bearer must NOT ride along; each chunk declares its absolute
    /// `Content-Range` against the total; and a 202 resumes from the
    /// SERVER's `nextExpectedRanges` start, not from the end of the chunk
    /// just sent - a server is allowed to accept only part of a chunk, and
    /// advancing to the chunk end would skip the unaccepted tail.
    #[tokio::test]
    async fn chunked_upload_drops_the_bearer_and_resumes_from_the_server_offset() {
        let client = GraphClient::new("token");
        client.script_aux([
            // Accepted only the first three bytes of a five-byte chunk.
            ScriptedRestResponse::json(
                reqwest::StatusCode::ACCEPTED,
                json!({ "nextExpectedRanges": ["3-11"] }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::ACCEPTED,
                json!({ "nextExpectedRanges": ["8-11"] }),
            ),
            ScriptedRestResponse::json(reqwest::StatusCode::OK, json!({ "id": "drive-item-1" })),
        ]);

        let item = upload_chunks(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            5,
            &CancellationToken::new(),
            fresh_deadline(),
        )
        .await
        .expect("the upload completes");
        assert_eq!(item, "drive-item-1");

        let requests = client.take_aux_requests();
        assert_eq!(requests.len(), 3);
        let ranges: Vec<&str> = requests
            .iter()
            .map(|request| request.header("Content-Range").expect("Content-Range"))
            .collect();
        assert_eq!(
            ranges,
            vec!["bytes 0-4/12", "bytes 3-7/12", "bytes 8-11/12"],
            "the second chunk resumes at the server's offset, not at 5"
        );
        let bodies: Vec<Bytes> = requests
            .iter()
            .map(|request| request.body.clone())
            .collect();
        assert_eq!(
            bodies,
            vec![
                Bytes::from_static(b"01234"),
                Bytes::from_static(b"34567"),
                Bytes::from_static(b"89AB"),
            ]
        );
        for request in &requests {
            assert_eq!(request.method, "PUT");
            assert_eq!(request.url, SESSION_URL);
            assert!(
                !request.bearer,
                "the pre-authed session URL must never carry the Graph bearer"
            );
        }
    }

    /// Upload `data` in 5-byte chunks with exactly ONE scripted answer, and
    /// return the classified error plus how many PUTs went out. The script
    /// holds one response, so a loop that resumed after it instead of
    /// refusing would hit the exhausted seam and panic.
    async fn refusal_of_first_answer(
        data: &'static [u8],
        answer: ScriptedRestResponse,
    ) -> (bifrost_types::AccountError, usize) {
        let client = GraphClient::new("token");
        client.script_aux([answer]);
        let error = upload_chunks(
            &client,
            &session(&client),
            Bytes::from_static(data),
            5,
            &CancellationToken::new(),
            fresh_deadline(),
        )
        .await
        .expect_err("the answer must end the upload");
        (classify(error), client.take_aux_requests().len())
    }

    fn accepted(body: serde_json::Value) -> ScriptedRestResponse {
        ScriptedRestResponse::json(reqwest::StatusCode::ACCEPTED, body)
    }

    /// A 202 is the resume signal and nothing else, so one that names no
    /// usable offset is the provider's MALFORMED response. The old code read
    /// each of these as "resume at the end of the chunk just sent" (a body
    /// that did not parse became an empty progress, and an unparseable range
    /// fell back to `end`), which skips every byte the server did not
    /// accept and corrupts the file with no error at all; against it this
    /// test fails, because the upload goes on to a second PUT the script
    /// does not hold.
    #[tokio::test]
    async fn a_202_without_a_parseable_offset_is_a_malformed_response() {
        for answer in [
            ScriptedRestResponse::text(reqwest::StatusCode::ACCEPTED, ""),
            ScriptedRestResponse::text(reqwest::StatusCode::ACCEPTED, "<html>ok</html>"),
            accepted(json!({})),
            accepted(json!({ "nextExpectedRanges": null })),
            accepted(json!({ "nextExpectedRanges": [5] })),
            accepted(json!({ "nextExpectedRanges": ["abc-"] })),
            accepted(json!({ "nextExpectedRanges": ["-"] })),
            // No dash: the old parser read "5" as offset 5.
            accepted(json!({ "nextExpectedRanges": ["5"] })),
            accepted(json!({ "nextExpectedRanges": ["+5-"] })),
            accepted(json!({ "nextExpectedRanges": ["5-3"] })),
        ] {
            let (error, puts) = refusal_of_first_answer(b"0123456789AB", answer).await;
            assert_eq!(puts, 1);
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ParseFailed
                )
            );
        }
    }

    /// The final chunk's 200/201 must carry the drive item. One that does not
    /// decode is the provider's parse failure, and since `GraphError::Json`'s
    /// label names no format, its message must say which body failed: it
    /// used to carry serde's bare text, naming neither OneDrive nor JSON.
    #[tokio::test]
    async fn an_undecodable_drive_item_names_its_source() {
        let (error, puts) = refusal_of_first_answer(
            b"01234",
            ScriptedRestResponse::text(reqwest::StatusCode::CREATED, "<html>ok</html>"),
        )
        .await;
        assert_eq!(puts, 1);
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ParseFailed
            )
        );
        assert!(
            format!("{error:?}").contains("OneDrive upload drive item JSON body"),
            "{error:?}"
        );
    }

    /// A 202 that parses but names an impossible offset is the provider's
    /// CONTRACT VIOLATION, not a parse failure: an empty range list (a 202
    /// that wants nothing more), an offset that does not advance (which
    /// would spin the loop), and an offset past the bytes just sent (bytes
    /// 0-4 went out; resuming at 7 or 20 would skip bytes the server never
    /// received). Against the old code: the empty list resumed at 5, the
    /// forward offsets were followed and skipped bytes 5 and 6 (so the
    /// second PUT hit the exhausted script), and the non-advancing one was
    /// refused as `ParseFailed` through the parse-failure helper.
    #[tokio::test]
    async fn a_202_offset_outside_the_bytes_sent_is_a_contract_violation() {
        for ranges in [json!([]), json!(["0-11"]), json!(["7-11"]), json!(["20-"])] {
            let (error, puts) = refusal_of_first_answer(
                b"0123456789AB",
                accepted(json!({ "nextExpectedRanges": ranges.clone() })),
            )
            .await;
            assert_eq!(puts, 1, "{ranges}");
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ContractViolation
                ),
                "{ranges}"
            );
            assert!(!error.recovery().is_retryable(), "{ranges}");
        }
    }

    /// A 202 that acknowledges the LAST byte has promised completion without
    /// the drive item only a 200/201 carries. The old code let the loop run
    /// out and reported this through the parse-failure helper
    /// (`Protocol(ParseFailed)`); it is a contract violation.
    #[tokio::test]
    async fn a_202_acknowledging_every_byte_is_a_contract_violation() {
        for ranges in [json!([]), json!(["4-"])] {
            let (error, puts) = refusal_of_first_answer(
                b"0123",
                accepted(json!({ "nextExpectedRanges": ranges.clone() })),
            )
            .await;
            assert_eq!(puts, 1, "{ranges}");
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ContractViolation
                ),
                "{ranges}"
            );
        }
    }

    /// The final chunk may be partially accepted like any other: a 202 short
    /// of the total on the last chunk resumes and re-sends the rest. This
    /// passes against the old code too; it pins that refusing a full
    /// acknowledgement did not also refuse a partial one.
    #[tokio::test]
    async fn a_partially_accepted_final_chunk_resumes() {
        let client = GraphClient::new("token");
        client.script_aux([
            accepted(json!({ "nextExpectedRanges": ["5-"] })),
            accepted(json!({ "nextExpectedRanges": ["10-11"] })),
            // The final chunk (10-11): only byte 10 landed.
            accepted(json!({ "nextExpectedRanges": ["11-11"] })),
            ScriptedRestResponse::json(reqwest::StatusCode::CREATED, json!({ "id": "item" })),
        ]);

        let item = upload_chunks(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            5,
            &CancellationToken::new(),
            fresh_deadline(),
        )
        .await
        .expect("the upload completes");
        assert_eq!(item, "item");
        let ranges: Vec<String> = client
            .take_aux_requests()
            .iter()
            .map(|request| request.header("Content-Range").expect("range").to_string())
            .collect();
        assert_eq!(
            ranges,
            vec![
                "bytes 0-4/12",
                "bytes 5-9/12",
                "bytes 10-11/12",
                "bytes 11-11/12"
            ]
        );
    }

    /// A session URL that would send the attachment bytes and the session
    /// credential in the clear, or behind a misleading authority, is refused
    /// at receipt as the provider's contract violation: nothing is PUT and
    /// no sharing link is minted. The old code followed any string: the
    /// chunk PUT went out to the plain-http URL, consuming the scripted
    /// response this test deliberately does not provide.
    #[tokio::test]
    async fn partial_commit_upload_admission_refusal_keeps_the_session() {
        for upload_url in [
            "http://upload.example/session/abc?token=preauth",
            "https://user:secret@upload.example/session/abc",
            "https://upload.example@attacker.example/session/abc",
            "ftp://upload.example/session/abc",
            "/session/abc",
            "not a url",
        ] {
            let client = GraphClient::new("token");
            client.script_rest([ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({ "uploadUrl": upload_url, "expirationDateTime": "2099-01-01T00:00:00Z" }),
            )]);
            let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
            let payload = Bytes::from_static(b"hello");

            let error = host_attachment(
                account,
                payload.clone(),
                CloudUploadMeta::new(
                    "a.txt",
                    "text/plain",
                    payload.len() as u64,
                    ShareScope::Organization,
                ),
            )
            .await
            .expect_err("the upload URL is refused");
            let record = error.partial_commit().expect("session already created");
            assert_eq!(record.primary().effect, None);
            assert_eq!(record.steps()[0].effect(), Some(StepEffect::Created));
            assert!(record.compensations().is_empty());
            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ContractViolation
                ),
                "{upload_url}"
            );
            assert!(client.take_aux_requests().is_empty(), "{upload_url}");
            assert_eq!(client.take_rest_requests().len(), 1, "{upload_url}");
            // The refusal names why, never the credential-bearing URL.
            assert!(
                !format!("{error:?}").contains("upload.example/session"),
                "{upload_url}: {error:?}"
            );
        }
    }

    /// What the admission lets through: https on any host (real session
    /// hosts are SharePoint and OneDrive, never the Graph host), and plain
    /// http only on the configured Graph origin, which already holds the
    /// account bearer. The sent URL is the admitted one.
    #[test]
    fn upload_url_admission_takes_https_anywhere_and_http_only_on_the_graph_origin() {
        let client = GraphClient::new("token");
        for admitted in [
            "https://contoso-my.sharepoint.com/personal/u/_api/v2.0/uploadSession?tempauth=x",
            "HTTPS://api.onedrive.com/rup/abc",
        ] {
            assert!(admit_upload_url(&client, admitted).is_ok(), "{admitted}");
        }

        let local = GraphClient::with_api_base("http://127.0.0.1:8181/v1.0", "token");
        let admitted =
            admit_upload_url(&local, "http://127.0.0.1:8181/upload/abc").expect("same origin");
        assert_eq!(admitted.as_str(), "http://127.0.0.1:8181/upload/abc");
        for refused in [
            "http://127.0.0.1:8182/upload/abc",
            "http://localhost:8181/upload/abc",
            "http://user@127.0.0.1:8181/upload/abc",
        ] {
            admit_upload_url(&local, refused).expect_err(refused);
        }
    }

    /// A cancelled account shutdown aborts the upload between chunks
    /// instead of finishing it, and the abort is `Unsent`-state so the
    /// recovery mapping reads it as a client-side abort rather than a
    /// server failure.
    #[tokio::test]
    async fn a_shutdown_aborts_the_upload_before_the_next_chunk() {
        let client = GraphClient::new("token");
        // Armed and EMPTY: any PUT at all panics at the seam, so "aborted
        // before sending" is proven rather than assumed.
        client.script_aux(std::iter::empty::<ScriptedRestResponse>());
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        // Through the public entry point, which used to race the whole upload
        // against the token and could report this same shutdown as InFlight.
        let error = upload_file_chunked(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            CHUNK_ALIGNMENT,
            &shutdown,
        )
        .await
        .expect_err("a cancelled upload fails");
        assert!(client.take_aux_requests().is_empty());
        assert!(
            matches!(
                &error,
                GraphError::Net(bifrost_net::Error::Network {
                    transmission_state: bifrost_types::TransmissionState::Unsent,
                    ..
                })
            ),
            "{error:?}"
        );
    }

    /// The whole hosting call: `createUploadSession` (bearer, JSON, the
    /// de-branded folder and `rename` conflict behavior), the chunk PUT
    /// against the URL the session handed back (no bearer), then
    /// `createLink` for the requested scope.
    ///
    /// The REST and aux legs share one scripted transport, because in
    /// production they share one wire. The script is therefore in wire
    /// order - session, chunk, link - and that ordering is itself an
    /// assertion: a leg that went out in a different order, or an extra
    /// leg, gets the wrong response or exhausts the script rather than
    /// passing. The two surfaces still record their requests separately
    /// below, since their recorded shapes differ.
    #[tokio::test]
    async fn hosting_an_attachment_uploads_then_mints_a_link() {
        let client = GraphClient::new("token");
        client.script_rest([
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({ "uploadUrl": SESSION_URL, "expirationDateTime": "2099-01-01T00:00:00Z" }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::CREATED,
                json!({ "id": "drive-item-1" }),
            ),
            ScriptedRestResponse::json(
                reqwest::StatusCode::OK,
                json!({ "link": { "webUrl": "https://1drv.ms/x/abc" } }),
            ),
        ]);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let payload = Bytes::from_static(b"hello world");

        let hosted = host_attachment(
            account,
            payload.clone(),
            CloudUploadMeta::new(
                "report #1.pdf",
                "application/pdf",
                payload.len() as u64,
                ShareScope::Organization,
            ),
        )
        .await
        .expect("hosting succeeds");
        assert_eq!(hosted.share_url, "https://1drv.ms/x/abc");
        assert_eq!(hosted.provider_file_id, "drive-item-1");

        let rest = client.take_rest_requests();
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].method, "POST");
        assert!(
            rest[0]
                .url
                // The recorded URL is the admitted one, exactly as sent: the
                // URL parser (the same one reqwest applies) encodes the space.
                .ends_with("/me/drive/root:/Attachments/report%20%231.pdf:/createUploadSession"),
            "{}",
            rest[0].url
        );
        let session_body = rest[0].body.as_ref().expect("session body");
        assert_eq!(
            session_body["@microsoft.graph.conflictBehavior"].as_str(),
            Some("rename"),
            "hosting must never overwrite an existing drive item"
        );
        assert_eq!(session_body["item"]["name"].as_str(), Some("report #1.pdf"));
        assert!(
            rest[1]
                .url
                .contains("/me/drive/items/drive-item-1/createLink")
        );
        assert_eq!(
            rest[1].body.as_ref().expect("link body")["scope"].as_str(),
            Some("organization")
        );

        let aux = client.take_aux_requests();
        assert_eq!(aux.len(), 1);
        assert_eq!(aux[0].url, SESSION_URL);
        assert!(!aux[0].bearer);
        assert_eq!(aux[0].header("Content-Range"), Some("bytes 0-10/11"));
        assert_eq!(aux[0].body, payload);
    }

    /// Host `payload` through the public entry point against a scripted
    /// session answer followed by `aux` (chunk PUTs, then the cancel).
    async fn host_with(
        aux: impl IntoIterator<Item = ScriptedRestResponse>,
    ) -> (GraphClient, bifrost_types::AccountError) {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "uploadUrl": SESSION_URL, "expirationDateTime": "2099-01-01T00:00:00Z" }),
        )]);
        client.script_aux(aux);
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let payload = Bytes::from_static(b"0123456789AB");
        let error = host_attachment(
            account,
            payload.clone(),
            CloudUploadMeta::new(
                "a.txt",
                "text/plain",
                payload.len() as u64,
                ShareScope::Organization,
            ),
        )
        .await
        .expect_err("the upload fails");
        (client, error)
    }

    fn assert_cancelled_once(client: &GraphClient, puts: usize) {
        let aux = client.take_aux_requests();
        let methods: Vec<&str> = aux.iter().map(|request| request.method.as_str()).collect();
        let mut expected = vec!["PUT"; puts];
        expected.push("DELETE");
        assert_eq!(methods, expected);
        let cancel = aux.last().expect("the cancel");
        assert_eq!(
            cancel.url, SESSION_URL,
            "the cancel goes to the admitted URL"
        );
        assert!(
            !cancel.bearer,
            "the session cancel must never carry the Graph bearer"
        );
        assert!(cancel.body.is_empty());
    }

    /// An upload that fails after its session exists cancels the session
    /// (OneDrive would otherwise keep it, and the bytes it accepted, until
    /// it expires), and the caller still sees the upload's own failure.
    /// Against the old code the DELETE was never sent, so the scripted 204
    /// was left unconsumed and the recorded requests held one PUT.
    #[tokio::test]
    async fn a_refused_upload_cancels_its_session_and_keeps_its_own_error() {
        let (client, error) = host_with([
            accepted(json!({ "nextExpectedRanges": ["20-"] })),
            ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT),
        ])
        .await;
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
        assert_cancelled_once(&client, 1);
        assert_eq!(
            client.take_rest_requests().len(),
            1,
            "no sharing link for a failed upload"
        );
    }

    /// A redirect on the chunk PUT is not followed: the session URL was
    /// admitted under the upload rule, and a hop (here to plain http) would
    /// send the chunk somewhere never admitted. It is the provider's
    /// contract violation, the session is cancelled, and the cancel does not
    /// follow a redirect either. The script holds exactly the session, the
    /// PUT and the DELETE answers, so a followed hop on either leg exhausts
    /// it and panics. Against the old code the PUT's hop consumed the
    /// DELETE's answer.
    #[tokio::test]
    async fn a_redirected_chunk_put_is_a_contract_violation_and_is_not_followed() {
        let hop = |status| {
            ScriptedRestResponse::text(status, "moved")
                .with_header("Location", "http://elsewhere.example/landing")
        };
        let (client, error) = host_with([
            hop(reqwest::StatusCode::TEMPORARY_REDIRECT),
            hop(reqwest::StatusCode::PERMANENT_REDIRECT),
        ])
        .await;
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
        assert!(!error.recovery().is_retryable(), "{:?}", error.recovery());
        assert!(
            !format!("{error:?}").contains("elsewhere.example"),
            "{error:?}"
        );
        assert_eq!(client.wire_attempts(), 3, "session, PUT, DELETE and no hop");
        assert_cancelled_once(&client, 1);
    }

    /// The cancel DELETE is sent with redirects disabled, so an unfollowed
    /// 3xx comes back as the response itself. That is OneDrive answering
    /// without accepting the cancel: a `Failed` compensation, as Google
    /// records the same case, never a cancelled session. Its evidence names
    /// the status only, so a `Location` (which may carry the session
    /// credential) never reaches an export. Against the old code any `Ok`
    /// response was recorded as `Succeeded`.
    #[tokio::test]
    async fn partial_commit_upload_session_cancel_redirect_is_a_failed_compensation() {
        let (client, error) = host_with([
            accepted(json!({ "nextExpectedRanges": ["20-"] })),
            ScriptedRestResponse::text(reqwest::StatusCode::PERMANENT_REDIRECT, "moved")
                .with_header(
                    "Location",
                    "https://elsewhere.example/landing?token=preauth",
                ),
        ])
        .await;
        assert_cancelled_once(&client, 1);

        let record = error.partial_commit().expect("the session was created");
        let [compensation] = record.compensations() else {
            panic!("one cancel compensation: {record:?}");
        };
        let CompensationOutcome::Failed(cancel) = compensation.outcome() else {
            panic!("a redirected cancel did not cancel the session: {compensation:?}");
        };
        assert_eq!(
            cancel.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        );
        for export in [
            format!("{error:?}"),
            format!("{:?}", error.support_internal()),
            format!("{:?}", cancel.support_internal()),
        ] {
            assert!(!export.contains("elsewhere.example"), "{export}");
        }
    }

    /// A chunk the server refuses with an error status cancels the session
    /// too, and a cancel that itself fails does not replace the upload's
    /// error.
    #[tokio::test]
    async fn a_failed_cancel_does_not_mask_the_upload_error() {
        let (client, error) = host_with([
            ScriptedRestResponse::empty(reqwest::StatusCode::CONFLICT),
            ScriptedRestResponse::empty(reqwest::StatusCode::FORBIDDEN),
        ])
        .await;
        // The PUT's 409, not the cancel's 403.
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::ConcurrencyConflict
        );
        assert_cancelled_once(&client, 1);
    }

    /// The session URL is the upload credential, and the transport copies
    /// the HTTP stack's error text, which names the URL, into a transport
    /// failure. A failed cancel is kept as the record's compensation, so
    /// that text would reach every export of the error; it is withheld,
    /// while the compensation still classifies as a transport failure.
    #[tokio::test]
    async fn a_failed_cancel_compensation_does_not_carry_the_session_url() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::json(
            reqwest::StatusCode::OK,
            json!({ "uploadUrl": SESSION_URL, "expirationDateTime": "2099-01-01T00:00:00Z" }),
        )]);
        client.script_aux([ScriptedRestResponse::empty(reqwest::StatusCode::CONFLICT)]);
        client.script_wire(
            [bifrost_net::test_support::Canned::Error(
                bifrost_net::Error::Network {
                    message: format!("error sending request for url ({SESSION_URL})"),
                    transmission_state: TransmissionState::Unsent,
                    source: None,
                },
            )],
            bifrost_net::RetryPolicy::disabled(),
        );
        let account = GraphAccount::new_for_tests(client.clone(), PushMode::GraphSubscriptions);
        let payload = Bytes::from_static(b"0123456789AB");
        let error = host_attachment(
            account,
            payload.clone(),
            CloudUploadMeta::new(
                "a.txt",
                "text/plain",
                payload.len() as u64,
                ShareScope::Organization,
            ),
        )
        .await
        .expect_err("the upload fails");
        assert_cancelled_once(&client, 1);

        let record = error.partial_commit().expect("the session was created");
        let [compensation] = record.compensations() else {
            panic!("one cancel compensation: {record:?}");
        };
        let CompensationOutcome::Failed(cancel) = compensation.outcome() else {
            panic!("an unsent cancel failed: {compensation:?}");
        };
        assert_eq!(
            cancel.kind(),
            &bifrost_types::AccountErrorKind::Transport(bifrost_types::TransportErrorKind::Network)
        );
        for export in [
            format!("{error:?}"),
            format!("{:?}", error.support_internal()),
            format!("{:?}", cancel.support_internal()),
        ] {
            assert!(!export.contains("upload.example"), "{export}");
        }
    }

    /// The upload budget running out is exactly when a cancel matters, and
    /// is also when a cancel sharing that budget could never be sent: it
    /// gets its own. The failure is still the upload's own timeout.
    #[tokio::test]
    async fn a_spent_upload_budget_still_cancels_the_session() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::empty(reqwest::StatusCode::NO_CONTENT)]);

        let error = upload_or_cancel(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            5,
            &CancellationToken::new(),
            tokio::time::Instant::now(),
        )
        .await
        .expect_err("the budget is spent");
        assert!(
            matches!(
                &error,
                GraphError::Net(bifrost_net::Error::Timeout {
                    transmission_state: TransmissionState::Unsent,
                })
            ),
            "{error:?}"
        );
        assert_cancelled_once(&client, 0);
    }

    /// A cancel that never answers is bounded by its own budget, and the
    /// upload's failure comes back once it lapses.
    #[tokio::test(start_paused = true)]
    async fn a_hanging_cancel_is_bounded() {
        let client = GraphClient::new("token");
        client.script_aux([accepted(json!({ "nextExpectedRanges": [] }))]);
        client.script_aux_pending(1);
        let started = tokio::time::Instant::now();

        let error = upload_file_chunked(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            CHUNK_ALIGNMENT,
            &CancellationToken::new(),
        )
        .await
        .expect_err("the upload is refused");
        assert!(
            matches!(&error, GraphError::ContractViolation { .. }),
            "{error:?}"
        );
        assert_eq!(started.elapsed(), SESSION_CANCEL_TIMEOUT);
        assert_cancelled_once(&client, 1);
    }

    /// A shutdown that lands while the cancel is outstanding abandons it at
    /// once: a failed upload must not hold up `close()`.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_abandons_an_outstanding_cancel() {
        let client = GraphClient::new("token");
        client.script_aux([accepted(json!({ "nextExpectedRanges": [] }))]);
        client.script_aux_pending(1);
        let shutdown = CancellationToken::new();
        let trigger = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            trigger.cancel();
        });
        let started = tokio::time::Instant::now();

        let error = upload_file_chunked(
            &client,
            &session(&client),
            Bytes::from_static(b"0123456789AB"),
            CHUNK_ALIGNMENT,
            &shutdown,
        )
        .await
        .expect_err("the upload is refused");
        assert!(
            matches!(&error, GraphError::ContractViolation { .. }),
            "{error:?}"
        );
        assert_eq!(started.elapsed(), std::time::Duration::from_secs(1));
        assert_cancelled_once(&client, 1);
    }

    #[tokio::test]
    async fn an_onedrive_session_throttle_has_no_mailbox_tag() {
        let client = GraphClient::new("token");
        client.script_rest([ScriptedRestResponse::empty(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
        )]);
        assert!(matches!(
            create_upload_session(&client, "a.txt").await,
            Err(GraphError::Net(bifrost_net::Error::RateLimited { .. }))
        ));
        assert_eq!(client.wire_attempts(), 1);
    }

    #[test]
    fn onedrive_chunk_aligned() {
        assert_eq!(DEFAULT_CHUNK_SIZE % CHUNK_ALIGNMENT, 0);
        assert_eq!(DEFAULT_CHUNK_SIZE, 5 * 1024 * 1024);
    }

    #[test]
    fn encode_onedrive_path_escapes() {
        assert_eq!(encode_onedrive_path("file.txt"), "file.txt");
        assert_eq!(encode_onedrive_path("file #1.txt"), "file %231.txt");
        assert_eq!(encode_onedrive_path("100% done?.txt"), "100%25 done%3F.txt");
    }

    #[test]
    fn session_path_is_debranded() {
        // The de-branded folder must appear and the ratatoskr brand must not.
        let path = format!(
            "/me/drive/root:/{ATTACHMENTS_FOLDER}/{}:/createUploadSession",
            encode_onedrive_path("report.pdf")
        );
        assert!(path.contains("/Attachments/"), "{path}");
        assert!(!path.contains("Ratatoskr"), "{path}");
    }

    #[test]
    fn share_scope_maps_to_onedrive_scope() {
        assert_eq!(onedrive_scope(ShareScope::Anyone), "anonymous");
        assert_eq!(onedrive_scope(ShareScope::Organization), "organization");
    }

    #[test]
    fn create_link_response_deserializes() {
        let json = r#"{"link":{"webUrl":"https://1drv.ms/x/abc"}}"#;
        let resp: CreateLinkResponse = serde_json::from_str(json).expect("deserializes");
        assert_eq!(resp.link.web_url, "https://1drv.ms/x/abc");
    }

    #[test]
    fn range_start_reads_open_and_closed_ranges() {
        // The server's `nextExpectedRanges` start is the authoritative
        // resume offset, even when it is short of the chunk end the client
        // just PUT (a partially-accepted chunk).
        assert_eq!(range_start("1024-"), Some(1024));
        assert_eq!(range_start("524288-1048575"), Some(524_288));
        assert_eq!(range_start(" 7-7 "), Some(7));
        for junk in ["", "-", "-5", "5", "abc-", "+5-", "5-x", "5-3", "5 -6"] {
            assert_eq!(range_start(junk), None, "{junk:?}");
        }
    }

    #[test]
    fn a_202_body_resumes_at_its_first_range() {
        let body = Bytes::from_static(br#"{"nextExpectedRanges":["327680-","700000-"]}"#);
        assert_eq!(
            admitted_resume_offset(&body, 0, 327_680, 1_000_000).expect("admitted"),
            327_680
        );
    }

    #[test]
    fn upload_session_deserializes() {
        let json = r#"{"uploadUrl":"https://upload.example/123","expirationDateTime":"2025-01-01T00:00:00Z"}"#;
        let session: UploadSession = serde_json::from_str(json).expect("deserializes");
        assert_eq!(session.upload_url, "https://upload.example/123");
    }
}
