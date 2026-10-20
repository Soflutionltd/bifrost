//! Google Drive large-attachment hosting: resumable upload + sharing link.
//!
//! COPY-AND-ADAPTed from ratatoskr's `gmail/gdrive.rs`. The pure pieces (serde
//! structs, chunk-range math, the scope vocabulary) copy cleanly; the transport
//! bodies are rewritten onto bifrost-net's `RequestBuilder`/`Response` - no bare
//! `reqwest::Client`, no hand-built `Authorization: Bearer` headers (the
//! AccountNet token source supplies them; the pre-authenticated chunk PUT skips
//! auth with `.without_bearer_auth()`).
//!
//! The whole flow is one call: upload the bytes via the resumable session, then
//! create a sharing permission and read the `webViewLink`. It returns only when
//! both steps succeed; a stray uploaded-but-unlinked file is the worst failure
//! mode, so a failed link step after a successful upload is still an `Err`.
//!
//! The UPLOAD step has an equivalent, and it is handled here rather than left to
//! the consumer. A resumable session is server-side state created before the
//! first byte is sent, and Drive holds an incomplete one for about a week. Any
//! failure between `create_upload_session` and a completed upload therefore
//! leaves a partial upload behind, so the upload step cancels its own session on
//! the way out (`DELETE` against the session URI) before returning the original
//! error. Cancellation is one bounded, un-retried request whose own failure is
//! swallowed: it must never replace or delay the error the caller actually needs.
//!
//! Two things this deliberately does NOT do. It does not reclassify the failure
//! as `Protocol(PartialResponse)` / `Acknowledged` the way the cross-calendar
//! event move does - that vocabulary says "the first leg landed and a consumer
//! can go look at the target", and here there is no target: Drive publishes no
//! file until the upload completes, so a `CheckTarget` / `DedupeByClientId`
//! directive would send a consumer looking for something that provably does not
//! exist, and would demote a transient transport failure out of the retry lane
//! it belongs in. It also does not resume the session on a later call: the
//! session URI is per-call state that no `HostAttachment` request carries back
//! in, so resumption would require a published surface change.
//!
//! Where cancellation itself fails, the returned `AccountError` is decorated
//! with support-only text saying the session was left behind, so a support
//! export can tell "cleaned up" from "expires in a week". The decoration goes
//! through `into_builder`, which preserves the kind and hence the recovery
//! class. The session URI is pre-authenticated and is deliberately NOT written
//! into that text.
//!
//! Residual, accepted: a caller who drops the returned future mid-upload gets no
//! cancellation at all, because a `Drop` impl cannot await. The alternative - a
//! guard that spawns the DELETE from `drop` - trades an expiring server-side
//! session for a detached task outliving the account handle, which is a worse
//! leak in a crate whose shutdown story is the caller dropping the future.
//!
//! The chunk PUTs and the cancel DELETE go to the pre-authenticated session URI
//! with redirects disabled per request (`FollowRedirects::Disabled`), so every
//! 3xx comes back to this module as a plain response and nothing is ever sent
//! to a `Location`. Drive's 308 Resume Incomplete carries a `Range` header and
//! NO `Location`, so it reaches the chunk loop as progress; a 3xx that DOES
//! carry a `Location` is a real redirect the resumable protocol never uses, and
//! is refused as `Protocol(ContractViolation)` without echoing the `Location`.
//! A redirected cancel is reported as an abandoned session.
//!
//! Session creation, chunk PUTs and cancel DELETEs all use the guarded
//! `GmailClient` builder funnel, including its API-surface attribution and
//! inbound byte accounting. A session Location is accepted only on the HTTPS
//! Drive upload surface; chunk and cancel also check it before dispatch.
//!
//! Audit boundary: the 2026-07 google bug sweep did NOT line-audit this
//! module; it was skipped as already test-dense. A later auditor should read
//! it fresh rather than assuming that sweep covered it.

use std::sync::Arc;
use std::time::Duration;

use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, CloudUploadMeta, DiagnosticText,
    HostedAttachment, ShareScope,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::account::error::{GmailErrorContext, into_account_error};
use crate::client::GmailClient;
use crate::error::{Error, GmailResponseHeaders, GmailService, ProviderFault};

/// Minimum alignment for upload chunks (256 KiB per Google Drive API spec).
const GDRIVE_CHUNK_ALIGN: usize = 256 * 1024;

/// Default chunk size: 5 MiB. Must be a multiple of `GDRIVE_CHUNK_ALIGN`.
const GDRIVE_CHUNK_SIZE: usize = 5 * 1024 * 1024;
const GDRIVE_MAX_EXTRA_CHUNK_ATTEMPTS: usize = 8;

/// Wall-clock bound on the session-cancel request. The account default request
/// timeout is `None`, so without an explicit bound a cleanup DELETE against an
/// unresponsive host would hold the caller's already-failed upload open
/// indefinitely. Cleanup is allowed to fail; it is not allowed to hang.
const GDRIVE_CANCEL_TIMEOUT: Duration = Duration::from_secs(30);

/// Support-only marker recorded when an upload failed AND the session cancel
/// also failed, so Drive keeps the partial upload until it expires.
const ABANDONED_SESSION_TEXT: &str = "google drive resumable upload session could not be cancelled after the upload failed; \
     the partial upload remains on the drive until Drive expires it (about one week)";

const SESSION_URL: &str = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable";

/// The completed file metadata returned after a successful upload.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GDriveFileResponse {
    id: String,
    #[serde(default)]
    #[allow(dead_code)]
    name: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    web_view_link: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    size: Option<String>,
}

/// Request body for file metadata when creating an upload session.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileMetadata<'a> {
    name: &'a str,
    mime_type: &'a str,
}

/// Response from getting file metadata with `webViewLink`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileWebLinkResponse {
    web_view_link: String,
}

/// Dispatch entry point wired from the `Account` impl. Drives the whole
/// upload + link flow on a cloned client handle.
pub(crate) fn host_attachment(
    client: Arc<GmailClient>,
    account_email: String,
    bytes: Bytes,
    meta: CloudUploadMeta,
    shutdown: tokio_util::sync::CancellationToken,
) -> AccountFuture<Result<HostedAttachment, AccountError>> {
    Box::pin(async move {
        if shutdown.is_cancelled() {
            return Err(super::account_closed_error(
                AccountOperation::HostAttachment,
            ));
        }
        run_with_shutdown(&client, &account_email, bytes, meta, shutdown).await
    })
}

/// What became of the resumable session on a failing path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionDisposition {
    /// No session was open when the failure happened - it had not been created
    /// yet, or the upload had already completed and closed it.
    NotOpen,
    /// The session was open and the cancel request was accepted, so no partial
    /// upload is left on the drive.
    Cancelled,
    /// The session was open and could not be cancelled. Drive holds the partial
    /// upload until it expires.
    Abandoned,
}

#[cfg(test)]
async fn run(
    client: &GmailClient,
    account_email: &str,
    bytes: Bytes,
    meta: CloudUploadMeta,
) -> Result<HostedAttachment, AccountError> {
    run_with_shutdown(
        client,
        account_email,
        bytes,
        meta,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
}

async fn run_with_shutdown(
    client: &GmailClient,
    account_email: &str,
    bytes: Bytes,
    meta: CloudUploadMeta,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<HostedAttachment, AccountError> {
    match upload_and_link(client, account_email, bytes, meta, shutdown).await {
        Ok(hosted) => Ok(hosted),
        Err((error, disposition)) => {
            let error = into_account_error(error, ctx());
            Err(match disposition {
                SessionDisposition::NotOpen | SessionDisposition::Cancelled => error,
                // Decoration, not reclassification: the kind and primary cause
                // ride through unchanged, so `recovery` derives to exactly what
                // it would have without the note.
                SessionDisposition::Abandoned => error
                    .into_builder()
                    .text(DiagnosticText::support_only(ABANDONED_SESSION_TEXT))
                    .try_build()
                    .expect("decorating an existing classification preserves its invariants"),
            })
        }
    }
}

fn ctx() -> GmailErrorContext {
    let mut ctx = GmailErrorContext::base(AccountOperation::HostAttachment);
    ctx.surface = crate::quota::ApiSurface::Drive;
    ctx
}

async fn upload_and_link(
    client: &GmailClient,
    account_email: &str,
    bytes: Bytes,
    meta: CloudUploadMeta,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<HostedAttachment, (Error, SessionDisposition)> {
    if meta.size != bytes.len() as u64 {
        return Err((
            Error::invalid_request(
                AccountOperation::HostAttachment,
                format!(
                    "declared size {} does not match payload length {}",
                    meta.size,
                    bytes.len()
                ),
            ),
            SessionDisposition::NotOpen,
        ));
    }
    // Refused here, before the session POST, so the refusal really is of an
    // unsent request. Raised inside the chunk loop instead, it came after a
    // session had been created (and then had to be cancelled).
    if bytes.is_empty() {
        return Err((
            Error::invalid_request(AccountOperation::HostAttachment, "cannot upload empty file"),
            SessionDisposition::NotOpen,
        ));
    }

    // The permission body needs the account's domain for domain-scoped
    // sharing. Deriving it after the upload refused only once the bytes were
    // on the drive, leaving an uploaded file that nothing shares; derived
    // here, an unusable account email costs nothing.
    let permission =
        permission_body(meta.scope, account_email).map_err(|e| (e, SessionDisposition::NotOpen))?;

    // Let session creation finish even during shutdown: interrupting it can
    // discard the only response carrying the URI needed for cleanup. The
    // transport request deadline still bounds this wait.
    let upload_url = create_upload_session(client, &meta)
        .await
        .map_err(|e| (e, SessionDisposition::NotOpen))?;

    // From here until the upload completes there is server-side state to clean
    // up, so every exit on this leg goes through the cancel.
    //
    // Account shutdown interrupts the chunk loop below and awaits cleanup.
    // Every exit except one: if the caller drops this future mid-upload, no
    // cancel runs and Drive holds the partial for a week. A `Drop` guard is
    // deliberately NOT the fix. `Drop` cannot await, and spawning the DELETE
    // from `drop` trades an expiring server-side session for a detached task
    // that outlives the account handle. Resuming on reopen is out too: the
    // session URI is per-call state that no `HostAttachment` request carries
    // back in, so resumption would need a published surface change.
    let uploaded = tokio::select! {
        biased;
        () = shutdown.cancelled() => Err(Error::Net(bifrost_net::Error::Cancelled)),
        result = upload_file_chunked(client, &upload_url, bytes, GDRIVE_CHUNK_SIZE) => result,
    };
    let file_id = match uploaded {
        Ok(file_id) => file_id,
        Err(error) => {
            let disposition = cancel_upload_session(client, &upload_url).await;
            return Err((error, disposition));
        }
    };

    // The session closed itself when the final chunk was accepted; a failure
    // from here leaves an uploaded-but-unlinked file, not a partial session,
    // and the DELETE below would not address it.
    let share_url = create_sharing_permission(client, &file_id, &permission)
        .await
        .map_err(|e| (e, SessionDisposition::NotOpen))?;

    Ok(HostedAttachment::new(share_url, file_id))
}

/// Cancel an abandoned resumable session so Drive does not hold the partial
/// upload for a week.
///
/// Deliberately one un-retried, deadline-bounded request against the
/// pre-authenticated session URI, and deliberately infallible from the caller's
/// point of view: this runs on a path that already has an error to report, and
/// the cleanup must neither replace that error nor extend the failure by a
/// retry budget's worth of backoff.
///
/// Google answers an accepted cancel with `499 Client Closed Request`, which
/// bifrost-net's retry loop surfaces as `Err(Status)` rather than
/// `Ok(Response)` - a 4xx never arrives as a success here. A `404`/`410` means
/// the session is already gone, which is the outcome we wanted. Anything else
/// is reported as abandoned.
async fn cancel_upload_session(client: &GmailClient, upload_url: &str) -> SessionDisposition {
    if !reqwest::Url::parse(upload_url).is_ok_and(|url| is_drive_upload_url(&url)) {
        return SessionDisposition::Abandoned;
    }
    // Not followed, for the same reason as the chunk PUT: the session URI is
    // pre-authenticated and admitted only as itself.
    let request = client
        .account_net()
        .delete(upload_url)
        .without_bearer_auth()
        .follow_redirects(bifrost_net::FollowRedirects::Disabled)
        .retry(bifrost_net::RetryPolicy::disabled())
        .timeout(GDRIVE_CANCEL_TIMEOUT);
    let outcome = client.execute_builder(request, "Drive API").await;

    match outcome {
        // An unfollowed 3xx is not Drive accepting the cancel.
        Ok(response) if response.status().is_redirection() => SessionDisposition::Abandoned,
        Ok(_) => SessionDisposition::Cancelled,
        Err(error)
            if super::error::response_parts(&error)
                .is_some_and(|(status, _)| matches!(status, 404 | 410 | 499)) =>
        {
            SessionDisposition::Cancelled
        }
        Err(_) => SessionDisposition::Abandoned,
    }
}

/// Create a resumable upload session. Returns the pre-authenticated upload URL
/// read from the `Location` response header.
///
/// The typed `GmailClient::post` helper cannot set `X-Upload-Content-*`, so
/// this uses the raw builder funnel, including its API-surface redirect guard;
/// the AccountNet token source supplies the Bearer.
async fn create_upload_session(
    client: &GmailClient,
    meta: &CloudUploadMeta,
) -> Result<String, Error> {
    let metadata = FileMetadata {
        name: &meta.file_name,
        mime_type: &meta.mime,
    };

    let request = client
        .account_net()
        .post(SESSION_URL)
        .header("X-Upload-Content-Type", meta.mime.as_str())
        .header("X-Upload-Content-Length", &meta.size.to_string())
        .json(&metadata);
    let response = client.execute_builder(request, "Drive API").await?;

    let status = response.status();
    if !status.is_success() {
        return Err(response_error(
            &response.headers,
            status.as_u16(),
            response.body,
        ));
    }

    // A success status with no usable session URI is Drive breaking the
    // resumable protocol, not the caller asking for something wrong. An
    // absent header is missing; a present one that is not visible-ASCII text,
    // or not an absolute http(s) URL with a host, did not parse. An empty or
    // relative value accepted here would send every chunk PUT and the cancel
    // DELETE to a bogus URL and surface as a transport failure instead.
    let Some(location) = response.headers.get("location") else {
        return Err(Error::provider_response(
            ProviderFault::MissingField,
            "upload session response missing Location header",
        ));
    };
    let unparseable = |why: &str| {
        Error::provider_response(
            ProviderFault::ParseFailed,
            format!("upload session response Location header {why}"),
        )
    };
    let text = location
        .to_str()
        .map_err(|_| unparseable("is not valid text"))?;
    let url = reqwest::Url::parse(text).map_err(|_| unparseable("is not an absolute URL"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none_or(str::is_empty) {
        return Err(unparseable("is not an http(s) URL with a host"));
    }
    if !is_drive_upload_url(&url) {
        return Err(Error::provider_response(
            ProviderFault::ContractViolation,
            "upload session Location is outside the Drive upload surface",
        ));
    }
    Ok(text.to_owned())
}

fn is_drive_upload_url(url: &reqwest::Url) -> bool {
    url.scheme() == "https"
        && url.host_str() == Some("www.googleapis.com")
        && url.port_or_known_default() == Some(443)
        && url.username().is_empty()
        && url.password().is_none()
        && (url.path() == "/upload/drive/v3/files"
            || url.path().starts_with("/upload/drive/v3/files/"))
}

/// Upload the payload in `chunk_size`-aligned chunks against the pre-authenticated
/// session URL.
///
/// 200/201 -> the final chunk was accepted; parse the file metadata. 308 ->
/// resume, advancing the offset from the parsed `Range: bytes=0-N` header.
///
/// Bug fix vs. the ratatoskr source: a 308 never advances by `offset = end`. The
/// old fallback silently skips a gap whenever the server accepted fewer bytes
/// than were sent, corrupting the upload.
///
/// Every refusal below comes after a complete response, and is classified by
/// what that response did. Drive persisting bytes we never sent, forgetting
/// bytes it already reported, or reporting every byte without completing is the
/// provider breaking the resumable protocol (`Protocol(ContractViolation)`); a
/// `Range` that does not parse is `Protocol(ParseFailed)`. A chunk that made no
/// progress is legal - Drive documents resending from its reported offset - so
/// refusing it, like the total attempt budget, is this crate's own limit
/// (`Internal(LimitExceeded)`). None of it is the caller's fault.
async fn upload_file_chunked(
    client: &GmailClient,
    upload_url: &str,
    data: Bytes,
    chunk_size: usize,
) -> Result<String, Error> {
    if !reqwest::Url::parse(upload_url).is_ok_and(|url| is_drive_upload_url(&url)) {
        return Err(Error::provider_response(
            ProviderFault::ContractViolation,
            "upload session is outside the Drive upload surface",
        ));
    }
    // Both are guaranteed by the callers (the chunk size is a constant, and
    // `upload_and_link` refuses an empty payload before opening a session), so
    // reaching either is this crate's bug.
    if chunk_size == 0 || !chunk_size.is_multiple_of(GDRIVE_CHUNK_ALIGN) {
        return Err(Error::internal(format!(
            "chunk_size must be a positive multiple of {GDRIVE_CHUNK_ALIGN}, got {chunk_size}"
        )));
    }

    let total = data.len();
    if total == 0 {
        return Err(Error::internal(
            "empty payload reached the chunk loop; it is refused before the session opens",
        ));
    }

    let mut offset = 0usize;
    let max_attempts = total
        .div_ceil(chunk_size)
        .saturating_add(GDRIVE_MAX_EXTRA_CHUNK_ATTEMPTS);
    let mut attempts = 0usize;
    while offset < total {
        attempts = attempts.saturating_add(1);
        if attempts > max_attempts {
            return Err(Error::limit_exceeded_after_response(format!(
                "resumable upload exceeded {max_attempts} chunk attempts"
            )));
        }
        let end = (offset + chunk_size).min(total);
        let chunk = data.slice(offset..end);
        let content_range = format!("bytes {offset}-{}/{total}", end - 1);

        // Redirects are never followed: the session URI is pre-authenticated
        // and admitted only as itself, so a hop would carry the payload
        // somewhere nobody admitted. Drive's `308 Resume Incomplete` carries
        // no `Location` and still arrives here as a plain response.
        let request = client
            .account_net()
            .put(upload_url)
            .without_bearer_auth()
            .follow_redirects(bifrost_net::FollowRedirects::Disabled)
            .header("Content-Range", &content_range)
            .body(chunk);
        let response = client.execute_builder(request, "Drive API").await?;

        let status = response.status().as_u16();
        if response.status().is_redirection() && response.headers.contains_key("location") {
            return Err(redirect_refused("chunk upload", status));
        }
        match status {
            200 | 201 => {
                let file: GDriveFileResponse = serde_json::from_slice(response.body.as_ref())
                    .map_err(|source| Error::JsonDecode {
                        service: GmailService::GmailApi,
                        source,
                    })?;
                return Ok(file.id);
            }
            308 => {
                let resumed = parse_resume_offset(&response.headers)?;
                if resumed > end {
                    return Err(Error::provider_response(
                        ProviderFault::ContractViolation,
                        format!(
                            "resumable 308 reports {resumed} bytes received after only {end} were sent"
                        ),
                    ));
                }
                if resumed < offset {
                    return Err(Error::provider_response(
                        ProviderFault::ContractViolation,
                        format!(
                            "resumable 308 reports {resumed} bytes received, fewer than the {offset} it already reported"
                        ),
                    ));
                }
                if resumed == offset {
                    return Err(Error::limit_exceeded_after_response(format!(
                        "resumable 308 made no progress from {offset}; refusing to resend without progress"
                    )));
                }
                if resumed == total {
                    return Err(Error::provider_response(
                        ProviderFault::ContractViolation,
                        format!(
                            "resumable 308 reports all {total} bytes received but did not complete the upload"
                        ),
                    ));
                }
                offset = resumed;
            }
            _ => {
                return Err(response_error(&response.headers, status, response.body));
            }
        }
    }

    // Unreachable: `offset` only ever takes a 308's `resumed`, which the arm
    // above has already refused when it reaches `total`.
    Err(Error::internal(
        "chunk loop ended without a file response or a refusal",
    ))
}

/// A 3xx carrying a `Location` against the pre-authenticated session URI: a
/// real redirect, which the resumable protocol never uses (its 308 carries no
/// `Location`). The provider broke the protocol, and the complete response
/// makes it acknowledged. The `Location` is deliberately not echoed: it is
/// server-chosen and may itself carry credentials.
fn redirect_refused(leg: &str, status: u16) -> Error {
    Error::provider_response(
        ProviderFault::ContractViolation,
        format!("resumable {leg} answered {status} with a Location; redirects are not followed"),
    )
}

/// Parse the number of bytes Drive has persisted from a 308 `Range: bytes=0-N`
/// header (`N + 1`). An ABSENT header is Drive's documented way of saying no
/// bytes have been received, so it reads as zero; the caller's progress check
/// then refuses it, and it can never skip a gap. A present header that does not
/// parse is an error, never a guess.
fn parse_resume_offset(headers: &reqwest::header::HeaderMap) -> Result<usize, Error> {
    let Some(value) = headers.get("range") else {
        return Ok(0);
    };
    value
        .to_str()
        .ok()
        .and_then(|range| range.strip_prefix("bytes=0-"))
        .and_then(|end| end.parse::<usize>().ok())
        .and_then(|last_byte| last_byte.checked_add(1))
        .ok_or_else(|| {
            Error::provider_response(
                ProviderFault::ParseFailed,
                "resumable 308 had an unparseable Range header; refusing to skip a gap",
            )
        })
}

/// The Drive `permissions` body for `scope`, built before anything is sent.
fn permission_body(scope: ShareScope, account_email: &str) -> Result<serde_json::Value, Error> {
    Ok(match scope {
        ShareScope::Anyone => serde_json::json!({
            "role": "reader",
            "type": "anyone",
        }),
        // `ShareScope` is `#[non_exhaustive]`; `Organization` plus any unknown
        // future scope are treated as the most restrictive (domain-only)
        // sharing rather than silently widening to `anyone`.
        _ => serde_json::json!({
            "role": "reader",
            "type": "domain",
            "domain": account_domain(account_email)?,
        }),
    })
}

/// Create a sharing permission (two round-trips): POST the permission, then GET
/// the `webViewLink`. These are header-free JSON calls, so the typed
/// `post`/`get` helpers are correct here.
async fn create_sharing_permission(
    client: &GmailClient,
    file_id: &str,
    body: &serde_json::Value,
) -> Result<String, Error> {
    let file_id = bifrost_net::url::encode_path_component(file_id);
    let path = format!("https://www.googleapis.com/drive/v3/files/{file_id}/permissions?fields=id");
    let _perm: serde::de::IgnoredAny = client.post(&path, body).await?;

    let link_path =
        format!("https://www.googleapis.com/drive/v3/files/{file_id}?fields=webViewLink");
    let file: FileWebLinkResponse = client.get(&link_path).await?;
    Ok(file.web_view_link)
}

/// Derive the account's primary domain from its email address, for the
/// `type: domain` Drive permission.
///
/// The address is the one Gmail's `users.getProfile` returned when the account
/// opened, not caller input, so an address with no domain is the provider's
/// value failing to parse (`Protocol(ParseFailed)`), refused before this
/// exchange sends anything (`Unsent`).
fn account_domain(account_email: &str) -> Result<String, Error> {
    account_email
        .rsplit_once('@')
        .map(|(_, domain)| domain.to_owned())
        .filter(|d| !d.is_empty())
        .ok_or_else(|| {
            Error::provider_value_unusable(
                ProviderFault::ParseFailed,
                "the profile's account email has no domain for Organization-scoped sharing",
            )
        })
}

fn response_error(headers: &reqwest::header::HeaderMap, status: u16, body: Bytes) -> Error {
    let parsed = GmailResponseHeaders::from_headers(headers);
    Error::response_from_parts(GmailService::GmailApi, status, parsed, body)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn review_r1_drive_session_redirect_to_held_gmail_is_rejected() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("location", "https://gmail.test/profile".parse().unwrap());
        let script = ScriptedDispatch::new([
            Canned::Response {
                status: reqwest::StatusCode::TEMPORARY_REDIRECT,
                headers,
                body: Bytes::new(),
            },
            session_created(),
        ]);
        let client = scripted_client(&script).with_quota_project("123".into());
        let wall = std::time::SystemTime::UNIX_EPOCH;
        client
            .account_net()
            .shared_hold("gmail-project|123")
            .record(
                bifrost_net::HoldDeadline::new(
                    wall + Duration::from_secs(60),
                    wall,
                    tokio::time::Instant::now(),
                ),
                bifrost_net::HoldKind::QuotaExhausted,
            );
        let error = create_upload_session(&client, &upload_meta())
            .await
            .expect_err("cross-surface redirect rejected");
        assert!(
            matches!(
                error,
                Error::Net(ref net) if matches!(net.terminal(), bifrost_net::Error::RedirectRejected { .. })
            ),
            "unexpected error: {error:?}"
        );
        assert_eq!(script.requests().len(), 1);
    }

    fn final_review_held_client(script: &Arc<ScriptedDispatch>) -> GmailClient {
        let client = scripted_client(script).with_quota_project("123".into());
        let wall = std::time::SystemTime::UNIX_EPOCH;
        client
            .account_net()
            .shared_hold("gmail-project|123")
            .record(
                bifrost_net::HoldDeadline::new(
                    wall + Duration::from_secs(60),
                    wall,
                    tokio::time::Instant::now(),
                ),
                bifrost_net::HoldKind::QuotaExhausted,
            );
        client
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_successful_session_cannot_name_gmail() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("location", "https://gmail.test/session".parse().unwrap());
        let script = ScriptedDispatch::new([
            Canned::Response {
                status: reqwest::StatusCode::OK,
                headers,
                body: Bytes::new(),
            },
            Canned::Response {
                status: reqwest::StatusCode::OK,
                headers: Default::default(),
                body: Bytes::from_static(br#"{"id":"wrong"}"#),
            },
        ]);
        let client = final_review_held_client(&script);
        let result = create_upload_session(&client, &upload_meta()).await;
        if let Ok(url) = &result {
            let _ = upload_file_chunked(
                &client,
                url,
                Bytes::from_static(b"payload"),
                GDRIVE_CHUNK_SIZE,
            )
            .await;
        }
        assert_eq!(script.requests().len(), 1, "no Gmail wire call");
        assert!(result.is_err(), "cross-surface session URL must be refused");
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_chunk_rejects_non_drive_surface() {
        let script = ScriptedDispatch::new([Canned::Response {
            status: reqwest::StatusCode::OK,
            headers: Default::default(),
            body: Bytes::from_static(br#"{"id":"wrong"}"#),
        }]);
        let client = final_review_held_client(&script);
        let result = upload_file_chunked(
            &client,
            "https://gmail.test/session",
            Bytes::from_static(b"payload"),
            GDRIVE_CHUNK_SIZE,
        )
        .await;
        assert!(
            script.requests().is_empty(),
            "held Gmail surface must not dispatch"
        );
        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn final_review_cancel_rejects_non_drive_surface() {
        let script = ScriptedDispatch::new([cancel_accepted()]);
        let client = final_review_held_client(&script);
        let disposition = cancel_upload_session(&client, "https://gmail.test/session").await;
        assert!(
            script.requests().is_empty(),
            "held Gmail surface must not dispatch"
        );
        assert_eq!(disposition, SessionDisposition::Abandoned);
    }

    fn session_created() -> Canned {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "location",
            "https://www.googleapis.com/upload/drive/v3/files/session/abc"
                .parse()
                .expect("valid URL"),
        );
        Canned::Response {
            status: reqwest::StatusCode::OK,
            headers,
            body: Bytes::new(),
        }
    }

    fn network_failure() -> Canned {
        Canned::Error(bifrost_net::Error::Network {
            message: "connection reset mid-chunk".to_owned(),
            transmission_state: bifrost_types::TransmissionState::InFlight,
            source: None,
        })
    }

    fn scripted_client(script: &Arc<ScriptedDispatch>) -> GmailClient {
        let net = bifrost_net::test_support::scripted_account(
            script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        GmailClient::with_account_net("https://gmail.test", net)
    }

    fn upload_meta() -> CloudUploadMeta {
        CloudUploadMeta::new("report.pdf", "application/pdf", 7, ShareScope::Anyone)
    }

    fn resume_response(last_byte: usize) -> Canned {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "range",
            format!("bytes=0-{last_byte}").parse().expect("valid Range"),
        );
        Canned::Response {
            status: reqwest::StatusCode::PERMANENT_REDIRECT,
            headers,
            body: Bytes::new(),
        }
    }

    #[test]
    fn chunk_size_must_be_aligned() {
        assert_eq!(GDRIVE_CHUNK_SIZE % GDRIVE_CHUNK_ALIGN, 0);
        assert_eq!(GDRIVE_CHUNK_SIZE, 5 * 1024 * 1024);
    }

    #[test]
    fn chunk_range_calculation() {
        let total = 12 * 1024 * 1024usize; // 12 MiB
        let chunk_size = GDRIVE_CHUNK_SIZE; // 5 MiB

        let offset = 0;
        let end = (offset + chunk_size).min(total);
        let range = format!("bytes {offset}-{}/{total}", end - 1);
        assert_eq!(range, format!("bytes 0-{}/{total}", 5 * 1024 * 1024 - 1));

        let offset = end;
        let end = (offset + chunk_size).min(total);
        let range = format!("bytes {offset}-{}/{total}", end - 1);
        assert_eq!(
            range,
            format!("bytes {}-{}/{total}", 5 * 1024 * 1024, 10 * 1024 * 1024 - 1)
        );

        let offset = end;
        let end = (offset + chunk_size).min(total);
        let range = format!("bytes {offset}-{}/{total}", end - 1);
        assert_eq!(
            range,
            format!(
                "bytes {}-{}/{total}",
                10 * 1024 * 1024,
                12 * 1024 * 1024 - 1
            )
        );
    }

    #[test]
    fn gdrive_file_response_deserializes() {
        let json = r#"{
            "id": "abc123",
            "name": "report.pdf",
            "webViewLink": "https://drive.google.com/file/d/abc123/view",
            "size": "1048576"
        }"#;
        let file: GDriveFileResponse = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(file.id, "abc123");
        assert_eq!(file.name.as_deref(), Some("report.pdf"));
        assert_eq!(
            file.web_view_link.as_deref(),
            Some("https://drive.google.com/file/d/abc123/view")
        );

        let minimal: GDriveFileResponse =
            serde_json::from_str(r#"{"id":"xyz789"}"#).expect("minimal should deserialize");
        assert_eq!(minimal.id, "xyz789");
        assert!(minimal.web_view_link.is_none());
    }

    #[test]
    fn permission_body_anyone_and_domain() {
        let anyone = serde_json::json!({ "role": "reader", "type": "anyone" });
        assert_eq!(anyone["type"], "anyone");

        let domain = serde_json::json!({
            "role": "reader",
            "type": "domain",
            "domain": "example.com",
        });
        assert_eq!(domain["type"], "domain");
        assert_eq!(domain["domain"], "example.com");
    }

    #[test]
    fn share_scope_maps_to_drive_vocab() {
        // Anyone -> type: anyone; Organization -> type: domain with the
        // account's domain.
        assert_eq!(account_domain("user@example.com").unwrap(), "example.com");
        assert!(account_domain("no-domain").is_err());
        assert!(account_domain("trailing@").is_err());
    }

    #[test]
    fn resume_offset_parses_range_header() {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert("range", "bytes=0-1048575".parse().unwrap());
        assert_eq!(parse_resume_offset(&h).unwrap(), 1_048_576);
    }

    #[test]
    fn unparseable_308_range_fails_does_not_skip_gap() {
        // An absent Range header on a 308 is Drive's documented "no bytes
        // received": it reads as zero, never as `offset = end` (which would
        // silently skip a gap if the server accepted fewer bytes than were
        // sent). The chunk loop then refuses it as no progress; see
        // `an_absent_range_after_progress_is_refused_without_skipping_a_gap`.
        let empty = reqwest::header::HeaderMap::new();
        assert_eq!(parse_resume_offset(&empty).unwrap(), 0);

        // A present-but-garbage Range header must fail.
        let mut garbage = reqwest::header::HeaderMap::new();
        garbage.insert("range", "bytes=garbage".parse().unwrap());
        assert!(parse_resume_offset(&garbage).is_err());
    }

    #[tokio::test]
    async fn repeated_resume_offset_terminates_instead_of_spinning() {
        let script = ScriptedDispatch::new([resume_response(0), resume_response(0)]);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        let client = GmailClient::with_account_net("https://gmail.test", net);

        let result = upload_file_chunked(
            &client,
            "https://www.googleapis.com/upload/drive/v3/files/session",
            Bytes::from_static(b"payload"),
            GDRIVE_CHUNK_ALIGN,
        )
        .await;

        assert!(
            result.is_err(),
            "a repeated offset must terminate the upload"
        );
        assert_eq!(
            script.requests().len(),
            2,
            "the loop must stop on the first stall"
        );
    }

    #[tokio::test]
    async fn tiny_resume_progress_is_bounded_by_an_attempt_limit() {
        let responses = (0..9).map(resume_response).collect::<Vec<_>>();
        let script = ScriptedDispatch::new(responses);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        let client = GmailClient::with_account_net("https://gmail.test", net);

        let result = upload_file_chunked(
            &client,
            "https://www.googleapis.com/upload/drive/v3/files/session",
            Bytes::from_static(b"a payload longer than nine bytes"),
            GDRIVE_CHUNK_ALIGN,
        )
        .await;

        assert!(
            result.is_err(),
            "tiny progress must exhaust a finite budget"
        );
        assert_eq!(
            script.requests().len(),
            9,
            "attempt ten must fail before dispatch"
        );
    }

    #[tokio::test]
    async fn host_attachment_after_shutdown_never_creates_a_session() {
        let script = ScriptedDispatch::new([]);
        let client = Arc::new(scripted_client(&script));
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();
        assert!(
            host_attachment(
                client,
                "user@example.com".into(),
                Bytes::from_static(b"payload"),
                upload_meta(),
                shutdown,
            )
            .await
            .is_err()
        );
        assert!(script.requests().is_empty());
    }

    #[tokio::test]
    async fn shutdown_during_upload_awaits_session_cleanup() {
        let script = ScriptedDispatch::new([session_created(), Canned::Pending, cancel_accepted()]);
        let client = scripted_client(&script);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let mut upload = Box::pin(run_with_shutdown(
            &client,
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
            shutdown.clone(),
        ));
        for _ in 0..16 {
            assert!(futures::future::poll_immediate(&mut upload).await.is_none());
            if script.requests().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(script.requests().len(), 2);
        shutdown.cancel();
        assert!(upload.await.is_err());
        let requests = script.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].method, reqwest::Method::DELETE);
        assert_eq!(script.remaining(), 0);
    }

    #[tokio::test]
    async fn shutdown_during_session_creation_still_obtains_uri_and_cleans_up() {
        let script = ScriptedDispatch::yielding([session_created(), cancel_accepted()]);
        let client = scripted_client(&script);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let mut upload = Box::pin(run_with_shutdown(
            &client,
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
            shutdown.clone(),
        ));
        for _ in 0..16 {
            assert!(futures::future::poll_immediate(&mut upload).await.is_none());
            if !script.requests().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            script.requests().len(),
            1,
            "session POST is still awaiting its response"
        );
        shutdown.cancel();
        assert!(upload.await.is_err());
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, reqwest::Method::POST);
        assert_eq!(requests[1].method, reqwest::Method::DELETE);
    }

    /// A `Net` failure mid-upload must clean up the resumable session.
    #[tokio::test]
    async fn net_failure_mid_upload_cancels_the_resumable_session() {
        let script = ScriptedDispatch::new([
            session_created(),
            network_failure(),
            Canned::Response {
                status: reqwest::StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
        ]);
        let client = scripted_client(&script);

        let error = run(
            &client,
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("a dropped connection mid-chunk fails the upload");

        let requests = script.requests();
        assert_eq!(
            requests.len(),
            3,
            "session create, the failing chunk PUT, and the cancel DELETE"
        );
        assert_eq!(
            requests[2].method,
            reqwest::Method::DELETE,
            "the session is cancelled with a DELETE"
        );
        assert_eq!(
            requests[2].url.as_str(),
            "https://www.googleapis.com/upload/drive/v3/files/session/abc",
            "the cancel targets the session URI, not the create endpoint"
        );
        assert_eq!(script.remaining(), 0, "the whole script was consumed");
        assert!(
            !requests[2]
                .headers
                .contains_key(reqwest::header::AUTHORIZATION),
            "the session URI is pre-authenticated, like the chunk PUT"
        );

        let consented = error.support_consented();
        assert!(
            !consented.support_text.contains(&ABANDONED_SESSION_TEXT),
            "a session that was cancelled must not be reported as abandoned"
        );
    }

    /// The cancel is best-effort. When it fails too, the original error is
    /// still what the caller gets - decorated so a support export can tell a
    /// cleaned-up failure from one that left a partial upload behind.
    #[tokio::test]
    async fn a_failed_cancel_is_recorded_as_an_abandoned_session() {
        let script =
            ScriptedDispatch::new([session_created(), network_failure(), network_failure()]);
        let client = scripted_client(&script);

        let error = run(
            &client,
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("the upload still failed");

        assert_eq!(script.requests().len(), 3, "the cancel was attempted once");
        let consented = error.support_consented();
        assert!(
            consented.support_text.contains(&ABANDONED_SESSION_TEXT),
            "an uncancellable session must be recorded, got {:?}",
            consented.support_text
        );
        assert!(
            !consented
                .support_text
                .iter()
                .any(|text| text.contains("upload.test/session")),
            "the pre-authenticated session URI must not reach a support export"
        );
    }

    /// Drive answers an accepted cancel with 499, which bifrost-net's retry
    /// loop surfaces as `Err(Status)` and never as `Ok(Response)`. Reading that
    /// as a failed cancel would report every successful cleanup as abandoned.
    #[tokio::test]
    async fn a_499_cancel_response_counts_as_a_cancelled_session() {
        let script = ScriptedDispatch::new([
            session_created(),
            network_failure(),
            Canned::Response {
                status: reqwest::StatusCode::from_u16(499).expect("valid status"),
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
        ]);
        let client = scripted_client(&script);

        let error = run(
            &client,
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("the upload still failed");

        assert!(
            !error
                .support_consented()
                .support_text
                .contains(&ABANDONED_SESSION_TEXT),
            "499 Client Closed Request is Drive accepting the cancel"
        );
    }

    /// The cancel must not touch the recovery classification. A transport drop
    /// on a non-idempotent upload reconciles; adding cleanup text to it must
    /// leave that answer exactly where it was.
    #[tokio::test]
    async fn cleanup_does_not_change_the_recovery_class() {
        let cancelled = ScriptedDispatch::new([
            session_created(),
            network_failure(),
            Canned::Response {
                status: reqwest::StatusCode::OK,
                headers: reqwest::header::HeaderMap::new(),
                body: Bytes::new(),
            },
        ]);
        let abandoned =
            ScriptedDispatch::new([session_created(), network_failure(), network_failure()]);

        let a = run(
            &scripted_client(&cancelled),
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("upload failed");
        let b = run(
            &scripted_client(&abandoned),
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("upload failed");

        assert_eq!(a.kind(), b.kind(), "decoration must not reclassify");
        assert_eq!(
            a.message_key(),
            b.message_key(),
            "decoration must not move the telemetry key"
        );
        assert_eq!(
            format!("{:?}", a.recovery()),
            format!("{:?}", b.recovery()),
            "decoration must not change what the engine does next"
        );
    }

    // -----------------------------------------------------------------
    // Classification: none of the upload refusals below is the caller's
    // fault. Each was `Request(Malformed)` (`ClientBug`, "fix the client
    // request") before, with no transmission evidence, so it read `Unsent`
    // although a complete response had arrived.
    // -----------------------------------------------------------------

    fn attempt_state(error: &AccountError) -> Option<bifrost_types::TransmissionState> {
        error.chain().iter().find_map(|cause| match cause {
            bifrost_types::Cause::Attempt(attempt) => Some(attempt.transmission_state),
            _ => None,
        })
    }

    fn resume_with(range: Option<&str>) -> Canned {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(range) = range {
            headers.insert("range", range.parse().expect("valid Range"));
        }
        Canned::Response {
            status: reqwest::StatusCode::PERMANENT_REDIRECT,
            headers,
            body: Bytes::new(),
        }
    }

    fn cancel_accepted() -> Canned {
        Canned::Response {
            status: reqwest::StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::new(),
        }
    }

    async fn failed_upload(script: &Arc<ScriptedDispatch>) -> AccountError {
        run(
            &scripted_client(script),
            "user@example.com",
            Bytes::from_static(b"payload"),
            upload_meta(),
        )
        .await
        .expect_err("the upload fails")
    }

    fn assert_provider_fault(error: &AccountError, kind: bifrost_types::ProtocolErrorKind) {
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(kind),
            "a broken provider response is the provider's fault"
        );
        assert_eq!(
            attempt_state(error),
            Some(bifrost_types::TransmissionState::Acknowledged),
            "a complete response arrived"
        );
    }

    fn assert_client_limit(error: &AccountError) {
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Internal(
                bifrost_types::InternalErrorKind::LimitExceeded
            ),
            "declining to go further is this crate's own limit"
        );
        assert_eq!(
            attempt_state(error),
            Some(bifrost_types::TransmissionState::Acknowledged),
            "every request sent had been answered"
        );
    }

    /// A 2xx session response without `Location` leaves nothing to upload
    /// to: Drive broke the protocol. There is no session URI, so no cancel.
    #[tokio::test]
    async fn a_session_response_without_location_is_a_provider_missing_field() {
        let script = ScriptedDispatch::new([cancel_accepted()]);
        let error = failed_upload(&script).await;

        assert_provider_fault(&error, bifrost_types::ProtocolErrorKind::MissingField);
        assert_eq!(script.requests().len(), 1, "only the session POST was sent");
    }

    /// A `Location` that is present but not visible-ASCII text did not
    /// parse; it was not missing.
    #[tokio::test]
    async fn a_session_location_that_is_not_text_is_a_provider_parse_failure() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "location",
            reqwest::header::HeaderValue::from_bytes(
                b"https://www.googleapis.com/upload/drive/v3/files/session/\xff",
            )
            .expect("obs-text is a legal header value"),
        );
        let script = ScriptedDispatch::new([Canned::Response {
            status: reqwest::StatusCode::OK,
            headers,
            body: Bytes::new(),
        }]);
        let error = failed_upload(&script).await;

        assert_provider_fault(&error, bifrost_types::ProtocolErrorKind::ParseFailed);
        assert_eq!(script.requests().len(), 1, "only the session POST was sent");
    }

    /// A `Location` that is text but not an absolute http(s) URL with a host
    /// did not parse either. Accepted, it sent every chunk PUT to a bogus URL.
    #[tokio::test]
    async fn a_session_location_that_is_not_an_absolute_http_url_is_a_provider_parse_failure() {
        for location in [
            "",
            "/upload/session/abc",
            "ftp://upload.test/session",
            "file:///session",
        ] {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                "location",
                reqwest::header::HeaderValue::from_str(location).expect("valid header value"),
            );
            let script = ScriptedDispatch::new([
                Canned::Response {
                    status: reqwest::StatusCode::OK,
                    headers,
                    body: Bytes::new(),
                },
                cancel_accepted(),
                cancel_accepted(),
            ]);
            let error = failed_upload(&script).await;

            assert_eq!(
                error.kind(),
                &bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::ParseFailed
                ),
                "Location {location:?} must be refused as unparseable"
            );
            assert_eq!(
                attempt_state(&error),
                Some(bifrost_types::TransmissionState::Acknowledged),
                "a complete response arrived for {location:?}"
            );
            assert_eq!(
                script.requests().len(),
                1,
                "only the session POST was sent for {location:?}"
            );
        }
    }

    fn redirect_to_elsewhere(status: reqwest::StatusCode) -> Canned {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "location",
            "https://elsewhere.test/steal?token=secret"
                .parse()
                .expect("valid URL"),
        );
        Canned::Response {
            status,
            headers,
            body: Bytes::new(),
        }
    }

    /// A chunk PUT answered by a real redirect must not carry the payload to
    /// the `Location`: the session URI is pre-authenticated and admitted only
    /// as itself. The redirect is Drive breaking the resumable protocol, and
    /// the error must not echo the server-chosen `Location`.
    #[tokio::test]
    async fn a_redirect_on_a_chunk_put_is_not_followed_and_is_a_contract_violation() {
        for status in [
            reqwest::StatusCode::TEMPORARY_REDIRECT,
            reqwest::StatusCode::PERMANENT_REDIRECT,
        ] {
            let script = ScriptedDispatch::new([
                session_created(),
                redirect_to_elsewhere(status),
                cancel_accepted(),
                cancel_accepted(),
            ]);
            let error = failed_upload(&script).await;

            let requests = script.requests();
            assert!(
                requests
                    .iter()
                    .all(|request| request.url.host_str() != Some("elsewhere.test")),
                "a {status} Location must never be requested"
            );
            assert_provider_fault(&error, bifrost_types::ProtocolErrorKind::ContractViolation);
            assert_eq!(requests.len(), 3, "session, the redirected PUT, cancel");
            assert_eq!(requests[2].method, reqwest::Method::DELETE);
            assert_eq!(requests[2].url.host_str(), Some("www.googleapis.com"));
            let consented = error.support_consented();
            assert!(
                !consented
                    .support_text
                    .iter()
                    .any(|text| text.contains("elsewhere.test") || text.contains("secret")),
                "the Location must not be echoed, got {:?}",
                consented.support_text
            );
        }
    }

    /// A cancel DELETE answered by a redirect is not followed, and is not
    /// Drive accepting the cancel: the session is reported abandoned.
    #[tokio::test]
    async fn a_redirect_on_the_cancel_is_not_followed_and_abandons_the_session() {
        let script = ScriptedDispatch::new([
            session_created(),
            network_failure(),
            redirect_to_elsewhere(reqwest::StatusCode::TEMPORARY_REDIRECT),
            cancel_accepted(),
        ]);
        let error = failed_upload(&script).await;

        let requests = script.requests();
        assert_eq!(requests.len(), 3, "session, the failing PUT, one cancel");
        assert!(
            requests
                .iter()
                .all(|request| request.url.host_str() != Some("elsewhere.test")),
            "the cancel's Location must never be requested"
        );
        assert!(
            error
                .support_consented()
                .support_text
                .contains(&ABANDONED_SESSION_TEXT),
            "a redirected cancel did not cancel anything"
        );
    }

    /// The account email comes from Gmail's profile, not the caller, so an
    /// address with no domain is the provider's value failing to parse. It
    /// must be refused before anything is sent: refused after the upload, it
    /// left an uploaded file that nothing shares.
    #[tokio::test]
    async fn an_account_email_without_a_domain_is_refused_before_the_upload() {
        let script = ScriptedDispatch::new(Vec::<Canned>::new());
        let error = run(
            &scripted_client(&script),
            "no-domain",
            Bytes::from_static(b"payload"),
            CloudUploadMeta::new("report.pdf", "application/pdf", 7, ShareScope::Organization),
        )
        .await
        .expect_err("domain sharing needs a domain");

        assert!(script.requests().is_empty(), "nothing was uploaded");
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ParseFailed
            ),
            "the profile's value is the provider's, not the caller's input"
        );
        assert_eq!(
            attempt_state(&error),
            Some(bifrost_types::TransmissionState::Unsent),
            "this exchange never reached the wire"
        );
    }

    #[tokio::test]
    async fn an_unparseable_308_range_is_a_provider_parse_failure() {
        let script = ScriptedDispatch::new([
            session_created(),
            resume_with(Some("bytes=garbage")),
            cancel_accepted(),
        ]);
        let error = failed_upload(&script).await;

        assert_provider_fault(&error, bifrost_types::ProtocolErrorKind::ParseFailed);
        assert_eq!(script.remaining(), 0, "the session was still cancelled");
    }

    /// Seven bytes were sent; Drive claims ten.
    #[tokio::test]
    async fn a_308_claiming_bytes_never_sent_is_a_contract_violation() {
        let script = ScriptedDispatch::new([
            session_created(),
            resume_with(Some("bytes=0-9")),
            cancel_accepted(),
        ]);
        let error = failed_upload(&script).await;

        assert_provider_fault(&error, bifrost_types::ProtocolErrorKind::ContractViolation);
    }

    /// Drive reported three bytes, then answered the next chunk with no
    /// `Range` (documented as "no bytes received"): it forgot bytes it had
    /// reported. The loop must neither resume past the gap nor blame the
    /// caller.
    #[tokio::test]
    async fn an_absent_range_after_progress_is_refused_without_skipping_a_gap() {
        let script = ScriptedDispatch::new([
            session_created(),
            resume_with(Some("bytes=0-2")),
            resume_with(None),
            cancel_accepted(),
        ]);
        let error = failed_upload(&script).await;

        assert_provider_fault(&error, bifrost_types::ProtocolErrorKind::ContractViolation);
        let requests = script.requests();
        assert_eq!(requests.len(), 4, "session, two chunk PUTs, cancel");
        assert_eq!(
            requests[2]
                .headers
                .get("content-range")
                .and_then(|value| value.to_str().ok()),
            Some("bytes 3-6/7"),
            "the second chunk resumed exactly where Drive said it stood"
        );
        assert_eq!(requests[3].method, reqwest::Method::DELETE);
    }

    /// A 308 reporting every byte is a 308 that should have been a 200/201.
    /// This used to fall out of the loop as "completed without a file
    /// response" and be blamed on the caller.
    #[tokio::test]
    async fn a_308_reporting_every_byte_without_completing_is_a_contract_violation() {
        let script = ScriptedDispatch::new([
            session_created(),
            resume_with(Some("bytes=0-6")),
            cancel_accepted(),
        ]);
        let error = failed_upload(&script).await;

        assert_provider_fault(&error, bifrost_types::ProtocolErrorKind::ContractViolation);
        assert_eq!(script.remaining(), 0, "the session was cancelled");
    }

    /// No progress on a chunk is legal (Drive documents resending from its
    /// offset); refusing to resend is this crate's decision.
    #[tokio::test]
    async fn a_chunk_without_progress_is_refused_as_a_client_limit() {
        let script =
            ScriptedDispatch::new([session_created(), resume_with(None), cancel_accepted()]);
        let error = failed_upload(&script).await;

        assert_client_limit(&error);
    }

    #[tokio::test]
    async fn an_exhausted_chunk_budget_is_a_client_limit() {
        let responses = (0..9).map(resume_response).collect::<Vec<_>>();
        let script = ScriptedDispatch::new(responses);

        let error = upload_file_chunked(
            &scripted_client(&script),
            "https://www.googleapis.com/upload/drive/v3/files/session",
            Bytes::from_static(b"a payload longer than nine bytes"),
            GDRIVE_CHUNK_ALIGN,
        )
        .await
        .expect_err("tiny progress exhausts the budget");
        let error = into_account_error(error, ctx());

        assert_client_limit(&error);
        assert_eq!(
            error.recovery(),
            &bifrost_types::RecoveryClass::InternalFailure,
            "a client limit is terminal, not a retry"
        );
    }

    /// An empty payload is the caller's input, and it is refused before any
    /// byte is sent. It used to open a session first, refuse inside the chunk
    /// loop, and then cancel the session it never needed.
    #[tokio::test]
    async fn an_empty_payload_is_refused_before_a_session_opens() {
        let script = ScriptedDispatch::new(Vec::<Canned>::new());
        let error = run(
            &scripted_client(&script),
            "user@example.com",
            Bytes::new(),
            CloudUploadMeta::new(
                "empty.bin",
                "application/octet-stream",
                0,
                ShareScope::Anyone,
            ),
        )
        .await
        .expect_err("an empty upload is refused");

        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert!(script.requests().is_empty(), "nothing was sent");
    }
}
