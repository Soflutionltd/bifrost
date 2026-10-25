use bifrost_types::{
    AccountError, AccountFuture, AccountOperation, FilterDiagnostic, FilterDiagnosticSeverity,
    FilterScript, FilterValidation, ScriptLanguage, ServerFilter, ServerFilterCreate,
    ServerFilterId, ServerFilterPatch,
};
use futures::StreamExt;

use crate::blob::BlobRef;
use crate::core::SetCreate;
use crate::core::id::BlobId;
use crate::core::set::SetError;
use crate::core::transport::HttpTransport;
use crate::sieve::validate::SieveScriptValidateRequest;
use crate::sieve::{
    Property as SieveProperty, SieveScriptGet, SieveScriptId, SieveScriptQuery, SieveScriptSet,
};

type SieveAccount<T> = crate::account::Account<T>;

const SIEVE_MIME: &str = "application/sieve";
const SIEVE_DOWNLOAD_NAME: &str = "filter.sieve";

#[inline]
fn to_acct_err(op: AccountOperation) -> impl Fn(crate::Error) -> AccountError {
    move |err| super::error::into_account_error(err, super::error::JmapErrorContext::new(op))
}

fn unsupported(op: AccountOperation, detail: &'static str) -> AccountError {
    super::error::unsupported_error(op, None, detail)
}

fn require_sieve<T: HttpTransport>(
    account: Option<SieveAccount<T>>,
    op: AccountOperation,
) -> Result<SieveAccount<T>, AccountError> {
    account.ok_or_else(|| unsupported(op, "JMAP Sieve capability not available"))
}

// Production requests, including the per-filter script-body downloads, are
// metered and bandwidth-limited by AccountNet independently of ByteTally.
// This passthrough returns values directly rather than emitting scope-stream
// batches, so it carries no Batch.bytes_in attribution - no passthrough door
// on any provider does. Revisit if a consumer needs per-call byte attribution
// for passthrough operations, or a production transport bypasses AccountNet
// without equivalent accounting and bandwidth admission.
pub(crate) fn list<T: HttpTransport>(
    sieve: Option<SieveAccount<T>>,
) -> AccountFuture<Result<Vec<ServerFilter>, AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FiltersList)?;
        let ids = account
            .call(SieveScriptQuery::new())
            .await
            .map_err(to_acct_err(AccountOperation::FiltersList))?
            .into_ids();
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // The query's id list is unbounded - it is whatever the account
        // holds - so hydrating it in ONE `SieveScript/get` puts a call on
        // the wire that a server may refuse wholesale with
        // `requestTooLarge` (RFC 8620 s5.1). Batch at the advertised
        // `maxObjectsInGet`.
        let batch = super::factory::max_objects_in_get(&account.client().session());
        let mut listed = Vec::new();
        let mut not_found: Vec<String> = Vec::new();
        for chunk in ids.chunks(batch) {
            let response = account
                .call(SieveScriptGet::new().ids(chunk.to_vec()).properties([
                    SieveProperty::Id,
                    SieveProperty::Name,
                    SieveProperty::BlobId,
                    SieveProperty::IsActive,
                ]))
                .await
                .map_err(to_acct_err(AccountOperation::FiltersList))?;
            not_found.extend(response.not_found().iter().map(ToString::to_string));
            listed.extend(response.into_list());
        }

        // Reconcile the answer against the SUBMITTED ids, the way
        // `hydrate::reconcile_hydration` and `contacts::get_cards` do.
        // Without this a script the server omits from `list` just
        // disappears from the returned Vec, and the consumer cannot tell
        // that from "the filter was deleted" - `notFound` decodes as
        // empty when absent (see the `/get` leniency rule), so absence
        // proves nothing on its own. This door has no per-item lane, so
        // the whole call fails, retryably.
        for id in &ids {
            let id = id.as_str();
            if listed
                .iter()
                .any(|script| script.id().is_some_and(|got| got.as_str() == id))
            {
                continue;
            }
            return Err(super::error::get_id_unresolved_after_query(
                id,
                not_found.iter().any(|gone| gone == id),
                super::error::JmapErrorContext::new(AccountOperation::FiltersList),
            ));
        }

        // One `SieveScript/get` describes every script, but each body
        // lives behind its own blob download. Serially that is N further
        // round trips, all of them independent. They run concurrently,
        // bounded by the SAME `maxConcurrentRequests` clamp the open-time
        // foreign probes use (`factory::api_request_concurrency`), so the
        // crate has one answer to how wide it may fan out rather than a
        // second bound that could drift from the session.
        //
        // `buffered` (not `buffer_unordered`) keeps completion order equal
        // to submission order, which is what preserves the error
        // accounting unchanged: the first failing download in SCRIPT order
        // is still the error the whole call returns, exactly as the serial
        // loop's `?` produced.
        let scripts = listed
            .into_iter()
            .map(|mut script| {
                (
                    script.take_id().into_string(),
                    script.name().map(str::to_owned),
                    script.is_active(),
                    script.blob_id().cloned(),
                )
            })
            .collect::<Vec<_>>();
        let concurrency = super::factory::api_request_concurrency(&account.client().session());
        let blob_ids = scripts
            .iter()
            .map(|(_, _, _, blob_id)| blob_id.clone())
            .collect::<Vec<_>>();
        let account_ref = &account;
        let bodies: Vec<Result<String, AccountError>> =
            futures::stream::iter(blob_ids.into_iter().map(move |blob_id| async move {
                match blob_id {
                    Some(blob_id) => {
                        download_script_body(account_ref, &blob_id, AccountOperation::FiltersList)
                            .await
                    }
                    None => Ok(String::new()),
                }
            }))
            .buffered(concurrency)
            .collect()
            .await;

        let mut filters = Vec::new();
        for ((id, name, is_active, _), body) in scripts.into_iter().zip(bodies) {
            filters.push(ServerFilter::Script(FilterScript {
                id: ServerFilterId(id),
                name,
                language: ScriptLanguage::Sieve,
                body: body?,
                is_active,
            }));
        }
        Ok(filters)
    })
}

pub(crate) fn create<T: HttpTransport>(
    sieve: Option<SieveAccount<T>>,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<ServerFilterId, AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FilterCreate)?;
        let ServerFilterCreate::Script(script) = filter else {
            return Err(unsupported(
                AccountOperation::FilterCreate,
                "JMAP Sieve only supports literal script filters",
            ));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Err(unsupported(
                AccountOperation::FilterCreate,
                "JMAP Sieve only supports Sieve scripts",
            ));
        }

        let blob_id = upload_script_body(&account, script.body, AccountOperation::FilterCreate)
            .await?
            .into_string();
        let mut create = crate::sieve::SieveScriptCreate::new(None);
        create.blob_id(BlobId::new(blob_id));
        if let Some(name) = script.name {
            create.name(name);
        }

        let mut set = SieveScriptSet::new();
        let create_id = set.create_item(create);
        if script.is_active {
            set = set.on_success_activate_script(create_id.clone());
        }

        let mut response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        let mut created = response
            .created(&create_id)
            .map_err(to_acct_err(AccountOperation::FilterCreate))?;
        Ok(ServerFilterId(created.take_id().into_string()))
    })
}

pub(crate) fn update<T: HttpTransport>(
    sieve: Option<SieveAccount<T>>,
    filter: ServerFilterId,
    patch: ServerFilterPatch,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FilterUpdate)?;
        let ServerFilterPatch::Script(patch) = patch else {
            return Err(unsupported(
                AccountOperation::FilterUpdate,
                "JMAP Sieve only supports literal script filters",
            ));
        };

        let id = SieveScriptId::new(filter.0);
        let mut set = SieveScriptSet::new();
        let mut script_patch = crate::sieve::SieveScriptPatch::default();
        let mut has_patch = false;

        if let Some(name) = patch.name {
            // A clear (Some(None)) collapses to an empty name: Sieve
            // script names are not nullable, so the empty string is the
            // closest representation of "unset".
            script_patch.name(name.unwrap_or_default());
            has_patch = true;
        }
        if let Some(body) = patch.body {
            let blob_id = upload_script_body(&account, body, AccountOperation::FilterUpdate)
                .await?
                .into_string();
            script_patch.blob_id(BlobId::new(blob_id));
            has_patch = true;
        }

        if has_patch {
            set.update_item(id.clone(), script_patch);
        }
        if let Some(is_active) = patch.is_active {
            if is_active {
                set = set.on_success_activate_script_id(id.clone());
            } else {
                // Sieve activation is global (at most one active script),
                // so deactivation targets "no active script" rather than
                // this id specifically.
                set = set.on_success_deactivate_script(true);
            }
        }
        if !has_patch && patch.is_active.is_none() {
            return Ok(());
        }

        let mut response = account
            .call(set)
            .await
            .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        if has_patch {
            response
                .updated(&id)
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        } else {
            response
                .unwrap_update_errors()
                .map_err(to_acct_err(AccountOperation::FilterUpdate))?;
        }
        Ok(())
    })
}

pub(crate) fn delete<T: HttpTransport>(
    sieve: Option<SieveAccount<T>>,
    filter: ServerFilterId,
) -> AccountFuture<Result<(), AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FilterDelete)?;
        let id = SieveScriptId::new(filter.0);
        let mut response = account
            .call(SieveScriptSet::new().destroy([id.clone()]))
            .await
            .map_err(to_acct_err(AccountOperation::FilterDelete))?;
        response
            .destroyed(&id)
            .map_err(to_acct_err(AccountOperation::FilterDelete))
    })
}

pub(crate) fn validate<T: HttpTransport>(
    sieve: Option<SieveAccount<T>>,
    filter: ServerFilterCreate,
) -> AccountFuture<Result<FilterValidation, AccountError>> {
    Box::pin(async move {
        let account = require_sieve(sieve, AccountOperation::FilterValidate)?;
        let ServerFilterCreate::Script(script) = filter else {
            return Err(unsupported(
                AccountOperation::FilterValidate,
                "JMAP Sieve only validates literal script filters",
            ));
        };
        if !matches!(script.language, ScriptLanguage::Sieve) {
            return Err(unsupported(
                AccountOperation::FilterValidate,
                "JMAP Sieve only validates Sieve scripts",
            ));
        }

        // SieveScript/validate takes a blob, so the body is uploaded
        // first. The blob is never referenced by a stored script, so the
        // server garbage-collects it.
        let blob_id = upload_script_body(&account, script.body, AccountOperation::FilterValidate)
            .await?
            .into_string();
        let response = account
            .call(SieveScriptValidateRequest::new(BlobId::new(blob_id)))
            .await
            .map_err(to_acct_err(AccountOperation::FilterValidate))?;
        Ok(validation_from_error(response.into_error()))
    })
}

fn validation_from_error(error: Option<SetError<String>>) -> FilterValidation {
    match error {
        None => FilterValidation::default(),
        Some(error) => FilterValidation {
            diagnostics: vec![FilterDiagnostic {
                severity: FilterDiagnosticSeverity::Error,
                message: error.to_string(),
                line: None,
                column: None,
            }],
        },
    }
}

async fn upload_script_body<T: HttpTransport>(
    account: &SieveAccount<T>,
    body: String,
    op: AccountOperation,
) -> Result<BlobId, AccountError> {
    let blob = account
        .upload(body.into_bytes(), Some(SIEVE_MIME))
        .await
        .map_err(to_acct_err(op))?;
    Ok(blob.blob_id)
}

async fn download_script_body<T: HttpTransport>(
    account: &SieveAccount<T>,
    blob_id: &BlobId,
    op: AccountOperation,
) -> Result<String, AccountError> {
    let blob = BlobRef::new(account.id().clone(), blob_id.clone())
        .with_name(SIEVE_DOWNLOAD_NAME)
        .with_content_type(SIEVE_MIME);
    let bytes = account
        .client()
        .download(&blob)
        .await
        .map_err(to_acct_err(op))?;
    String::from_utf8(bytes.to_vec()).map_err(|err| {
        to_acct_err(op)(crate::Error::NotParsable(format!(
            "Sieve script body is not UTF-8: {err}"
        )))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bifrost_types::{AccountErrorKind, ProtocolErrorKind};

    use super::*;
    use crate::core::transport::TransportError;

    /// A JMAP boundary that answers a scripted API sequence and serves
    /// blob downloads keyed by the blob id in the URL, while recording
    /// the high-water mark of downloads in flight at once.
    #[derive(Default)]
    struct DownloadMeter {
        in_flight: AtomicUsize,
        peak: AtomicUsize,
    }

    type Recorder = std::sync::Arc<StdMutex<Vec<serde_json::Value>>>;

    struct BlobTransport {
        api: StdMutex<VecDeque<String>>,
        meter: std::sync::Arc<DownloadMeter>,
        requests: Recorder,
    }

    impl BlobTransport {
        fn new(
            api: impl IntoIterator<Item = String>,
            meter: std::sync::Arc<DownloadMeter>,
            requests: Recorder,
        ) -> Self {
            Self {
                api: StdMutex::new(api.into_iter().collect()),
                meter,
                requests,
            }
        }
    }

    impl HttpTransport for BlobTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, TransportError> {
            self.requests
                .lock()
                .expect("recorded requests")
                .push(serde_json::from_slice(&body).expect("the client emits JSON"));
            match self.api.lock().expect("script").pop_front() {
                Some(reply) => Ok(bytes::Bytes::from(reply)),
                None => Err(TransportError::new("api script exhausted")),
            }
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, TransportError> {
            Err(TransportError::new("no upload reply"))
        }

        async fn download(&self, url: &str) -> Result<bytes::Bytes, TransportError> {
            // Answered without ever yielding, so its failure ARRIVES
            // before any download that parks below. Ordering that is
            // arrival-driven rather than submission-driven reports this
            // one; the ordering test exists to catch exactly that.
            if url.contains("/b-broken/") {
                return Err(TransportError::new("blob unavailable"));
            }

            let now = self.meter.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.meter.peak.fetch_max(now, Ordering::SeqCst);
            // Under `start_paused` the runtime auto-advances, so this is
            // a scheduling point rather than wall-clock time: every
            // download that MAY overlap is parked here at once, and the
            // peak counter above records how many that was.
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.meter.in_flight.fetch_sub(1, Ordering::SeqCst);

            if url.contains("/b-badutf8/") {
                return Ok(bytes::Bytes::from_static(&[0xff, 0xfe]));
            }
            Ok(bytes::Bytes::from(format!("# body for {url}")))
        }

        async fn get_session(&self, _url: &str) -> Result<bytes::Bytes, TransportError> {
            Err(TransportError::new("no session reply"))
        }
    }

    fn sieve_session() -> crate::core::session::Session {
        sieve_session_with_get_limit(256)
    }

    fn sieve_session_with_get_limit(max_objects_in_get: usize) -> crate::core::session::Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": max_objects_in_get,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:sieve": {}
            },
            "accounts": {"primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false, "accountCapabilities": {"urn:ietf:params:jmap:sieve": {}}}},
            "primaryAccounts": {"urn:ietf:params:jmap:sieve": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses")
    }

    fn query_reply(ids: &[&str]) -> String {
        serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[
                "SieveScript/query",
                {
                    "accountId": "primary",
                    "queryState": "q1",
                    "canCalculateChanges": false,
                    "position": 0,
                    "ids": ids
                },
                "s0"
            ]]
        })
        .to_string()
    }

    fn get_reply(scripts: &[(&str, &str)]) -> String {
        let list: Vec<_> = scripts
            .iter()
            .map(|(id, blob_id)| {
                serde_json::json!({
                    "id": id,
                    "name": id,
                    "blobId": blob_id,
                    "isActive": false
                })
            })
            .collect();
        serde_json::json!({
            "sessionState": "session-1",
            "methodResponses": [[
                "SieveScript/get",
                {"accountId": "primary", "state": "s1", "list": list, "notFound": []},
                "s0"
            ]]
        })
        .to_string()
    }

    fn sieve_account(
        api: impl IntoIterator<Item = String>,
        meter: &std::sync::Arc<DownloadMeter>,
    ) -> SieveAccount<BlobTransport> {
        sieve_account_with_session(api, meter, sieve_session(), &Recorder::default())
    }

    fn sieve_account_with_session(
        api: impl IntoIterator<Item = String>,
        meter: &std::sync::Arc<DownloadMeter>,
        session: crate::core::session::Session,
        requests: &Recorder,
    ) -> SieveAccount<BlobTransport> {
        let client = crate::client::Client::with_transport(
            BlobTransport::new(
                api,
                std::sync::Arc::clone(meter),
                std::sync::Arc::clone(requests),
            ),
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        crate::account::Account::new(client, "primary")
    }

    /// `filters_list` used to cost N+2 serial round trips: one query, one
    /// get, then a blob download per script awaited one at a time. The
    /// downloads are independent, so they run concurrently under the same
    /// `maxConcurrentRequests` clamp the open-time foreign probes use -
    /// here 4, so six scripts must show four downloads in flight at once
    /// and never a fifth.
    #[tokio::test(start_paused = true)]
    async fn script_bodies_download_concurrently_within_the_advertised_limit() {
        let meter = std::sync::Arc::new(DownloadMeter::default());
        let ids: Vec<&str> = vec!["s1", "s2", "s3", "s4", "s5", "s6"];
        let scripts: Vec<(&str, &str)> = ids.iter().map(|id| (*id, "b-ok")).collect();
        let account = sieve_account([query_reply(&ids), get_reply(&scripts)], &meter);

        let filters = list(Some(account)).await.expect("filters list");
        assert_eq!(filters.len(), 6);
        assert_eq!(
            meter.peak.load(Ordering::SeqCst),
            4,
            "the downloads must overlap, bounded by maxConcurrentRequests"
        );
    }

    /// The concurrency must not change WHICH failure the call reports.
    /// The serial loop returned the first failing script in list order;
    /// `buffered` preserves that by yielding in submission order, so a
    /// bad-UTF-8 body at position two still wins over a dead download at
    /// position three even though both fail in the same wave.
    #[tokio::test(start_paused = true)]
    async fn the_first_failing_script_in_order_is_the_reported_error() {
        let meter = std::sync::Arc::new(DownloadMeter::default());
        let ids = vec!["s1", "s2", "s3"];
        let account = sieve_account(
            [
                query_reply(&ids),
                get_reply(&[("s1", "b-ok"), ("s2", "b-badutf8"), ("s3", "b-broken")]),
            ],
            &meter,
        );

        let err = list(Some(account)).await.expect_err("a failing download");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Protocol(ProtocolErrorKind::ParseFailed),
            "the undecodable body at position two is the reported failure, \
             not the transport failure behind it"
        );
    }

    /// `SieveScript/query` returns however many scripts the account
    /// holds, so hydrating that list in ONE `SieveScript/get` builds a
    /// call a server may refuse outright on its own `maxObjectsInGet`.
    /// Five ids under a limit of two are three gets.
    #[tokio::test(start_paused = true)]
    async fn the_script_hydration_batches_at_max_objects_in_get() {
        let meter = std::sync::Arc::new(DownloadMeter::default());
        let requests = Recorder::default();
        let ids = vec!["s1", "s2", "s3", "s4", "s5"];
        let account = sieve_account_with_session(
            [
                query_reply(&ids),
                get_reply(&[("s1", "b-ok"), ("s2", "b-ok")]),
                get_reply(&[("s3", "b-ok"), ("s4", "b-ok")]),
                get_reply(&[("s5", "b-ok")]),
            ],
            &meter,
            sieve_session_with_get_limit(2),
            &requests,
        );

        let filters = list(Some(account)).await.expect("filters list");
        assert_eq!(filters.len(), 5);

        let gets: Vec<Vec<String>> = requests
            .lock()
            .expect("recorded requests")
            .iter()
            .filter_map(|request| {
                let call = request.get("methodCalls")?.get(0)?;
                (call.get(0)?.as_str()? == "SieveScript/get").then(|| {
                    call.get(1)
                        .and_then(|args| args.get("ids"))
                        .and_then(serde_json::Value::as_array)
                        .expect("a SieveScript/get carries ids")
                        .iter()
                        .map(|id| id.as_str().expect("string id").to_string())
                        .collect()
                })
            })
            .collect();
        assert_eq!(
            gets,
            vec![
                vec!["s1".to_string(), "s2".to_string()],
                vec!["s3".to_string(), "s4".to_string()],
                vec!["s5".to_string()],
            ],
        );
    }

    /// A script the `/get` leaves out of `list` used to vanish from the
    /// returned Vec, which reads to the consumer as "that filter does not
    /// exist" - a deletion the response never claimed. `notFound` cannot
    /// carry the distinction either: it decodes as empty when absent. The
    /// answer is reconciled against the submitted ids, and since this
    /// door has no per-item lane the whole call fails, retryably.
    #[tokio::test(start_paused = true)]
    async fn a_script_the_get_never_answers_fails_the_list() {
        for (case, reply) in [
            (
                "silently omitted",
                get_reply(&[("s1", "b-ok")]),
            ),
            (
                "declared notFound",
                serde_json::json!({
                    "sessionState": "session-1",
                    "methodResponses": [[
                        "SieveScript/get",
                        {
                            "accountId": "primary",
                            "state": "s1",
                            "list": [{"id": "s1", "name": "s1", "blobId": "b-ok", "isActive": false}],
                            "notFound": ["s2"]
                        },
                        "s0"
                    ]]
                })
                .to_string(),
            ),
        ] {
            let meter = std::sync::Arc::new(DownloadMeter::default());
            let account = sieve_account([query_reply(&["s1", "s2"]), reply], &meter);

            let err = list(Some(account))
                .await
                .expect_err("an unresolved script must not vanish");
            assert_eq!(
                err.kind(),
                &AccountErrorKind::Protocol(ProtocolErrorKind::PartialResponse),
                "{case}: retryable, not a shorter list and not terminal"
            );
        }
    }

    #[test]
    fn validate_maps_sieve_set_error_to_error_diagnostic() {
        let error = serde_json::from_str::<SetError<String>>(
            r#"{"type":"invalidScript","description":"line 1: bad command"}"#,
        )
        .expect("set error decodes");
        let validation = validation_from_error(Some(error));
        assert!(!validation.is_valid());
        assert_eq!(validation.diagnostics.len(), 1);
        assert_eq!(
            validation.diagnostics[0].severity,
            FilterDiagnosticSeverity::Error
        );
        assert!(
            validation.diagnostics[0]
                .message
                .contains("line 1: bad command")
        );
    }

    #[test]
    fn validate_success_has_no_diagnostics() {
        let validation = validation_from_error(None);
        assert!(validation.is_valid());
        assert!(validation.diagnostics.is_empty());
    }
}
