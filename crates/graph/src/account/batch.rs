//! Shared Graph batch transport semantics.
//!
//! Wire DTOs retain JSON bodies so sparse resources and error envelopes can be
//! classified after correlation. Domain owners retain chunk progress: splitting
//! a write here and returning a later transport error would lose earlier commits.

use bifrost_net::ReplayPolicy;
use bytes::Bytes;
use reqwest::{Method, header::HeaderMap};

use crate::client::{GraphClient, Resource};
use crate::error::GraphError;
use crate::types::{BatchRequest, BatchRequestItem, BatchResponse, BatchResponseItem};

pub(crate) const LIMIT: usize = 20;

/// Absence is successful only for desired-state deletion. Validator eviction
/// is independent: every successful write consumes its observed validator.
#[derive(Clone, Copy)]
pub(crate) enum AbsentMeans {
    Success,
    Failure,
}

/// Typed replay evidence decoded at the submission boundary. Invalid
/// methods or headers fail closed rather than dropping a conditional guard.
struct BatchLeg {
    method: Method,
    headers: HeaderMap,
    replay: ReplayPolicy,
}

impl BatchLeg {
    fn decode(item: &BatchRequestItem) -> Option<Self> {
        let method = Method::from_bytes(item.method.as_bytes()).ok()?;
        let mut headers = HeaderMap::new();
        if let Some(entries) = &item.headers {
            for (name, value) in entries {
                let name = reqwest::header::HeaderName::from_bytes(name.as_bytes()).ok()?;
                let value = reqwest::header::HeaderValue::from_str(value).ok()?;
                // Case aliases must not overwrite a guard in HashMap iteration
                // order. Presence of any precondition prevents replay.
                headers.append(name, value);
            }
        }
        let replay = if method == Method::GET || method == Method::HEAD {
            ReplayPolicy::IdempotentUnlessConditional
        } else {
            ReplayPolicy::Never
        };
        Some(Self {
            method,
            headers,
            replay,
        })
    }

    fn replayable(&self) -> bool {
        self.replay.permits_replay(&self.method, &self.headers)
    }
}

/// Only read legs permit uncertain replay at this generic boundary.
/// An absolute PATCH can overwrite an intervening edit, even without a guard.
/// DELETE may clear a property recreated after the first attempt, and replayed
/// resource deletion can report 404 after success unless its owner absorbs it.
/// Permanent deletion and moves use POST and cannot be blindly repeated either.
/// Outlook users expect one write, with uncertain outcomes reconciled before
/// another write. Even desired-state clears remain conservative here: this
/// boundary has neither their absence policy nor concurrency guarantees.
pub(crate) fn subrequest_replayable(item: &BatchRequestItem) -> bool {
    BatchLeg::decode(item).is_some_and(|leg| leg.replayable())
}

/// An envelope can be replayed only when every leg can be replayed. In
/// particular a conditional PATCH must reconcile after uncertain submission.
/// Outlook users expect a retry never to overwrite their intervening edit.
pub(crate) async fn submit(
    client: &GraphClient,
    resource: Resource,
    request: &BatchRequest,
) -> Result<BatchResponse, GraphError> {
    let replay = if request.requests.iter().all(subrequest_replayable) {
        ReplayPolicy::Idempotent
    } else {
        ReplayPolicy::Never
    };
    client.post_batch(resource, request, replay).await
}

/// Convert the correlated wire answer at one boundary. Status remains numeric
/// until batch_status_error validates it, so an invalid status becomes an
/// individual protocol failure without discarding valid siblings. Headers use
/// the same tolerant observation policy as top-level provider diagnostics.
pub(crate) fn response_parts(item: &BatchResponseItem) -> (HeaderMap, Bytes) {
    let mut headers = HeaderMap::new();
    if let Some(entries) = &item.headers {
        for (name, value) in entries {
            if let (Ok(name), Ok(value)) = (
                reqwest::header::HeaderName::from_bytes(name.as_bytes()),
                reqwest::header::HeaderValue::from_str(value),
            ) {
                headers.append(name, value);
            }
        }
    }
    // A serde_json::Value has no fallible custom serializer. Preserve the
    // original JSON projection for hydration and etag observation as well.
    let body = item
        .body
        .as_ref()
        .map(|value| Bytes::from(value.to_string()))
        .unwrap_or_default();
    (headers, body)
}
