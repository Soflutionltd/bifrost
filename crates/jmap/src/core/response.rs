use std::collections::HashMap;

use serde::Deserialize;
use serde::de;

use super::error::MethodError;
use super::method::JmapMethod;
use super::request::CallHandle;

/// A parsed JMAP response with typed method result extraction.
#[derive(Debug)]
pub(crate) struct Response {
    raw: Vec<(String, RawCallResult, String)>,
    session_state: String,
    created_ids: Option<HashMap<String, String>>,
}

/// A single method call result - either success data or a method error.
#[derive(Debug)]
enum RawCallResult {
    /// Raw JSON bytes - deserialized lazily in Response::get().
    Success(Box<serde_json::value::RawValue>),
    Error(MethodError),
}

impl Response {
    /// Extract a typed response by its call handle.
    ///
    /// Compile-time safe: the handle's type parameter ensures the response
    /// is deserialized into the correct type.
    ///
    /// RFC 8620 s3.2 lets ONE method call produce SEVERAL responses under
    /// the same call id. Each `get` takes the first remaining response for
    /// the handle, so repeated calls on one handle walk them in the order
    /// the server sent them. That ordering is the point of `remove` here:
    /// `swap_remove` is cheaper, but it moves the last entry into the
    /// vacated slot, which reorders every later lookup - including the
    /// second response under a repeated call id, and including unrelated
    /// handles read afterwards. The vector holds at most
    /// `maxCallsInRequest` entries, so the shift is not worth an ordering
    /// hazard.
    ///
    /// The API consumes one response at a time rather than returning a
    /// collection. The in-crate multi-response operation is RFC 8621's
    /// implicit `Email/set` after `EmailSubmission/set`; its caller reads
    /// that response with `get_method` after extracting the explicit result.
    /// A general `get_all` is not needed by current call sites.
    pub(crate) fn get<M: JmapMethod>(
        &mut self,
        handle: &CallHandle<M>,
    ) -> crate::Result<M::Response> {
        self.get_expected::<M>(&handle.call_id, handle.method_name)
    }

    /// Extract a response for a method whose call id is known but which
    /// was not explicitly added to the request. RFC 8621's implicit
    /// `Email/set` response uses the `EmailSubmission/set` call id.
    pub(crate) fn get_method<M: JmapMethod>(
        &mut self,
        call_id: &str,
    ) -> crate::Result<M::Response> {
        self.get_expected::<M>(call_id, M::NAME)
    }

    fn get_expected<M: JmapMethod>(
        &mut self,
        call_id: &str,
        expected_method: &'static str,
    ) -> crate::Result<M::Response> {
        let pos = self
            .raw
            .iter()
            .position(|(_, _, id)| id == call_id)
            .ok_or_else(|| crate::Error::CallNotFound(call_id.to_string()))?;

        let (method_name, result, call_id) = self.raw.remove(pos);

        match result {
            RawCallResult::Success(raw) => {
                if method_name != expected_method {
                    return Err(crate::Error::UnexpectedMethodResponse {
                        call_id,
                        expected: expected_method,
                        actual: method_name,
                    });
                }
                serde_json::from_str(raw.get()).map_err(crate::Error::from)
            }
            RawCallResult::Error(e) => Err(e.into()),
        }
    }

    pub(crate) fn session_state(&self) -> &str {
        &self.session_state
    }

    pub(crate) fn created_ids(&self) -> Option<&HashMap<String, String>> {
        self.created_ids.as_ref()
    }

    /// Build a `Response` from the already-split parts of an RFC 8887
    /// WebSocket `Response` frame.
    ///
    /// The WebSocket frame is not an HTTP response body: it carries the
    /// envelope fields alongside `@type` and `requestId`, so it cannot be
    /// deserialized straight into `Response`, and the frame's
    /// `sessionState` must be readable even when the method responses do
    /// not decode. This entry point takes `methodResponses` as the raw
    /// JSON slice the frame parse already captured and runs the SAME
    /// per-call split the envelope deserializer runs, so a response frame
    /// costs one pass over the array instead of a `serde_json::Value`
    /// rebuild followed by a second deserialization of it.
    pub(crate) fn from_frame_parts(
        method_responses: &serde_json::value::RawValue,
        created_ids: Option<HashMap<String, String>>,
        session_state: String,
    ) -> Result<Self, serde_json::Error> {
        let calls: Vec<(String, Box<serde_json::value::RawValue>, String)> =
            serde_json::from_str(method_responses.get())?;

        Ok(Response {
            raw: split_call_results(calls)?,
            session_state,
            created_ids,
        })
    }
}

/// Split raw `methodResponses` entries into success payloads and decoded
/// `MethodError`s. Shared by the HTTP envelope deserializer and the
/// WebSocket frame path so the two doors cannot drift.
fn split_call_results(
    calls: Vec<(String, Box<serde_json::value::RawValue>, String)>,
) -> Result<Vec<(String, RawCallResult, String)>, serde_json::Error> {
    let mut raw = Vec::with_capacity(calls.len());

    for (method_name, data, call_id) in calls {
        let result = if method_name == "error" {
            RawCallResult::Error(serde_json::from_str::<MethodError>(data.get())?)
        } else {
            RawCallResult::Success(data)
        };

        raw.push((method_name, result, call_id));
    }

    Ok(raw)
}

impl<'de> Deserialize<'de> for Response {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawEnvelope {
            #[serde(rename = "methodResponses")]
            method_responses: Vec<(String, Box<serde_json::value::RawValue>, String)>,
            #[serde(rename = "createdIds")]
            created_ids: Option<HashMap<String, String>>,
            #[serde(rename = "sessionState")]
            session_state: String,
        }

        let envelope = RawEnvelope::deserialize(deserializer)?;
        let raw = split_call_results(envelope.method_responses).map_err(de::Error::custom)?;

        Ok(Response {
            raw,
            session_state: envelope.session_state,
            created_ids: envelope.created_ids,
        })
    }
}
