use std::marker::PhantomData;

use serde::ser::{SerializeStruct, SerializeTuple};
use serde::{Serialize, Serializer};

use crate::client::{Client, SessionState};
use crate::core::transport::HttpTransport;

use super::capability::Capability;
use super::id::AccountId;
use super::method::JmapMethod;
use super::response::Response;

/// A typed handle to a method call in a request batch.
///
/// The type parameter `M` ties this handle to the method that produced it,
/// ensuring compile-time safety when extracting the response.
pub(crate) struct CallHandle<M: JmapMethod> {
    pub(crate) call_id: String,
    pub(crate) method_name: &'static str,
    pub(crate) _phantom: PhantomData<M>,
}

impl<M: JmapMethod> CallHandle<M> {
    /// Create a result reference pointing to a path in this call's response.
    ///
    /// Example: `handle.result_reference("/ids")` references the `ids` array
    /// from a query response.
    pub(crate) fn result_reference(&self, path: impl Into<String>) -> ResultReference {
        ResultReference {
            result_of: self.call_id.clone(),
            name: self.method_name,
            path: path.into(),
        }
    }
}

/// A JMAP result reference (RFC 8620 §3.7).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ResultReference {
    #[serde(rename = "resultOf")]
    pub(crate) result_of: String,
    pub(crate) name: &'static str,
    pub(crate) path: String,
}

/// A type-erased method call stored in the request batch.
pub(crate) struct RawMethodCall {
    pub(crate) name: &'static str,
    pub(crate) arguments: Box<serde_json::value::RawValue>,
    pub(crate) call_id: String,
}

impl Serialize for RawMethodCall {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(3)?;
        tuple.serialize_element(self.name)?;
        tuple.serialize_element(&self.arguments)?;
        tuple.serialize_element(&self.call_id)?;
        tuple.end()
    }
}

/// What the open session says about `maxCallsInRequest` (RFC 8620 §2).
///
/// Three states, kept apart deliberately. Only the third is a bound this
/// request may enforce; collapsing the first two into "the limit is zero"
/// turns a malformed or capability-shifted session into a request that
/// can never contain a single method call. That matters because account
/// opening issues probe requests *before* `sync::capabilities::build`
/// validates the session, so an enforced zero would fail the open as a
/// client bug and the session would never reach the classification -
/// `CapabilityChanged` for an absent core capability,
/// `Protocol(ContractViolation)` for a zero-valued one - that tells the
/// engine how to recover. Not enforcing here loses nothing: the
/// capability validation is the gate for both bad states, and a server
/// that advertised no usable limit has no limit for us to respect.
#[derive(Debug, Clone, Copy)]
enum CallLimit {
    /// No limit was advertised at all: either the session document
    /// carries no `urn:ietf:params:jmap:core` capability, or it carries
    /// one that omits `maxCallsInRequest`.
    Unadvertised,
    /// A limit was advertised and it is not usable: the core block
    /// advertises `maxCallsInRequest: 0` (which the spec forbids), or the
    /// block itself is present but unparseable, so whatever it advertised
    /// cannot be honoured.
    Invalid,
    /// A usable advertised bound.
    Advertised(std::num::NonZeroUsize),
}

impl CallLimit {
    fn read(session: &super::session::Session) -> Self {
        use super::session::CoreCapabilityState;
        match session.core_capability_state() {
            CoreCapabilityState::Absent => CallLimit::Unadvertised,
            CoreCapabilityState::Malformed => CallLimit::Invalid,
            CoreCapabilityState::Present(core) => match core.max_calls_in_request() {
                None => CallLimit::Unadvertised,
                Some(max) => std::num::NonZeroUsize::new(max)
                    .map_or(CallLimit::Invalid, CallLimit::Advertised),
            },
        }
    }

    /// The bound to enforce, if any. `None` for both unusable states.
    fn enforced(self) -> Option<usize> {
        match self {
            CallLimit::Unadvertised | CallLimit::Invalid => None,
            CallLimit::Advertised(max) => Some(max.get()),
        }
    }
}

/// A JMAP request batch.
pub(crate) struct Request<'x, T: HttpTransport = crate::transport_reqwest::ReqwestTransport> {
    client: &'x Client<T>,
    session_state: std::sync::Arc<SessionState>,
    account_id: AccountId,
    pub(crate) using: Vec<&'static str>,
    pub(crate) method_calls: Vec<RawMethodCall>,
    pub(crate) created_ids: Option<std::collections::HashMap<String, String>>,
    max_calls: CallLimit,
}

impl<T: HttpTransport> Serialize for Request<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let field_count = if self.created_ids.is_some() { 3 } else { 2 };
        let mut s = serializer.serialize_struct("Request", field_count)?;
        s.serialize_field("using", &self.using)?;
        s.serialize_field("methodCalls", &self.method_calls)?;
        if let Some(ref ids) = self.created_ids {
            s.serialize_field("createdIds", ids)?;
        }
        s.end()
    }
}

impl<'x, T: HttpTransport> Request<'x, T> {
    pub(crate) fn new(client: &'x Client<T>) -> Self {
        let session_state = client.session_state();
        let max_calls = CallLimit::read(session_state.session());
        Request {
            using: vec!["urn:ietf:params:jmap:core"],
            method_calls: Vec::new(),
            created_ids: None,
            account_id: session_state.default_account_id().clone(),
            client,
            session_state,
            max_calls,
        }
    }

    /// Session-derived values used to build this request. Holding the
    /// snapshot through send keeps its account, limits, and API endpoint
    /// together if another task refreshes the client meanwhile. WebSocket
    /// encoding must use this accessor too rather than re-reading Client's
    /// current session.
    pub(crate) fn session_state(&self) -> &SessionState {
        &self.session_state
    }

    pub(crate) fn account_id(mut self, account_id: impl Into<AccountId>) -> Self {
        self.account_id = account_id.into();
        self
    }

    /// The default account ID for this request.
    pub(crate) fn default_account_id(&self) -> &AccountId {
        &self.account_id
    }

    /// Add a method call to the batch. Returns a typed handle for
    /// extracting the response later.
    ///
    /// The request's account ID is injected into the method via
    /// [`JmapMethod::set_account_id`] just before serialization, so
    /// callers no longer pass `accountId` through every method-struct
    /// constructor.
    pub(crate) fn call<M: JmapMethod>(
        &mut self,
        mut method: M,
    ) -> Result<CallHandle<M>, crate::Error> {
        if let Some(max) = self.max_calls.enforced()
            && self.method_calls.len() >= max
        {
            return Err(crate::Error::RequestCallLimit { max });
        }
        let call_id = format!("s{}", self.method_calls.len());

        // Inject the request's account ID into the method's accountId
        // field. No-op for methods that don't carry an accountId.
        method.set_account_id(&self.account_id);

        // Serialize method arguments once as raw JSON. This is an
        // outbound encode; classify as RequestEncode rather than the
        // default ResponseDecode conversion.
        let arguments =
            serde_json::value::to_raw_value(&method).map_err(crate::Error::RequestEncode)?;

        // Accountless methods (notably PushSubscription/get and /set) remain
        // legal without primary accounts. Inspect the actual wire member so
        // this guard follows each method's accountId serialization contract.
        if self.account_id.as_str().is_empty() {
            let value: serde_json::Value =
                serde_json::from_str(arguments.get()).map_err(crate::Error::RequestEncode)?;
            if value.get("accountId").is_some() {
                return Err(crate::Error::NoPrimaryAccount {
                    capability: M::Cap::URI,
                });
            }
        }

        // Auto-add capability
        let uri = M::Cap::URI;
        if !self.using.contains(&uri) {
            self.using.push(uri);
        }

        self.method_calls.push(RawMethodCall {
            name: M::NAME,
            arguments,
            call_id: call_id.clone(),
        });

        Ok(CallHandle {
            call_id,
            method_name: M::NAME,
            _phantom: PhantomData,
        })
    }

    /// Add a capability URI to the `using` array.
    pub(crate) fn add_capability<C: Capability>(&mut self) {
        let uri = C::URI;
        if !self.using.contains(&uri) {
            self.using.push(uri);
        }
    }

    /// Byte length of this request exactly as [`Request::send`] will
    /// encode it.
    ///
    /// `maxSizeRequest` (RFC 8620 §2) is a hard limit the server
    /// rejects the whole request over, so a call site deciding how much
    /// to batch measures the real encoding rather than estimating from
    /// the call count. Shares `serde_json::to_vec` with the send path so
    /// the measurement and the wire bytes cannot drift apart.
    pub(crate) fn encoded_len(&self) -> crate::Result<usize> {
        serde_json::to_vec(self)
            .map(|body| body.len())
            .map_err(crate::Error::RequestEncode)
    }

    /// Send the request and get the full response.
    pub(crate) async fn send(self) -> crate::Result<Response> {
        self.client.send_request(&self).await
    }

    /// Send the request and extract the response for the given handle.
    ///
    /// Validates that the response contains a matching call ID and handles
    /// method errors. Equivalent to `send().await?.get(&handle)?`.
    pub(crate) async fn send_single<M: JmapMethod>(
        self,
        handle: &CallHandle<M>,
    ) -> crate::Result<M::Response> {
        let mut response = self.send().await?;
        response.get(handle)
    }
}

// -- Typed batch results --
//
// `Request::send_methods_within((m1, m2, ...), max_size)` adds the
// methods to the batch in order, sends the request if it fits the
// server's advertised `maxSizeRequest`, and returns a tuple of the
// typed responses. For batches that do not need a result reference
// between calls, this collapses the
// `let h = request.call(m)?; ... let r = response.get(&h)?` dance
// into a single expression:
//
// ```ignore
// let Some((q, g)) = account
//     .build()
//     .send_methods_within((email_query, email_get), max_size)
//     .await?
// else {
//     // too large for one request; fall back to smaller ones
// };
// ```
//
// The size limit is a required argument rather than an optional guard
// because `maxSizeRequest` is a hard limit (RFC 8620 §2) that the
// server enforces by rejecting the whole request: a batching call site
// that has not decided what to do when its batch does not fit has a
// bug, not a default.
//
// Result-reference flows (where method N needs a `CallHandle` from
// method N-1 to construct an `ids_ref`/`mailbox_ids_ref`/etc.) keep
// using the explicit `request.call(m)?` + `response.get(&h)?` path -
// the handles are not exposed through the tuple boundary by design.

/// Boxed extractor closure returned by [`MethodTuple::add_to_request`]:
/// consumes the response and produces the typed tuple of results.
type MethodTupleExtractor<R> = Box<dyn FnOnce(Response) -> crate::Result<R> + Send>;

/// Trait implemented for tuples of `JmapMethod` values, for typed
/// batch sends. See [`Request::send_methods`].
pub(crate) trait MethodTuple: Sized {
    type Responses;

    fn add_to_request<T: HttpTransport>(
        self,
        request: &mut Request<'_, T>,
    ) -> Result<MethodTupleExtractor<Self::Responses>, crate::Error>;
}

macro_rules! impl_method_tuple {
    ($($M:ident),+ $(,)?) => {
        impl<$($M),+> MethodTuple for ($($M,)+)
        where
            $($M: JmapMethod + 'static),+
        {
            type Responses = ($($M::Response,)+);

            fn add_to_request<TR: HttpTransport>(
                self,
                request: &mut Request<'_, TR>,
            ) -> Result<MethodTupleExtractor<Self::Responses>, crate::Error> {
                #[allow(non_snake_case)]
                let ($($M,)+) = self;
                $(
                    #[allow(non_snake_case)]
                    let $M: CallHandle<$M> = request.call($M)?;
                )+
                Ok(Box::new(move |mut response: Response| {
                    Ok(($(response.get(&$M)?,)+))
                }))
            }
        }
    };
}

impl_method_tuple!(M1);
impl_method_tuple!(M1, M2);
impl_method_tuple!(M1, M2, M3);
impl_method_tuple!(M1, M2, M3, M4);
impl_method_tuple!(M1, M2, M3, M4, M5);
impl_method_tuple!(M1, M2, M3, M4, M5, M6);
impl_method_tuple!(M1, M2, M3, M4, M5, M6, M7);
impl_method_tuple!(M1, M2, M3, M4, M5, M6, M7, M8);

impl<T: HttpTransport> Request<'_, T> {
    /// Send a tuple of methods in one batch and return their typed
    /// responses as a tuple. See the module-level note on result
    /// references.
    ///
    /// Returns `Ok(None)`, having sent nothing, when the encoded batch
    /// exceeds `max_size` - the caller then falls back to smaller
    /// requests rather than handing the server a batch it will reject
    /// wholesale with a request-level `limit` error.
    pub(crate) async fn send_methods_within<M: MethodTuple>(
        mut self,
        methods: M,
        max_size: usize,
    ) -> crate::Result<Option<M::Responses>> {
        let extract = methods.add_to_request(&mut self)?;
        if self.encoded_len()? > max_size {
            return Ok(None);
        }
        let response = self.send().await?;
        extract(response).map(Some)
    }
}

#[cfg(feature = "websockets")]
impl Request<'_, crate::transport_reqwest::ReqwestTransport> {
    pub(crate) async fn send_ws(self) -> crate::Result<String> {
        self.client.send_ws(self).await
    }

    /// Send over the WebSocket and await the response frame carrying the
    /// same RFC 8887 `requestId`, the WebSocket counterpart of
    /// [`Request::send`].
    pub(crate) async fn send_ws_awaiting(self) -> crate::Result<Response> {
        self.client.send_ws_awaiting(self).await?.response().await
    }
}

#[cfg(test)]
mod call_limit_tests {
    use super::CallLimit;
    use crate::core::session::Session;

    fn session_with_capabilities(capabilities: serde_json::Value) -> Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": capabilities,
            "accounts": {},
            "primaryAccounts": {},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    fn session_with_core(core: serde_json::Value) -> Session {
        session_with_capabilities(serde_json::json!({ "urn:ietf:params:jmap:core": core }))
    }

    /// The three lanes must be told apart at the source. "No core block",
    /// "core block that omits the limit", "core block that advertises
    /// zero" and "core block that does not parse" all decline to enforce,
    /// so nothing but the lane itself distinguishes them here - and the
    /// session validator classifies each differently.
    #[test]
    fn call_limit_reads_the_lane_the_session_actually_describes() {
        assert!(matches!(
            CallLimit::read(&session_with_capabilities(serde_json::json!({}))),
            CallLimit::Unadvertised
        ));
        assert!(
            matches!(
                CallLimit::read(&session_with_core(serde_json::json!({}))),
                CallLimit::Unadvertised
            ),
            "an OMITTED maxCallsInRequest advertised no limit; \
             it is not an advertised zero"
        );
        assert!(matches!(
            CallLimit::read(&session_with_core(
                serde_json::json!({"maxCallsInRequest": 0})
            )),
            CallLimit::Invalid
        ));
        assert!(
            matches!(
                CallLimit::read(&session_with_core(
                    serde_json::json!({"maxCallsInRequest": "16"})
                )),
                CallLimit::Invalid
            ),
            "a present-but-unparseable core block advertised something \
             unusable; it must not read as unadvertised"
        );
        assert!(matches!(
            CallLimit::read(&session_with_core(
                serde_json::json!({"maxCallsInRequest": 16})
            )),
            CallLimit::Advertised(max) if max.get() == 16
        ));
    }
}
