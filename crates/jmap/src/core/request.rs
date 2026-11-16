use std::marker::PhantomData;

use serde::ser::{SerializeStruct, SerializeTuple};
use serde::{Serialize, Serializer};

use crate::client::{Client, SessionState};
use crate::core::transport::HttpTransport;

use super::capability::{Capability, Core};
use super::id::AccountId;
use super::method::JmapMethod;
use super::response::Response;

/// A typed handle to a method call in a request batch.
///
/// The type parameter `M` ties this handle to the method that produced it,
/// ensuring compile-time safety when extracting the response.
pub(crate) struct CallHandle<M: JmapMethod> {
    pub(crate) call_id: String,
    pub(crate) answer_context: super::answer::AnswerContext,
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
            name: self.answer_context.method,
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
    pub(crate) replay_safe: bool,
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

/// A JMAP request batch.
pub(crate) struct Request<
    'x,
    T: HttpTransport = crate::transport_reqwest::ReqwestTransport,
    C: Capability = Core,
> {
    client: &'x Client<T>,
    session_state: std::sync::Arc<SessionState>,
    account_id: AccountId,
    capability: PhantomData<fn() -> C>,
    pub(crate) using: Vec<&'static str>,
    method_calls: Vec<RawMethodCall>,
    pub(crate) created_ids: Option<std::collections::HashMap<String, String>>,
}

impl<T: HttpTransport, C: Capability> Serialize for Request<'_, T, C> {
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

impl<'x, T: HttpTransport, C: Capability> Request<'x, T, C> {
    fn scoped(client: &'x Client<T>, account_id: AccountId) -> Self {
        let session_state = client.session_state();
        Request {
            using: vec!["urn:ietf:params:jmap:core"],
            method_calls: Vec::new(),
            created_ids: None,
            account_id,
            capability: PhantomData,
            client,
            session_state,
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

    #[cfg(test)]
    pub(crate) fn call_count(&self) -> usize {
        self.method_calls.len()
    }

    /// The wire replay policy of this batch: `Idempotent` when every call is
    /// replay-safe (an empty request has no effect and qualifies), else
    /// `Never`.
    pub(crate) fn replay_policy(&self) -> bifrost_net::ReplayPolicy {
        if self.method_calls.iter().all(|call| call.replay_safe) {
            bifrost_net::ReplayPolicy::Idempotent
        } else {
            bifrost_net::ReplayPolicy::Never
        }
    }

    pub(crate) fn for_account(account: &'x crate::account::Account<T, C>) -> Self {
        Self::scoped(account.client(), account.id().clone())
    }

    #[cfg(test)]
    pub(crate) fn test_scoped(client: &'x Client<T>, account_id: impl Into<AccountId>) -> Self {
        Self::scoped(client, account_id.into())
    }

    /// Add a method call to the batch. Returns a typed handle for
    /// extracting the response later.
    ///
    /// The request's account ID is injected into the method via
    /// [`JmapMethod::set_account_id`] just before serialization, so
    /// callers no longer pass `accountId` through every method-struct
    /// constructor.
    pub(crate) fn call<M: JmapMethod<Cap = C>>(
        &mut self,
        method: M,
    ) -> Result<CallHandle<M>, crate::Error> {
        self.push(method)
    }

    /// Explicit cross-family call on this same account, checked against the
    /// request's frozen session. Existing result references stay in this batch.
    pub(crate) fn call_as<M: JmapMethod>(
        &mut self,
        method: M,
    ) -> Result<CallHandle<M>, crate::Error> {
        require_capability::<M::Cap>(self.session_state.session(), &self.account_id)?;
        self.push(method)
    }

    fn push<M: JmapMethod>(&mut self, mut method: M) -> Result<CallHandle<M>, crate::Error> {
        if let Some(max) = self
            .session_state
            .session()
            .core_limits()
            .max_calls_in_request()
            && self.method_calls.len() >= max
        {
            return Err(crate::Error::RequestCallLimit { max });
        }
        let call_id = format!("s{}", self.method_calls.len());

        // Inject the request's account ID into the method's accountId
        // field. No-op for methods that don't carry an accountId.
        method.set_account_id(&self.account_id);

        // Serialize method arguments once as raw JSON. This is an
        // outbound encode; preserve its local request provenance.
        let arguments =
            serde_json::value::to_raw_value(&method).map_err(crate::Error::RequestEncode)?;

        // Read the actual serialized guard, including method-specific copy guards.
        #[derive(serde::Deserialize)]
        struct Guards {
            #[serde(rename = "ifInState")]
            state: Option<serde::de::IgnoredAny>,
            #[serde(rename = "ifFromInState")]
            from: Option<serde::de::IgnoredAny>,
            #[serde(rename = "destroyFromIfInState")]
            destroy: Option<serde::de::IgnoredAny>,
        }
        let guards: Guards =
            serde_json::from_str(arguments.get()).map_err(crate::Error::RequestEncode)?;
        let answer_context = super::answer::AnswerContext {
            method: M::NAME,
            guarded: guards.state.is_some() || guards.from.is_some() || guards.destroy.is_some(),
            write: !M::REPLAY_SAFE,
        };

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

        for uri in method.additional_capabilities() {
            if !self.using.contains(&uri) {
                self.using.push(uri);
            }
        }

        // Auto-add capability
        let uri = M::Cap::URI;
        if !self.using.contains(&uri) {
            self.using.push(uri);
        }

        self.method_calls.push(RawMethodCall {
            replay_safe: M::REPLAY_SAFE,
            name: M::NAME,
            arguments,
            call_id: call_id.clone(),
        });

        Ok(CallHandle {
            answer_context,
            call_id,
            _phantom: PhantomData,
        })
    }

    /// Add a capability URI to the `using` array.
    pub(crate) fn add_capability<D: Capability>(&mut self) {
        let uri = D::URI;
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
pub(crate) trait MethodTuple<C: Capability>: Sized {
    type Responses;

    fn add_to_request<T: HttpTransport>(
        self,
        request: &mut Request<'_, T, C>,
    ) -> Result<MethodTupleExtractor<Self::Responses>, crate::Error>;
}

macro_rules! impl_method_tuple {
    ($($M:ident),+ $(,)?) => {
        impl<C: Capability, $($M),+> MethodTuple<C> for ($($M,)+)
        where
            $($M: JmapMethod<Cap = C> + 'static),+
        {
            type Responses = ($($M::Response,)+);

            fn add_to_request<TR: HttpTransport>(
                self,
                request: &mut Request<'_, TR, C>,
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

impl<T: HttpTransport, C: Capability> Request<'_, T, C> {
    /// Send a tuple of methods in one batch and return their typed
    /// responses as a tuple. See the module-level note on result
    /// references.
    ///
    /// Returns `Ok(None)`, having sent nothing, when the encoded batch
    /// exceeds `max_size` - the caller then falls back to smaller
    /// requests rather than handing the server a batch it will reject
    /// wholesale with a request-level `limit` error.
    pub(crate) async fn send_methods_within<M: MethodTuple<C>>(
        mut self,
        methods: M,
        max_size: usize,
    ) -> crate::Result<Option<M::Responses>> {
        let extract = methods.add_to_request(&mut self)?;
        let advertised = self
            .session_state()
            .session()
            .core_limits()
            .max_size_request();
        let max_size = advertised.map_or(max_size, |max| max.min(max_size));
        if self.encoded_len()? > max_size {
            return Ok(None);
        }
        let response = self.send().await?;
        extract(response).map(Some)
    }
}

#[cfg(feature = "websockets")]
impl<C: Capability> Request<'_, crate::transport_reqwest::ReqwestTransport, C> {
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

impl<'x, T: HttpTransport> Request<'x, T, Core> {
    /// Accountless core requests (Echo and PushSubscription) need no primary.
    pub(crate) fn new(client: &'x Client<T>) -> Self {
        Self::scoped(client, AccountId::new(""))
    }
}

/// A primaryAccounts entry itself proves selection even on older servers
/// omitting an optional account block. Explicit foreign views require the
/// capability on that account; another account's primary is never a fallback.
pub(crate) fn require_capability<C: Capability>(
    session: &super::session::Session,
    id: &AccountId,
) -> crate::Result<()> {
    if C::URI == Core::URI
        || session
            .primary_accounts()
            .any(|(uri, account)| uri.as_str() == C::URI && account == id)
        || session
            .account(id.as_str())
            .is_some_and(|value| value.capability(C::URI).is_some())
    {
        Ok(())
    } else {
        Err(crate::Error::AccountCapability {
            account_id: id.clone(),
            capability: C::URI,
        })
    }
}

#[cfg(test)]
mod wire_tests;
