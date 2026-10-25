#![cfg(test)]

use serde_json::json;

use std::marker::PhantomData;

use super::SetCreate;
use super::get::{GetObject, GetResponse};
use super::method::JmapMethod;
use super::query::QueryObject;
use super::request::CallHandle;
use super::response::Response;
use crate::Error;

// -- Minimal test types --

mod test_marker {
    pub(crate) enum TestObj {}
}
pub(crate) type TestObjId = crate::core::id::Id<test_marker::TestObj>;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct TestObj {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct TestObjCreate {
    #[serde(skip)]
    _create_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct TestObjPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub(crate) enum TestProp {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
}

impl std::fmt::Display for TestProp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestProp::Id => write!(f, "id"),
            TestProp::Name => write!(f, "name"),
        }
    }
}

impl super::Object for TestObj {
    type Property = TestProp;
    type Id = TestObjId;
    fn requires_account_id() -> bool {
        true
    }
}

impl GetObject for TestObj {
    type GetArguments = ();
}

impl super::set::SetObject for TestObj {
    type Create = TestObjCreate;
    type Patch = TestObjPatch;
    type SetArguments = ();
}

impl SetCreate for TestObjCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }
    fn new(create_id: Option<usize>) -> Self {
        TestObjCreate {
            _create_id: create_id,
            name: None,
        }
    }
}

impl super::changes::ChangesObject for TestObj {
    type ChangesResponse = ();
}

impl QueryObject for TestObj {
    type QueryArguments = ();
    type Filter = ();
    type Sort = ();
}

crate::define_get_method!(TestGet, TestObj, "Test/get", crate::core::capability::Core);
crate::define_query_method!(
    TestQuery,
    TestObj,
    "Test/query",
    crate::core::capability::Core
);
// A second get method on a DIFFERENT capability, so the `using`
// accumulation in `Request::call` has something to accumulate. Every
// other test method rides `Core`, which `Request::new` already seeds.
crate::define_get_method!(
    TestMailGet,
    TestObj,
    "TestMail/get",
    crate::core::capability::Mail
);

fn make_handle<M: JmapMethod>(call_id: &str) -> CallHandle<M> {
    CallHandle {
        call_id: call_id.to_string(),
        method_name: M::NAME,
        _phantom: PhantomData,
    }
}

#[test]
fn response_get_extracts_typed_result() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Test/get", {
                "accountId": "A1",
                "state": "s1",
                "list": [{"id": "t1", "name": "hello"}],
                "notFound": []
            }, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s0");
    let result: GetResponse<TestObj> = response.get(&handle).unwrap();

    assert_eq!(result.state(), "s1");
    assert_eq!(result.list().len(), 1);
    assert_eq!(result.list()[0].name.as_deref(), Some("hello"));
}

/// The RFC 8887 WebSocket door cannot deserialize a frame straight into
/// `Response` - the frame carries `@type` and `requestId` beside the
/// envelope fields - so it decodes the `methodResponses` array through
/// `Response::from_frame_parts`. That path must produce exactly what the
/// HTTP envelope produces: typed extraction on a success entry, and the
/// `"error"` method name split into a `MethodError` rather than handed
/// out as data.
#[test]
fn frame_parts_decode_into_the_same_response_the_envelope_produces() {
    let method_responses = serde_json::value::RawValue::from_string(
        r#"[["Test/get",{"accountId":"A1","state":"s1","list":[{"id":"t1","name":"hello"}],"notFound":[]},"s0"],
            ["error",{"type":"unknownMethod"},"s1"]]"#
            .to_string(),
    )
    .unwrap();

    let mut response = Response::from_frame_parts(
        &method_responses,
        Some(std::collections::HashMap::from([(
            "k".to_string(),
            "v".to_string(),
        )])),
        "abc".to_string(),
    )
    .unwrap();

    assert_eq!(response.session_state(), "abc");
    assert_eq!(
        response.created_ids().and_then(|ids| ids.get("k")),
        Some(&"v".to_string())
    );

    let result: GetResponse<TestObj> = response.get(&make_handle::<TestGet>("s0")).unwrap();
    assert_eq!(result.list()[0].name.as_deref(), Some("hello"));

    let err = response.get(&make_handle::<TestGet>("s1")).unwrap_err();
    assert!(
        matches!(err, Error::Method(_)),
        "an `error` entry must stay a method error, got {err:?}"
    );
}

/// A `methodResponses` array that is not an array of call triples is a
/// decode failure, not a silently empty response: the WebSocket door
/// reports it as `Error::ResponseDecode` while still having read the
/// frame's `sessionState`.
#[test]
fn frame_parts_reject_malformed_method_responses() {
    let method_responses = serde_json::value::RawValue::from_string("[[1,2]]".to_string()).unwrap();

    assert!(Response::from_frame_parts(&method_responses, None, "abc".to_string()).is_err());
}

// Decode leniency only. An absent `notFound` decoding as empty is NOT
// evidence that every requested id was answered - `GetResponse` does not
// know which ids were requested. Callers with a closed per-item
// accounting contract reconcile against their own submitted ids
// (`sync::hydrate::reconcile_hydration`, `sync::contacts::reconcile_cards`).
#[test]
fn get_response_accepts_an_omitted_empty_not_found_list() {
    let raw_json = json!({
        "accountId": "A1",
        "state": "s1",
        "list": []
    });

    let response: GetResponse<TestObj> = serde_json::from_value(raw_json)
        .expect("servers that omit an empty notFound list are accepted");
    assert!(response.not_found().is_empty());
}

#[test]
fn response_get_returns_call_not_found_for_wrong_id() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Test/get", {"accountId": "A1", "state": "s1", "list": [], "notFound": []}, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s99");
    let err = response.get(&handle).unwrap_err();

    assert!(matches!(err, Error::CallNotFound(id) if id == "s99"));
}

#[test]
fn response_get_returns_method_error() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["error", {"type": "unknownMethod"}, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s0");
    let err = response.get(&handle).unwrap_err();

    assert!(matches!(err, Error::Method(_)));
}

#[test]
fn response_mixed_success_and_error() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Test/get", {
                "accountId": "A1",
                "state": "s1",
                "list": [{"id": "t1"}],
                "notFound": []
            }, "s0"],
            ["error", {"type": "unknownMethod"}, "s1"],
            ["Test/query", {
                "accountId": "A1",
                "queryState": "q1",
                "canCalculateChanges": true,
                "position": 0,
                "ids": ["t1", "t2"]
            }, "s2"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();

    let handle_get = make_handle::<TestGet>("s0");
    let get_result = response.get(&handle_get).unwrap();
    assert_eq!(get_result.list().len(), 1);

    let handle_err = make_handle::<TestGet>("s1");
    assert!(matches!(response.get(&handle_err), Err(Error::Method(_))));

    let handle_query = make_handle::<TestQuery>("s2");
    let query_result = response.get(&handle_query).unwrap();
    assert_eq!(query_result.ids().len(), 2);
}

#[test]
fn response_get_consumes_entry() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Test/get", {
                "accountId": "A1",
                "state": "s1",
                "list": [],
                "notFound": []
            }, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s0");

    let _ = response.get(&handle).unwrap();

    assert!(matches!(response.get(&handle), Err(Error::CallNotFound(_))));
}

#[test]
fn call_handle_result_reference() {
    let handle = make_handle::<TestGet>("s0");
    let ref_ = handle.result_reference("/ids");

    assert_eq!(ref_.result_of, "s0");
    assert_eq!(ref_.name, "Test/get");
    assert_eq!(ref_.path, "/ids");
}

/// Every net error shape that can carry a JMAP problem document has to
/// hand it to the problem-details conversion. A terminal 4xx arrives as
/// `Status`; a 429 or 5xx is retried first and arrives as `RateLimited`
/// / `RetryBudgetExhausted` with the final response preserved on the
/// error, so the body has to be lifted back out of that evidence.
#[test]
fn problem_details_from_every_body_bearing_transport_error() {
    use crate::core::transport::TransportError;

    let problem = bytes::Bytes::from(
        serde_json::to_vec(&json!({
            "type": "urn:ietf:params:jmap:error:limit",
            "title": "Too many requests"
        }))
        .unwrap(),
    );
    let final_response = |status| bifrost_net::FinalResponse {
        status,
        headers: reqwest::header::HeaderMap::new(),
        body: problem.clone(),
    };

    let shapes = [
        bifrost_net::Error::Status {
            code: reqwest::StatusCode::BAD_REQUEST,
            body: problem.clone(),
            headers: reqwest::header::HeaderMap::new(),
        },
        bifrost_net::Error::RateLimited {
            retry_after: None,
            final_response: final_response(reqwest::StatusCode::TOO_MANY_REQUESTS),
        },
        bifrost_net::Error::RetryBudgetExhausted {
            final_response: Some(final_response(reqwest::StatusCode::SERVICE_UNAVAILABLE)),
            retry_after_history: Vec::new(),
        },
    ];

    for shape in shapes {
        let described = shape.to_string();
        let error: Error = TransportError::from_net(shape).into();
        assert!(
            matches!(error, Error::Problem { .. }),
            "{described} must reach the problem-details mapping"
        );
    }
}

/// `AuthLost` is the deliberate exception: its 401 classification and
/// transmission evidence belong to bifrost-net, so its body is not
/// lifted into a status-derived problem guess.
///
/// Runs in EVERY configuration, which took a one-line change in bifrost-net to
/// become true. This used to be gated on `sync`, not because the behaviour
/// varied - bifrost-net depends on bifrost-types unconditionally, so it is
/// identical in every build - but because `sync` was what pulled in the
/// optional dependency needed to SPELL `TransmissionState`. A test whose
/// coverage depends on which features a sibling happens to enable is a
/// feature-graph accident; bifrost-net now re-exports the type that its own
/// `Error::AuthLost` carries, so the spelling is always available.
#[test]
fn auth_lost_stays_a_transport_error() {
    use crate::core::transport::TransportError;

    let error: Error = TransportError::from_net(bifrost_net::Error::AuthLost {
        transmission_state: Some(bifrost_net::TransmissionState::Acknowledged),
        final_response: Some(bifrost_net::FinalResponse {
            status: reqwest::StatusCode::UNAUTHORIZED,
            headers: reqwest::header::HeaderMap::new(),
            body: bytes::Bytes::from_static(br#"{"type":"urn:ietf:params:jmap:error:limit"}"#),
        }),
    })
    .into();

    assert!(matches!(error, Error::Transport(_)));
}

#[test]
fn transport_error_without_body_stays_transport() {
    use crate::core::transport::TransportError;

    let err = TransportError::new("connection refused");
    let error: Error = err.into();
    assert!(matches!(error, Error::Transport(_)));
}

#[test]
fn set_response_deserializes() {
    use super::set::SetResponse;

    let raw = json!({
        "accountId": "A1",
        "oldState": "s1",
        "newState": "s2",
        "created": {
            "c0": {"id": "new-1", "name": "created-obj"}
        },
        "updated": null,
        "destroyed": null,
        "notCreated": null,
        "notUpdated": null,
        "notDestroyed": null
    });

    let mut response: SetResponse<TestObj> = serde_json::from_value(raw).unwrap();
    assert_eq!(response.new_state(), Some("s2"));
    let created = response.created("c0").unwrap();
    assert_eq!(created.name.as_deref(), Some("created-obj"));
}

/// RFC 8620 s3.2 lets one method call produce SEVERAL responses under the
/// same call id, and this is the generic envelope, so consuming them out of
/// order is a fault of the layer rather than of any wired method.
/// `swap_remove` moved the LAST entry into the vacated slot, so the second
/// read under a repeated call id returned whichever response happened to
/// sit at the end - and it reordered unrelated handles read afterwards too.
///
/// Four same-id responses, not two: the first shape of this test used two
/// and PASSED against the `swap_remove` code by coincidence, since swapping
/// the last of two into slot zero is the order a second read wants anyway.
/// With four, the swapped-in tail is observable - the ablation fails with
/// "fourth" where "second" is due.
#[test]
fn repeated_call_ids_are_read_in_the_order_the_server_sent_them() {
    let raw = json!({
        "sessionState": "session-1",
        "methodResponses": [
            ["Test/get", {"accountId": "A1", "state": "s1", "list": [{"id": "first"}], "notFound": []}, "c0"],
            ["Test/get", {"accountId": "A1", "state": "s1", "list": [{"id": "second"}], "notFound": []}, "c0"],
            ["Test/get", {"accountId": "A1", "state": "s1", "list": [{"id": "third"}], "notFound": []}, "c0"],
            ["Test/get", {"accountId": "A1", "state": "s1", "list": [{"id": "fourth"}], "notFound": []}, "c0"]
        ]
    });
    let mut response: Response = serde_json::from_value(raw).expect("response decodes");
    let handle = make_handle::<TestGet>("c0");

    let ids = |response: super::get::GetResponse<TestObj>| {
        response
            .into_list()
            .into_iter()
            .filter_map(|obj| obj.id)
            .collect::<Vec<_>>()
    };

    assert_eq!(ids(response.get(&handle).expect("first")), vec!["first"]);
    assert_eq!(ids(response.get(&handle).expect("second")), vec!["second"]);
    assert_eq!(ids(response.get(&handle).expect("third")), vec!["third"]);
    assert_eq!(ids(response.get(&handle).expect("fourth")), vec!["fourth"]);
}

/// RFC 8620 s5.3 makes `newState` mandatory, so a response without one is
/// a non-conforming server - and it must stay distinguishable from a
/// server that reported an empty state. The accessor used to fold both
/// into `""`, which handed the sync layer's state cache (where an
/// explicitly empty entry has its own meaning) a value no server sent.
#[test]
fn an_absent_set_new_state_is_not_an_empty_one() {
    use super::set::SetResponse;

    let absent: SetResponse<TestObj> = serde_json::from_value(json!({
        "accountId": "A1",
        "oldState": "s1"
    }))
    .unwrap();
    assert_eq!(absent.new_state(), None);
    assert_eq!(absent.into_new_state(), None);

    let empty: SetResponse<TestObj> = serde_json::from_value(json!({
        "accountId": "A1",
        "oldState": "s1",
        "newState": ""
    }))
    .unwrap();
    assert_eq!(empty.new_state(), Some(""));
    assert_eq!(empty.into_new_state(), Some(String::new()));
}

#[test]
fn request_serializes_correctly() {
    let handle = make_handle::<TestQuery>("q0");
    let mut get = TestGet::new()
        .ids([TestObjId::new("obj-1")])
        .ids_ref(handle.result_reference("/ids"))
        .properties([TestProp::Id])
        .properties_ref(handle.result_reference("/properties"));
    get.set_account_id(&crate::core::id::AccountId::new("account-1"));
    let value = serde_json::to_value(&get).unwrap();
    assert_eq!(value.get("accountId"), Some(&json!("account-1")));

    let query = TestQuery::new()
        .filter(())
        .sort([super::query::Comparator::new(())])
        .position(1)
        .anchor("obj-1")
        .anchor_offset(-1)
        .limit(50)
        .calculate_total(true);
    let value = serde_json::to_value(&query).unwrap();
    assert_eq!(value.get("limit"), Some(&json!(50)));
}

// ---------------------------------------------------------------------------
// Request envelope over a stub transport
//
// `HttpTransport` is the crate's own transport seam, so the whole
// request/response envelope - `using` construction, `methodCalls`
// encoding, `accountId` injection, result references, `Response::get`
// matching, `send_methods` - can be exercised in-process with no
// listener, no port, and no daemon. The stub records the exact JSON body
// the client would have POSTed and replays a queued reply.
// ---------------------------------------------------------------------------

mod envelope {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use serde_json::{Value, json};

    use super::{TestGet, TestMailGet, TestObjId, TestQuery};
    use crate::client::Client;
    use crate::core::capability;
    use crate::core::session::Session;
    use crate::core::transport::{HttpTransport, TransportError};

    /// In-process `HttpTransport`. Captures every outbound API body as
    /// parsed JSON and answers from a FIFO of canned reply bodies. An
    /// exhausted queue answers a transport error, which is what a real
    /// connection failure looks like to the caller.
    struct StubTransport {
        sent: Arc<Mutex<Vec<Value>>>,
        policies: Arc<Mutex<Vec<bifrost_net::ReplayPolicy>>>,
        replies: Arc<Mutex<VecDeque<String>>>,
    }

    impl HttpTransport for StubTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            replay: bifrost_net::ReplayPolicy,
        ) -> Result<Bytes, TransportError> {
            self.policies.lock().unwrap().push(replay);
            let parsed: Value =
                serde_json::from_slice(&body).expect("client emitted a non-JSON request body");
            self.sent.lock().expect("stub sent lock").push(parsed);
            let next = self.replies.lock().expect("stub reply lock").pop_front();
            match next {
                Some(reply) => Ok(Bytes::from(reply)),
                None => Err(TransportError::new("stub transport has no queued reply")),
            }
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport does not upload"))
        }

        async fn download(&self, url: &str) -> Result<Bytes, TransportError> {
            self.sent
                .lock()
                .expect("stub sent lock")
                .push(json!({"downloadUrl": url}));
            Ok(Bytes::from_static(b"blob"))
        }

        async fn get_session(&self, _url: &str) -> Result<Bytes, TransportError> {
            Err(TransportError::new("stub transport has no session route"))
        }
    }

    /// A stub client plus a handle on everything it sent.
    struct Stub {
        client: Client<StubTransport>,
        sent: Arc<Mutex<Vec<Value>>>,
    }

    impl Stub {
        /// The `n`th request body the client POSTed.
        fn request(&self, n: usize) -> Value {
            self.sent.lock().expect("stub sent lock")[n].clone()
        }

        fn request_count(&self) -> usize {
            self.sent.lock().expect("stub sent lock").len()
        }
    }

    /// A session fixture. `primary_accounts` is spelled out per test so each
    /// request's account selection stays explicit, even though the generic
    /// client fallback is now stable.
    fn session(primary_accounts: Value) -> Session {
        session_with_max_calls(primary_accounts, 8)
    }

    fn session_with_max_calls(primary_accounts: Value, max_calls: usize) -> Session {
        serde_json::from_value(json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": max_calls,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                }
            },
            "accounts": {},
            "primaryAccounts": primary_accounts,
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("session fixture parses")
    }

    crate::define_set_method!(
        ReplayTestSet,
        super::TestObj,
        "Test/set",
        crate::core::capability::Core
    );

    #[test]
    fn request_replay_policy_is_idempotent_only_when_every_call_is_replay_safe() {
        let stub = stub(mail_primary(), []);
        let mut request = stub.client.build();
        assert_eq!(
            request.replay_policy(),
            bifrost_net::ReplayPolicy::Idempotent
        );
        request.call(TestGet::new()).unwrap();
        request.call(TestQuery::new()).unwrap();
        assert_eq!(
            request.replay_policy(),
            bifrost_net::ReplayPolicy::Idempotent
        );
        request.call(ReplayTestSet::new()).unwrap();
        assert_eq!(request.replay_policy(), bifrost_net::ReplayPolicy::Never);
        #[cfg(feature = "mail")]
        {
            let mut request = stub.client.build();
            request
                .call(crate::email::import::EmailImportRequest::new())
                .unwrap();
            assert_eq!(request.replay_policy(), bifrost_net::ReplayPolicy::Never);
        }
    }
    #[tokio::test]
    async fn send_request_hands_the_typed_batch_policy_to_the_transport() {
        let policies = Arc::new(Mutex::new(Vec::new()));
        let client = Client::with_transport(
            StubTransport {
                sent: Arc::new(Mutex::new(Vec::new())),
                replies: Arc::new(Mutex::new(VecDeque::new())),
                policies: Arc::clone(&policies),
            },
            session(mail_primary()),
            "https://example.test/.well-known/jmap",
        )
        .unwrap();
        let mut request = client.build();
        request.call(TestGet::new()).unwrap();
        assert!(client.send_request(&request).await.is_err());
        let mut request = client.build();
        request
            .call(ReplayTestSet::new().if_in_state("caller-old"))
            .unwrap();
        assert!(client.send_request(&request).await.is_err());
        assert_eq!(
            *policies.lock().unwrap(),
            [
                bifrost_net::ReplayPolicy::Idempotent,
                bifrost_net::ReplayPolicy::Never
            ]
        );
    }

    fn stub(primary_accounts: Value, replies: impl IntoIterator<Item = String>) -> Stub {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let transport = StubTransport {
            sent: Arc::clone(&sent),
            policies: Arc::new(Mutex::new(Vec::new())),
            replies: Arc::new(Mutex::new(replies.into_iter().collect())),
        };
        let client = Client::with_transport(
            transport,
            session(primary_accounts),
            "https://example.test/.well-known/jmap",
        )
        .expect("stub client builds");
        Stub { client, sent }
    }

    fn stub_with_max_calls(
        primary_accounts: Value,
        max_calls: usize,
        replies: impl IntoIterator<Item = String>,
    ) -> Stub {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let transport = StubTransport {
            sent: Arc::clone(&sent),
            policies: Arc::new(Mutex::new(Vec::new())),
            replies: Arc::new(Mutex::new(replies.into_iter().collect())),
        };
        let client = Client::with_transport(
            transport,
            session_with_max_calls(primary_accounts, max_calls),
            "https://example.test/.well-known/jmap",
        )
        .expect("stub client builds");
        Stub { client, sent }
    }

    #[cfg(feature = "blob")]
    #[tokio::test]
    async fn download_dot_filenames_preserve_the_resource_path() {
        let stub = stub(mail_primary(), []);
        for (name, encoded) in [
            (".", "%252E"),
            ("..", "%252E%252E"),
            ("a/b?x#+", "a%2Fb%3Fx%23%2B"),
        ] {
            let blob = crate::blob::BlobRef::new(
                crate::core::id::AccountId::new("A1"),
                crate::core::id::BlobId::new("B1"),
            )
            .with_name(name)
            .with_content_type("text/plain");
            stub.client.download(&blob).await.unwrap();
            let request = stub.request(stub.request_count() - 1);
            let parsed = reqwest::Url::parse(request["downloadUrl"].as_str().unwrap()).unwrap();
            assert_eq!(
                parsed.path(),
                format!("/download/A1/B1/{encoded}/text%2Fplain")
            );
        }
    }

    #[test]
    fn accountless_methods_work_without_primary_accounts() {
        let stub = stub(json!({}), []);
        let mut request = stub.client.build();
        request
            .call(crate::push_subscription::PushSubscriptionGet::new())
            .unwrap();
        request
            .call(crate::push_subscription::PushSubscriptionSet::new())
            .unwrap();
        let encoded = serde_json::to_value(&request).unwrap();
        for call in encoded["methodCalls"].as_array().unwrap() {
            assert!(call[1].get("accountId").is_none());
        }
        assert!(matches!(
            request.call(TestGet::new()),
            Err(crate::Error::NoPrimaryAccount { .. })
        ));
    }

    fn mail_primary() -> Value {
        json!({ "urn:ietf:params:jmap:mail": "A1" })
    }

    fn get_result(call_id: &str) -> Value {
        json!([
            "Test/get",
            {
                "accountId": "A1",
                "state": "s1",
                "list": [{"id": "t1", "name": "hello"}],
                "notFound": []
            },
            call_id
        ])
    }

    fn query_result(call_id: &str) -> Value {
        json!([
            "Test/query",
            {
                "accountId": "A1",
                "queryState": "q1",
                "canCalculateChanges": true,
                "position": 0,
                "ids": ["t1", "t2"]
            },
            call_id
        ])
    }

    fn reply(session_state: &str, results: Vec<Value>) -> String {
        json!({
            "sessionState": session_state,
            "methodResponses": results,
        })
        .to_string()
    }

    #[tokio::test]
    async fn request_serializes_using_method_calls_and_injected_account_id() {
        let stub = stub(mail_primary(), [reply("session-1", vec![get_result("s0")])]);

        let mut request = stub.client.build();
        let handle = request
            .call(TestGet::new().ids([TestObjId::new("t1")]))
            .expect("method encodes");
        let mut response = request.send().await.expect("stub replies");
        let result = response.get(&handle).expect("typed extraction");
        assert_eq!(result.state(), "s1");

        let body = stub.request(0);
        // `using` always carries core; the request has no other capability.
        assert_eq!(body["using"], json!(["urn:ietf:params:jmap:core"]));
        // `createdIds` is omitted entirely when unset (no `null` on the wire).
        assert!(body.get("createdIds").is_none());

        let calls = body["methodCalls"]
            .as_array()
            .expect("methodCalls is an array");
        assert_eq!(calls.len(), 1);
        // Each call is the RFC 8620 3-tuple [name, arguments, callId].
        assert_eq!(calls[0][0], json!("Test/get"));
        assert_eq!(calls[0][2], json!("s0"));
        // The accountId is injected from the request, not from the
        // method-struct constructor.
        assert_eq!(calls[0][1]["accountId"], json!("A1"));
        assert_eq!(calls[0][1]["ids"], json!(["t1"]));
    }

    #[tokio::test]
    async fn using_accumulates_each_capability_exactly_once() {
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![get_result("s0"), get_result("s1"), get_result("s2")],
            )],
        );

        let mut request = stub.client.build();
        let _core_a = request.call(TestGet::new()).expect("encodes");
        let _mail = request.call(TestMailGet::new()).expect("encodes");
        let _core_b = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");

        let body = stub.request(0);
        // Core is seeded by `Request::new`; Mail is appended once even
        // though a Core method is added on either side of it.
        assert_eq!(
            body["using"],
            json!(["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"])
        );
        let calls = body["methodCalls"].as_array().expect("array");
        // Call ids are positional and monotonic in insertion order.
        assert_eq!(calls[0][2], json!("s0"));
        assert_eq!(calls[1][2], json!("s1"));
        assert_eq!(calls[2][2], json!("s2"));
        assert_eq!(calls[1][0], json!("TestMail/get"));
    }

    #[tokio::test]
    async fn account_scoped_and_explicitly_overridden_account_ids_reach_the_wire() {
        let stub = stub(
            mail_primary(),
            [
                reply("session-1", vec![get_result("s0")]),
                reply("session-1", vec![get_result("s0")]),
            ],
        );

        // `Account::build` stamps the capability's primary account.
        let mail = stub
            .client
            .primary_account::<capability::Mail>()
            .expect("session lists a mail primary");
        mail.call(TestGet::new()).await.expect("stub replies");
        assert_eq!(
            stub.request(0)["methodCalls"][0][1]["accountId"],
            json!("A1")
        );

        // An explicit override wins over the client default.
        let mut request = stub.client.build().account_id("B2");
        let _ = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");
        assert_eq!(
            stub.request(1)["methodCalls"][0][1]["accountId"],
            json!("B2")
        );
    }

    #[tokio::test]
    async fn send_methods_batches_in_order_and_returns_typed_responses() {
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![get_result("s0"), query_result("s1")],
            )],
        );

        let (get, query) = stub
            .client
            .build()
            .send_methods_within((TestGet::new(), TestQuery::new()), usize::MAX)
            .await
            .expect("batch send")
            .expect("batch fits an unbounded size limit");

        assert_eq!(get.list().len(), 1);
        assert_eq!(query.ids().len(), 2);
        assert_eq!(stub.request_count(), 1, "one batch is one round trip");

        let calls = stub.request(0)["methodCalls"]
            .as_array()
            .expect("array")
            .clone();
        assert_eq!(calls[0][0], json!("Test/get"));
        assert_eq!(calls[1][0], json!("Test/query"));
    }

    #[tokio::test]
    async fn responses_returned_out_of_order_still_match_their_handles() {
        // RFC 8620 lets a server return method responses in any order it
        // likes; `Response::get` matches on the call id, so the tuple
        // extraction must not depend on positional alignment.
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![query_result("s1"), get_result("s0")],
            )],
        );

        let (get, query) = stub
            .client
            .build()
            .send_methods_within((TestGet::new(), TestQuery::new()), usize::MAX)
            .await
            .expect("batch send")
            .expect("batch fits an unbounded size limit");

        assert_eq!(get.state(), "s1");
        assert_eq!(query.ids().len(), 2);
    }

    /// `maxSizeRequest` is a hard limit: a batch that does not fit must
    /// not reach the wire at all, or the server rejects every call in it
    /// with a request-level `limit` error.
    #[tokio::test]
    async fn a_batch_over_the_size_limit_is_not_sent() {
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![get_result("s0"), query_result("s1")],
            )],
        );

        let sent = stub
            .client
            .build()
            .send_methods_within((TestGet::new(), TestQuery::new()), 8)
            .await
            .expect("size rejection is not an error");

        assert!(sent.is_none(), "an oversized batch reports no responses");
        assert_eq!(stub.request_count(), 0, "and sends nothing");
    }

    #[test]
    fn request_construction_refuses_the_advertised_call_limit() {
        let stub = stub_with_max_calls(mail_primary(), 1, []);
        let mut request = stub.client.build();
        request.call(TestGet::new()).expect("first call fits");

        let error = match request.call(TestQuery::new()) {
            Ok(_) => panic!("second call must exceed maxCallsInRequest"),
            Err(error) => error,
        };

        assert!(matches!(error, crate::Error::RequestCallLimit { max: 1 }));
        assert_eq!(
            request.method_calls.len(),
            1,
            "the rejected call is not added"
        );
        assert_eq!(
            stub.request_count(),
            0,
            "construction never reaches the wire"
        );
    }

    #[tokio::test]
    async fn result_references_serialize_as_hash_prefixed_arguments() {
        let stub = stub(
            mail_primary(),
            [reply(
                "session-1",
                vec![query_result("s0"), get_result("s1")],
            )],
        );

        let mut request = stub.client.build();
        let query = request.call(TestQuery::new()).expect("encodes");
        let _get = request
            .call(TestGet::new().ids_ref(query.result_reference("/ids")))
            .expect("encodes");
        request.send().await.expect("stub replies");

        let args = stub.request(0)["methodCalls"][1][1].clone();
        assert_eq!(
            args["#ids"],
            json!({"resultOf": "s0", "name": "Test/query", "path": "/ids"})
        );
        // Setting a reference clears the literal ids, so the server never
        // sees both forms of the same argument.
        assert!(args.get("ids").is_none());
    }

    #[tokio::test]
    async fn a_diverging_session_state_marks_the_cached_session_stale() {
        let stub = stub(
            mail_primary(),
            [
                reply("session-1", vec![get_result("s0")]),
                reply("session-2", vec![get_result("s0")]),
            ],
        );
        assert!(stub.client.is_session_updated());

        // A reply echoing the session's own state leaves it fresh.
        let mut request = stub.client.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");
        assert!(stub.client.is_session_updated());

        // A reply carrying a newer sessionState marks it stale so the
        // caller knows to re-read the session resource.
        let mut request = stub.client.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");
        assert!(!stub.client.is_session_updated());
    }

    #[tokio::test]
    async fn a_transport_failure_surfaces_as_error_transport() {
        // No queued reply: the stub answers the way a dropped connection
        // does, and the error must not be laundered into a decode error.
        let stub = stub(mail_primary(), Vec::<String>::new());
        let mut request = stub.client.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        let err = request.send().await.expect_err("transport failed");
        assert!(matches!(err, crate::Error::Transport(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn an_unparseable_response_body_is_a_response_decode_error() {
        let stub = stub(mail_primary(), ["not json at all".to_string()]);
        let mut request = stub.client.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        let err = request.send().await.expect_err("body is not JSON");
        assert!(
            matches!(err, crate::Error::ResponseDecode(_)),
            "got {err:?}"
        );
    }
}

#[test]
fn response_get_rejects_a_method_name_mismatch() {
    let raw_json = json!({
        "sessionState": "abc",
        "methodResponses": [
            ["Definitely/not-what-was-asked", {
                "accountId": "A1",
                "state": "s1",
                "list": [{"id": "t1", "name": "hello"}],
                "notFound": []
            }, "s0"]
        ]
    });

    let mut response: Response = serde_json::from_value(raw_json).unwrap();
    let handle = make_handle::<TestGet>("s0");
    let err = response
        .get::<TestGet>(&handle)
        .expect_err("a mismatched method name must be rejected");
    assert!(matches!(
        err,
        Error::UnexpectedMethodResponse {
            call_id,
            expected: "Test/get",
            actual,
        } if call_id == "s0" && actual == "Definitely/not-what-was-asked"
    ));
}
