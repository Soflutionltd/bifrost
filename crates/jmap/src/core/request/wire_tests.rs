use crate::core::method::JmapMethod;
use crate::core::test_support::*;
use serde_json::json;

#[test]
fn call_handle_result_reference() {
    let handle = make_handle::<TestGet>("s0");
    let ref_ = handle.result_reference("/ids");

    assert_eq!(ref_.result_of, "s0");
    assert_eq!(ref_.name, "Test/get");
    assert_eq!(ref_.path, "/ids");
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
        .sort([crate::core::query::Comparator::new(())])
        .position(1)
        .anchor("obj-1")
        .anchor_offset(-1)
        .limit(50)
        .calculate_total(true);
    let value = serde_json::to_value(&query).unwrap();
    assert_eq!(value.get("limit"), Some(&json!(50)));
}

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
        fn build(&self) -> crate::core::request::Request<'_, StubTransport> {
            crate::core::request::Request::test_scoped(&self.client, "A1")
        }

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

    crate::define_set_method!(ReplayTestSet, super::TestObj);

    #[test]
    fn unanswered_set_retains_the_serialized_request_guard() {
        let stub = stub(mail_primary(), []);
        for guarded in [false, true] {
            let mut request = stub.build();
            let mut set = ReplayTestSet::new();
            if guarded {
                set = set.if_in_state("s0");
            }
            let handle = request.call(set).unwrap();
            let mut response: crate::core::response::Response = serde_json::from_value(json!({
                "methodResponses": [["Test/set", {
                    "accountId": "a", "newState": "s1"
                }, "s0"]],
                "sessionState": "session"
            }))
            .unwrap();
            let mut result = response.get(&handle).unwrap();
            let error = result.updated(&TestObjId::new("missing")).unwrap_err();
            assert!(matches!(error, crate::Error::IdUnanswered { id, context }
                if id == "missing" && context.method == "Test/set"
                    && context.guarded == guarded && context.write));
        }
    }

    #[test]
    fn omitted_blob_copy_is_unanswered_but_a_declared_miss_is_refused() {
        use crate::blob::copy::CopyBlobRequest;
        use crate::core::id::BlobId;
        let stub = stub(mail_primary(), []);
        for refused in [false, true] {
            let mut request = stub.build();
            let handle = request
                .call(CopyBlobRequest::new("source").blob_id("blob"))
                .unwrap();
            let not_copied = if refused {
                json!({"blob": {"type": "notFound"}})
            } else {
                json!({})
            };
            let mut response: crate::core::response::Response = serde_json::from_value(json!({
                "methodResponses": [["Blob/copy", {
                    "fromAccountId": "source", "accountId": "a", "notCopied": not_copied
                }, "s0"]],
                "sessionState": "session"
            }))
            .unwrap();
            let mut result = response.get(&handle).unwrap();
            let error = result.copied(&BlobId::new("blob")).unwrap_err();
            if refused {
                assert!(matches!(error, crate::Error::Set(error)
                    if error.error_type() == &crate::core::set::SetErrorType::NotFound));
            } else {
                assert!(matches!(error, crate::Error::IdUnanswered { context, .. }
                    if context.method == "Blob/copy" && context.write && !context.guarded));
            }
        }
    }

    #[test]
    fn request_replay_policy_is_idempotent_only_when_every_call_is_replay_safe() {
        let stub = stub(mail_primary(), []);
        let mut request = stub.build();
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
            let mut request = stub.build();
            request
                .call_as(crate::email::import::EmailImportRequest::new())
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
        let mut request =
            crate::core::request::Request::<_, capability::Core>::test_scoped(&client, "A1");
        request.call(TestGet::new()).unwrap();
        assert!(client.send_request(&request).await.is_err());
        let mut request =
            crate::core::request::Request::<_, capability::Core>::test_scoped(&client, "A1");
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

        let mut request = stub.build();
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

        let mut request = stub.build();
        let _core_a = request.call(TestGet::new()).expect("encodes");
        let _mail = request.call_as(TestMailGet::new()).expect("encodes");
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
        mail.core()
            .call(TestGet::new())
            .await
            .expect("stub replies");
        assert_eq!(
            stub.request(0)["methodCalls"][0][1]["accountId"],
            json!("A1")
        );

        // An explicit override wins over the client default.
        let mut request =
            crate::core::request::Request::<_, capability::Core>::test_scoped(&stub.client, "B2");
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
        let mut request = stub.build();
        request.call(TestGet::new()).expect("first call fits");

        let error = match request.call(TestQuery::new()) {
            Ok(_) => panic!("second call must exceed maxCallsInRequest"),
            Err(error) => error,
        };

        assert!(matches!(error, crate::Error::RequestCallLimit { max: 1 }));
        assert_eq!(request.call_count(), 1, "the rejected call is not added");
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

        let mut request = stub.build();
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
        let mut request = stub.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");
        assert!(stub.client.is_session_updated());

        // A reply carrying a newer sessionState marks it stale so the
        // caller knows to re-read the session resource.
        let mut request = stub.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        request.send().await.expect("stub replies");
        assert!(!stub.client.is_session_updated());
    }

    #[tokio::test]
    async fn a_transport_failure_surfaces_as_error_transport() {
        // No queued reply: the stub answers the way a dropped connection
        // does, and the error must not be laundered into a decode error.
        let stub = stub(mail_primary(), Vec::<String>::new());
        let mut request = stub.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        let err = request.send().await.expect_err("transport failed");
        assert!(matches!(err, crate::Error::Transport(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn an_unparseable_response_body_is_a_response_decode_error() {
        let stub = stub(mail_primary(), ["not json at all".to_string()]);
        let mut request = stub.build();
        let _ = request.call(TestGet::new()).expect("encodes");
        let err = request.send().await.expect_err("body is not JSON");
        assert!(
            matches!(err, crate::Error::EnvelopeDecode(_)),
            "got {err:?}"
        );
    }
    #[cfg(feature = "calendars")]
    #[test]
    fn capability_accounts_keep_distinct_primaries_and_reject_crossed_views() {
        let stub = stub(
            json!({
                "urn:ietf:params:jmap:mail": "mail-account",
                "urn:ietf:params:jmap:calendars": "calendar-account"
            }),
            [],
        );
        let mail: crate::account::Account<_, capability::Mail> =
            stub.client.primary_account::<capability::Mail>().unwrap();
        let calendar: crate::account::Account<_, capability::Calendars> = stub
            .client
            .primary_account::<capability::Calendars>()
            .unwrap();
        let mut mail_request = mail.build();
        mail_request.call(TestMailGet::new()).unwrap();
        let mut calendar_request = calendar.build();
        calendar_request
            .call(crate::calendar::CalendarGet::new())
            .unwrap();
        assert_eq!(
            serde_json::to_value(&mail_request).unwrap()["methodCalls"][0][1]["accountId"],
            "mail-account"
        );
        assert_eq!(
            serde_json::to_value(&calendar_request).unwrap()["methodCalls"][0][1]["accountId"],
            "calendar-account"
        );
        assert!(matches!(mail.view::<capability::Calendars>(),
            Err(crate::Error::AccountCapability { account_id, capability })
                if account_id.as_str() == "mail-account" && capability == "urn:ietf:params:jmap:calendars"));
        assert!(matches!(
            mail_request.call_as(crate::calendar::CalendarGet::new()),
            Err(crate::Error::AccountCapability { .. })
        ));
        assert_eq!(mail_request.call_count(), 1);
        assert_eq!(stub.request_count(), 0);
    }

    #[cfg(feature = "mail")]
    #[test]
    fn shared_submission_view_keeps_the_selected_account_and_checks_its_capability() {
        let mut session = serde_json::to_value(session(json!({
            "urn:ietf:params:jmap:mail": "primary",
            "urn:ietf:params:jmap:submission": "submission-primary"
        })))
        .unwrap();
        session["accounts"] = json!({
            "shared": { "name": "Shared", "isPersonal": false, "isReadOnly": false,
                "accountCapabilities": {"urn:ietf:params:jmap:mail": {}, "urn:ietf:params:jmap:submission": {"maxDelayedSend": 0}} },
            "read-only-share": { "name": "Share", "isPersonal": false, "isReadOnly": false,
                "accountCapabilities": {"urn:ietf:params:jmap:mail": {}} }
        });
        let source = stub(mail_primary(), []);
        let client = Client::with_transport(
            StubTransport {
                sent: Arc::clone(&source.sent),
                policies: Arc::new(Mutex::new(Vec::new())),
                replies: Arc::new(Mutex::new(VecDeque::new())),
            },
            serde_json::from_value(session).unwrap(),
            "https://example.test/session",
        )
        .unwrap();
        let shared = client.account::<capability::Mail>("shared").unwrap();
        let submission: crate::account::Account<_, capability::Submission> = shared.view().unwrap();
        assert_eq!(submission.id_str(), "shared");
        let mut request = shared.build();
        request.call(TestMailGet::new()).unwrap();
        request
            .call_as(crate::identity::IdentityGet::new())
            .unwrap();
        for call in serde_json::to_value(&request).unwrap()["methodCalls"]
            .as_array()
            .unwrap()
        {
            assert_eq!(call[1]["accountId"], "shared");
        }
        assert!(
            client
                .account::<capability::Mail>("read-only-share")
                .unwrap()
                .view::<capability::Submission>()
                .is_err()
        );
        assert!(
            client
                .account::<capability::Submission>("departed")
                .is_err()
        );
    }

    #[test]
    fn core_blob_copy_uses_the_explicit_account_without_a_blob_extension() {
        let stub = stub(mail_primary(), []);
        let mail = stub.client.primary_account::<capability::Mail>().unwrap();
        let core = mail.core();
        let mut request = core.build();
        request
            .call(crate::blob::copy::CopyBlobRequest::new("source").blob_id("b1"))
            .unwrap();
        let wire = serde_json::to_value(request).unwrap();
        assert_eq!(wire["methodCalls"][0][1]["accountId"], "A1");
        assert_eq!(wire["methodCalls"][0][1]["fromAccountId"], "source");
        assert_eq!(wire["using"], json!(["urn:ietf:params:jmap:core"]));
    }
}
