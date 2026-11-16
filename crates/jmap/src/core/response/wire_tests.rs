use crate::Error;
use crate::core::get::GetResponse;
use crate::core::response::Response;
use crate::core::test_support::*;
use serde_json::json;

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
        "abc".into(),
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
/// reports it as `Error::EnvelopeDecode` while still having read the
/// frame's `sessionState`.
#[test]
fn frame_parts_reject_malformed_method_responses() {
    let method_responses = serde_json::value::RawValue::from_string("[[1,2]]".to_string()).unwrap();

    assert!(Response::from_frame_parts(&method_responses, None, "abc".into()).is_err());
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

    let ids = |response: crate::core::get::GetResponse<TestObj>| {
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
