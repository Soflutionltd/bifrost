use crate::core::get::GetResponse;
use crate::core::test_support::*;
use serde_json::json;

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
