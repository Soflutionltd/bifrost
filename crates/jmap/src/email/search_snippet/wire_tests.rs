mod object_wire {
    use serde_json::json;

    #[test]
    fn search_snippet_request_shape() {
        let request = crate::email::search_snippet::SearchSnippetGetRequest::new()
            .filter(crate::email::query::Filter::text("needle"))
            .email_ids(["e1", "e2"]);
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value.get("filter"), Some(&json!({"text": "needle"})));
        assert_eq!(value.get("emailIds"), Some(&json!(["e1", "e2"])));
        assert!(value.get("#emailIds").is_none());
    }
}
