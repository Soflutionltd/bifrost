mod blob_data_source_serialization {
    use crate::blob::manage::{DataSource, DataSourceBase64, DataSourceBlob, DataSourceText};
    use serde_json::json;

    #[test]
    fn data_source_text_serialization() {
        let src = DataSource::Text(DataSourceText {
            value: "Hello, world!".into(),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"data:asText": "Hello, world!"}));
    }

    #[test]
    fn data_source_base64_serialization() {
        let src = DataSource::Base64(DataSourceBase64 {
            value: "SGVsbG8=".into(),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"data:asBase64": "SGVsbG8="}));
    }

    #[test]
    fn data_source_blob_serialization() {
        let src = DataSource::Blob(DataSourceBlob {
            blob_id: "blob-123".into(),
            offset: Some(10),
            length: Some(100),
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(
            value,
            json!({"blobId": "blob-123", "offset": 10, "length": 100})
        );
    }

    #[test]
    fn data_source_blob_without_range() {
        let src = DataSource::Blob(DataSourceBlob {
            blob_id: "blob-456".into(),
            offset: None,
            length: None,
        });
        let value = serde_json::to_value(&src).unwrap();
        assert_eq!(value, json!({"blobId": "blob-456"}));
        // offset and length should be absent
        assert!(value.get("offset").is_none());
        assert!(value.get("length").is_none());
    }
}

mod blob_get_request_serialization {
    use crate::blob::manage::BlobGetRequest;
    use serde_json::json;

    #[test]
    fn blob_get_request_basic_serialization() {
        use crate::core::method::JmapMethod;
        let mut req = BlobGetRequest::new()
            .ids(["blob-1", "blob-2"])
            .properties(["data:asText", "size"]);
        req.set_account_id(&crate::core::id::AccountId::new("acct-1"));

        let value = serde_json::to_value(&req).unwrap();

        // accountId at top level
        assert_eq!(value.get("accountId"), Some(&json!("acct-1")));

        // ids as a flat array
        let ids = value.get("ids").expect("ids missing");
        assert!(ids.is_array());
        let ids_arr = ids.as_array().unwrap();
        assert_eq!(ids_arr.len(), 2);
        assert!(ids_arr.contains(&json!("blob-1")));
        assert!(ids_arr.contains(&json!("blob-2")));

        // properties at top level
        let props = value.get("properties").expect("properties missing");
        assert!(props.is_array());
        let props_arr = props.as_array().unwrap();
        assert!(props_arr.contains(&json!("data:asText")));
        assert!(props_arr.contains(&json!("size")));
    }

    #[test]
    fn blob_get_request_with_offset_and_length() {
        use crate::core::method::JmapMethod;
        let mut req = BlobGetRequest::new()
            .ids(["blob-1"])
            .offset(100)
            .length(500);
        req.set_account_id(&crate::core::id::AccountId::new("acct-1"));

        let value = serde_json::to_value(&req).unwrap();
        assert_eq!(value.get("offset"), Some(&json!(100)));
        assert_eq!(value.get("length"), Some(&json!(500)));
    }

    #[test]
    fn blob_get_request_without_optional_fields() {
        use crate::core::method::JmapMethod;
        let mut req = BlobGetRequest::new().ids(["blob-1"]);
        req.set_account_id(&crate::core::id::AccountId::new("acct-1"));

        let value = serde_json::to_value(&req).unwrap();
        // properties, offset, length should be absent
        assert!(
            value.get("properties").is_none(),
            "unset properties should be absent"
        );
        assert!(
            value.get("offset").is_none(),
            "unset offset should be absent"
        );
        assert!(
            value.get("length").is_none(),
            "unset length should be absent"
        );
    }
}

mod blob_management_wire {
    use crate::blob::manage::{
        BlobGetResponse, BlobLookupRequest, BlobLookupResponse, BlobUploadRequest, DataSource,
        DataSourceBlob,
    };
    use serde_json::json;

    #[test]
    fn upload_request_keys_creates_and_concatenates_sources() {
        let mut request = BlobUploadRequest::new();
        let first = request.create_from_text("hello", Some("text/plain"));
        let second = request.create_with_sources(
            vec![
                DataSource::Blob(DataSourceBlob {
                    blob_id: "b1".into(),
                    offset: Some(0),
                    length: Some(4),
                }),
                DataSource::Text(crate::blob::manage::DataSourceText {
                    value: "!".to_string(),
                }),
            ],
            None::<String>,
        );
        assert_eq!(first, "b0");
        assert_eq!(second, "b1");

        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value.get("create").and_then(|c| c.get("b0")),
            Some(&json!({"data": [{"data:asText": "hello"}], "type": "text/plain"}))
        );
        assert_eq!(
            value.get("create").and_then(|c| c.get("b1")),
            Some(&json!({
                "data": [
                    {"blobId": "b1", "offset": 0, "length": 4},
                    {"data:asText": "!"}
                ]
            })),
            "an omitted content type must not appear as null"
        );
    }

    #[test]
    fn get_response_splits_named_fields_from_dynamic_ones() {
        let response: BlobGetResponse = serde_json::from_str(
            r#"{"accountId":"a1","list":[{"id":"b1","size":5,"isTruncated":false,
                "data:asText":"hello","digest:sha-256":"deadbeef"}],"notFound":["b2"]}"#,
        )
        .expect("blob get decodes");

        let entry = &response.list()[0];
        assert_eq!(entry.size, Some(5));
        assert_eq!(entry.is_truncated, Some(false));
        assert_eq!(entry.data_as_text(), Some("hello"));
        assert_eq!(entry.digest("sha-256"), Some("deadbeef"));
        // The named fields must not be duplicated into the flatten map.
        assert!(!entry.properties.contains_key("size"));
        assert!(!entry.properties.contains_key("id"));
        assert_eq!(
            response.not_found().map(<[crate::core::id::BlobId]>::len),
            Some(1),
            "notFound is optional but present here"
        );
    }

    #[test]
    fn lookup_round_trip() {
        let request = BlobLookupRequest::new()
            .type_names(["Email", "CalendarEvent"])
            .ids(["b1"]);
        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value.get("typeNames"),
            Some(&json!(["Email", "CalendarEvent"]))
        );
        assert_eq!(value.get("ids"), Some(&json!(["b1"])));

        let response: BlobLookupResponse = serde_json::from_str(
            r#"{"accountId":"a1","list":[{"id":"b1","matchedIds":{"Email":["e1","e2"]}}]}"#,
        )
        .expect("lookup decodes");
        assert_eq!(
            response.list()[0].matched_ids.get("Email"),
            Some(&vec!["e1".to_string(), "e2".to_string()])
        );
        assert!(response.not_found().is_none());
    }
}
