mod request_wire {
    use serde_json::json;

    fn wire(value: &impl serde::Serialize) -> serde_json::Value {
        serde_json::to_value(value).expect("query fragments serialize")
    }

    /// `Principal/getAvailability` (RFC 9670 Section 4.1): the request is a
    /// plain method object, so the only thing worth pinning is that every
    /// field carries its camelCase wire name and that `showDetails` is
    /// omitted rather than sent as null when unset.
    #[test]
    fn principal_get_availability_request_wire_shape() {
        use crate::principal::availability::PrincipalGetAvailabilityRequest;

        let bare = PrincipalGetAvailabilityRequest::new(
            "p-1",
            "2026-01-02T00:00:00Z",
            "2026-01-03T00:00:00Z",
        );
        assert_eq!(
            wire(&bare),
            json!({
                "accountId": "",
                "id": "p-1",
                "utcStart": "2026-01-02T00:00:00Z",
                "utcEnd": "2026-01-03T00:00:00Z",
            }),
            "showDetails must be absent, not null, when the caller did not set it"
        );

        let detailed = PrincipalGetAvailabilityRequest::new(
            "p-1",
            "2026-01-02T00:00:00Z",
            "2026-01-03T00:00:00Z",
        )
        .show_details(true);
        assert_eq!(
            wire(&detailed).get("showDetails"),
            Some(&json!(true)),
            "showDetails rides the request once set"
        );
    }
}
