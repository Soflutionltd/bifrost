mod filter_wire {
    use crate::quota::query::Filter as QFilter;
    use serde_json::json;

    #[test]
    fn quota_name_filter() {
        let filter = QFilter::name("Storage");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name": "Storage"}));
    }

    #[test]
    fn quota_scope_filter() {
        let filter = QFilter::scope("account");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"scope": "account"}));
    }

    #[test]
    fn quota_resource_type_filter() {
        let filter = QFilter::resource_type("octets");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"resourceType": "octets"}));
    }
}

mod query_wire {
    use serde_json::json;

    fn wire(value: &impl serde::Serialize) -> serde_json::Value {
        serde_json::to_value(value).expect("query fragments serialize")
    }

    /// RFC 9404 Section 2.3 (`Quota/query`). The first three filters are
    /// pinned in `query_filter_serialization`; `type` and both comparators
    /// were the gap.
    #[cfg(feature = "quota")]
    #[test]
    fn quota_type_filter_and_comparator_property_names() {
        use crate::quota::query::{Comparator, Filter};

        assert_eq!(wire(&Filter::type_("Mail")), json!({"type": "Mail"}));

        assert_eq!(
            wire(&Comparator::name()),
            json!({"isAscending": true, "property": "name"})
        );
        assert_eq!(
            wire(&Comparator::used().descending()),
            json!({"isAscending": false, "property": "used"})
        );
    }
}
