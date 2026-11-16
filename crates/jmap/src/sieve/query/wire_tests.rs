mod query_wire {
    use serde_json::json;

    fn wire(value: &impl serde::Serialize) -> serde_json::Value {
        serde_json::to_value(value).expect("query fragments serialize")
    }

    /// draft-ietf-jmap-sieve Section 2.3 (`SieveScript/query`).
    #[cfg(feature = "mail")]
    #[test]
    fn sieve_script_filter_and_comparator_property_names() {
        use crate::sieve::query::{Comparator, Filter};

        assert_eq!(wire(&Filter::name("vacation")), json!({"name": "vacation"}));
        assert_eq!(wire(&Filter::is_active(true)), json!({"isActive": true}));

        assert_eq!(
            wire(&Comparator::name()),
            json!({"isAscending": true, "property": "name"})
        );
        assert_eq!(
            wire(&Comparator::is_active().descending()),
            json!({"isAscending": false, "property": "isActive"})
        );
    }
}
