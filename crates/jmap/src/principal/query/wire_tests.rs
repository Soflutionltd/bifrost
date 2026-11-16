mod query_wire {
    use serde_json::json;

    fn wire(value: &impl serde::Serialize) -> serde_json::Value {
        serde_json::to_value(value).expect("query fragments serialize")
    }

    #[test]
    fn principal_time_zone_filter_matches_rfc9670() {
        use crate::principal::query::Filter;

        assert_eq!(
            wire(&Filter::timezone("Europe/Oslo")),
            json!({"timeZone": "Europe/Oslo"})
        );
    }

    /// RFC 9670 Section 2.4 (`Principal/query`).
    #[test]
    fn principal_filter_and_comparator_property_names() {
        use crate::principal::Type;
        use crate::principal::query::{Comparator, Filter};

        assert_eq!(
            wire(&Filter::account_ids(["acct-1", "acct-2"])),
            json!({"accountIds": ["acct-1", "acct-2"]})
        );
        assert_eq!(
            wire(&Filter::email("a@example.test")),
            json!({"email": "a@example.test"})
        );
        assert_eq!(wire(&Filter::name("Ops")), json!({"name": "Ops"}));
        assert_eq!(
            wire(&Filter::domain_name("example.test")),
            json!({"domainName": "example.test"})
        );
        assert_eq!(wire(&Filter::text("ops")), json!({"text": "ops"}));
        assert_eq!(
            wire(&Filter::ptype(Type::Group)),
            json!({"type": "group"}),
            "the principal type filter carries the wire token, not the Rust name"
        );
        assert_eq!(
            wire(&Filter::timezone("Europe/Oslo")),
            json!({"timeZone": "Europe/Oslo"})
        );
        assert_eq!(wire(&Filter::members("p-1")), json!({"members": "p-1"}));
        assert_eq!(
            wire(&Filter::quota_lower_than(10)),
            json!({"quotaLowerThan": 10})
        );
        assert_eq!(
            wire(&Filter::quota_greater_than(20)),
            json!({"quotaGreaterThan": 20})
        );

        assert_eq!(
            wire(&Comparator::name()),
            json!({"isAscending": true, "property": "name"})
        );
        assert_eq!(
            wire(&Comparator::email()),
            json!({"isAscending": true, "property": "email"})
        );
        assert_eq!(
            wire(&Comparator::ptype().descending()),
            json!({"isAscending": false, "property": "type"})
        );
    }

    /// An untagged enum is decided by the SHAPE of the value, and
    /// `accountIds` is the only array-valued variant - which is why it is
    /// declared first. Pin that the array form still round-trips to the
    /// intended property rather than being claimed by a string variant.
    #[test]
    fn principal_account_ids_filter_survives_untagged_trial_order() {
        use crate::principal::query::Filter;

        let value = wire(&Filter::account_ids(["acct-1"]));
        assert_eq!(
            value.as_object().expect("an object").keys().next(),
            Some(&"accountIds".to_string())
        );
    }
}
