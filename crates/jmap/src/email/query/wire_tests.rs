mod email_query_wire {
    use crate::email::query::{Comparator, Filter};
    use serde_json::json;

    fn wire(filter: Filter) -> serde_json::Value {
        serde_json::to_value(&filter).unwrap()
    }

    #[test]
    fn filter_condition_property_names() {
        assert_eq!(wire(Filter::in_mailbox("mb1")), json!({"inMailbox": "mb1"}));
        assert_eq!(
            wire(Filter::in_mailbox_other_than(["a", "b"])),
            json!({"inMailboxOtherThan": ["a", "b"]})
        );
        assert_eq!(
            wire(Filter::has_keyword(
                crate::email::keyword::Keyword::parse("$seen").unwrap()
            )),
            json!({"hasKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::not_keyword(
                crate::email::keyword::Keyword::parse("$seen").unwrap()
            )),
            json!({"notKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::all_in_thread_have_keyword(
                crate::email::keyword::Keyword::parse("$seen").unwrap()
            )),
            json!({"allInThreadHaveKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::some_in_thread_have_keyword(
                crate::email::keyword::Keyword::parse("$seen").unwrap()
            )),
            json!({"someInThreadHaveKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::none_in_thread_have_keyword(
                crate::email::keyword::Keyword::parse("$seen").unwrap()
            )),
            json!({"noneInThreadHaveKeyword": "$seen"})
        );
        assert_eq!(
            wire(Filter::has_attachment(true)),
            json!({"hasAttachment": true})
        );
        assert_eq!(wire(Filter::min_size(10)), json!({"minSize": 10}));
        assert_eq!(wire(Filter::max_size(20)), json!({"maxSize": 20}));
        assert_eq!(wire(Filter::text("q")), json!({"text": "q"}));
        assert_eq!(
            wire(Filter::from("a@example.com")),
            json!({"from": "a@example.com"})
        );
        assert_eq!(
            wire(Filter::to("a@example.com")),
            json!({"to": "a@example.com"})
        );
        assert_eq!(
            wire(Filter::cc("a@example.com")),
            json!({"cc": "a@example.com"})
        );
        assert_eq!(
            wire(Filter::bcc("a@example.com")),
            json!({"bcc": "a@example.com"})
        );
        assert_eq!(wire(Filter::subject("s")), json!({"subject": "s"}));
        assert_eq!(wire(Filter::body("b")), json!({"body": "b"}));
    }

    #[test]
    fn header_filter_is_a_one_or_two_element_array() {
        assert_eq!(
            wire(Filter::header("X-Vendor", None::<String>)),
            json!({"header": ["X-Vendor"]})
        );
        assert_eq!(
            wire(Filter::header("X-Vendor", Some("v"))),
            json!({"header": ["X-Vendor", "v"]})
        );
    }

    #[test]
    fn date_filters_serialise_as_jmap_utcdate() {
        // RFC 8620 s1.4 UTCDate: "YYYY-MM-DDTHH:MM:SSZ", no offset, no
        // fractional seconds for a whole-second instant.
        assert_eq!(
            wire(Filter::before(jiff::Timestamp::UNIX_EPOCH)),
            json!({"before": "1970-01-01T00:00:00Z"})
        );
        assert_eq!(
            wire(Filter::after(
                jiff::Timestamp::from_second(1_767_323_045).unwrap()
            )),
            json!({"after": "2026-01-02T03:04:05Z"})
        );
    }

    #[test]
    fn comparators_flatten_the_property_tag() {
        assert_eq!(
            serde_json::to_value(Comparator::received_at().descending()).unwrap(),
            json!({"isAscending": false, "property": "receivedAt"})
        );
        assert_eq!(
            serde_json::to_value(Comparator::has_keyword(
                crate::email::keyword::Keyword::parse("$flagged").unwrap()
            ))
            .unwrap(),
            json!({"isAscending": true, "property": "hasKeyword", "keyword": "$flagged"})
        );
        assert_eq!(
            serde_json::to_value(Comparator::size().collation("i;ascii-casemap".to_string()))
                .unwrap(),
            json!({"isAscending": true, "collation": "i;ascii-casemap", "property": "size"})
        );
    }

    #[test]
    fn query_request_carries_collapse_threads_and_paging() {
        let query = crate::email::EmailQuery::new()
            .filter(Filter::in_mailbox("mb1"))
            .sort([Comparator::received_at().descending()])
            .position(20)
            .limit(10)
            .calculate_total(true)
            .collapse_threads(true);
        let value = serde_json::to_value(&query).unwrap();

        assert_eq!(value.get("filter"), Some(&json!({"inMailbox": "mb1"})));
        assert_eq!(value.get("position"), Some(&json!(20)));
        assert_eq!(value.get("limit"), Some(&json!(10)));
        assert_eq!(value.get("calculateTotal"), Some(&json!(true)));
        assert_eq!(value.get("collapseThreads"), Some(&json!(true)));
        // Unset paging arguments must not appear at all.
        assert!(value.get("anchor").is_none());
        assert!(value.get("anchorOffset").is_none());
    }

    #[test]
    fn filter_operators_nest() {
        use crate::core::query::{Filter as CoreFilter, Operator};
        let in_mailbox: CoreFilter<Filter> = CoreFilter::FilterCondition(Filter::in_mailbox("mb1"));
        let has_keyword: CoreFilter<Filter> = CoreFilter::FilterCondition(Filter::has_keyword(
            crate::email::keyword::Keyword::parse("$seen").unwrap(),
        ));
        let negated: CoreFilter<Filter> = CoreFilter::not([has_keyword]);
        let combined: CoreFilter<Filter> = CoreFilter::and([in_mailbox, negated]);
        let value = serde_json::to_value(&combined).unwrap();
        assert_eq!(
            value,
            json!({
                "operator": "AND",
                "conditions": [
                    {"inMailbox": "mb1"},
                    {"operator": "NOT", "conditions": [{"hasKeyword": "$seen"}]}
                ]
            })
        );
        assert_eq!(
            serde_json::to_value(Operator::Or).unwrap(),
            json!("OR"),
            "operator names are upper-case in RFC 8620 s5.5"
        );
    }
}
