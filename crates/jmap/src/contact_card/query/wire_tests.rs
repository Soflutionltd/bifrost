mod filter_wire {
    use crate::contact_card::query::Filter as CCFilter;
    use serde_json::json;

    #[test]
    fn contact_card_name_given_filter() {
        let filter = CCFilter::name_given("Alice");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/given": "Alice"}));
    }

    #[test]
    fn contact_card_name_surname_filter() {
        let filter = CCFilter::name_surname("Smith");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/surname": "Smith"}));
    }

    #[test]
    fn contact_card_name_surname2_filter() {
        let filter = CCFilter::name_surname2("Garcia");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"name/surname2": "Garcia"}));
    }

    #[test]
    fn contact_card_in_address_book_filter() {
        let filter = CCFilter::in_address_book("ab-123");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"inAddressBook": "ab-123"}));
    }

    #[test]
    fn contact_card_nickname_filter() {
        let filter = CCFilter::nickname("Al");
        let value = serde_json::to_value(&filter).unwrap();
        assert_eq!(value, json!({"nickname": "Al"}));
    }
}
