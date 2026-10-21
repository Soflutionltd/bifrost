use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, ConvenienceShape, CursorFreshness,
    FilterRuleShape, MutationCapabilities, MutationConcurrency, MutationReplaySafety,
    PimMethodSupport, PushCapability, QuotaSignal, RateLimitClass,
};

pub(crate) fn carddav_capabilities() -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::Hybrid,
        blob_range: BlobRangeSupport::No,
        blob_digest_pre_download: false,
        push: PushCapability::None,
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::ExpectedVersion {
                contact_update: true,
                event_update: false,
            },
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 0,
            max_wait: Default::default(),
            flush_on_input_close: false,
        },
        rate_limit_class: RateLimitClass::Generous,
        quota_signal: QuotaSignal::Implicit,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            address_books_list: true,
            contacts_list: true,
            contact_get: true,
            contact_create: true,
            contact_update: true,
            contact_delete: true,
            contact_search: true,
            contact_autocomplete: true,
            ..PimMethodSupport::default()
        },
        filter_rule_shape: FilterRuleShape::None,
        conveniences: ConvenienceShape::default(),
        // A share arriving in the addressbook home set is presented as
        // an ordinary own collection; this crate models no foreign
        // namespace, so there is nothing a rediscovery reopen could
        // surface under that label.
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

#[cfg(test)]
mod tests {
    use bifrost_types::{
        BlobRangeSupport, CursorFreshness, MutationConcurrency, MutationReplaySafety,
        PushCapability,
    };

    use super::*;

    #[test]
    fn capability_builder_matches_contact_only_shape() {
        let caps = carddav_capabilities();
        assert_eq!(caps.cursor_freshness, CursorFreshness::Hybrid);
        assert_eq!(caps.batching_policy.max_items, 0);
        assert!(!caps.batching_policy.flush_on_input_close);
        assert_eq!(caps.blob_range, BlobRangeSupport::No);
        assert_eq!(caps.push, PushCapability::None);
        assert!(!caps.push_in_process());
        assert_eq!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                contact_update: true,
                event_update: false
            }
        );
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
        assert!(caps.pim_methods.address_books_list);
        assert!(caps.pim_methods.contacts_list);
        assert!(caps.pim_methods.contact_get);
        assert!(caps.pim_methods.contact_create);
        assert!(caps.pim_methods.contact_update);
        assert!(caps.pim_methods.contact_delete);
        assert!(caps.pim_methods.contact_search);
        assert!(caps.pim_methods.contact_autocomplete);
        assert!(!caps.pim_methods.search);
        assert!(!caps.pim_methods.filters_list);
    }
}
