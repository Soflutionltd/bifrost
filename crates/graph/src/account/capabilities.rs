use std::time::Duration;

use bifrost_types::{
    AccountCapabilities, BatchingPolicy, BlobRangeSupport, CursorFreshness, FilterRuleShape,
    MutationCapabilities, MutationConcurrency, MutationReplaySafety, PimMethodSupport,
    PushCapability, QuotaSignal, RateLimitClass, StarredFlagShape,
};

use super::PushMode;

/// Build the capability snapshot for a Graph account.
///
/// `has_shared_mailboxes` is the live emptiness of the account's
/// `shared_clients` map, not a static property of the provider. Graph CAN
/// route a send-as, but only through a mailbox it holds a `/users/{id}`
/// client for, and that map is seeded from configuration (plus, optionally,
/// delegate Autodiscover). An account with no shared mailboxes can satisfy no
/// `send_as` request at all, so the feature is absent and the flag is false.
///
/// The send path reads the SAME emptiness to pick its rejection, which is what
/// keeps the flag and the behaviour from drifting apart: empty map answers
/// `Unsupported(Send)` (the feature is absent), non-empty map answers
/// `Request(Malformed)` for an id it does not hold (the feature is present and
/// the argument is wrong). That split is stated once, on
/// `SendRequest::send_as`, and this is a reader of it rather than a second
/// copy. An earlier version of this comment justified the false flag by saying
/// every request on that path would be `Request(Malformed)` - which described
/// the bug rather than the contract.
pub(crate) fn build_capabilities(
    push_mode: PushMode,
    has_shared_mailboxes: bool,
) -> AccountCapabilities {
    AccountCapabilities {
        cursor_freshness: CursorFreshness::ServerIssued,
        blob_range: BlobRangeSupport::Conditional,
        blob_digest_pre_download: false,
        push: match push_mode {
            PushMode::GraphSubscriptions => PushCapability::WebhookOrEwsStream,
            PushMode::EwsStreaming => PushCapability::InProcess,
        },
        mutation: MutationCapabilities {
            concurrency: MutationConcurrency::ExpectedVersion {
                contact_update: true,
                event_update: true,
                contact_delete: true,
                event_delete: true,
            },
            replay_safety: MutationReplaySafety::None,
        },
        batching_policy: BatchingPolicy {
            max_items: 20,
            max_wait: Duration::from_millis(100),
            flush_on_input_close: true,
        },
        rate_limit_class: RateLimitClass::Tiered,
        quota_signal: QuotaSignal::RetryAfter,
        requires_uidvalidity_recheck: false,
        historyid_expires_after: None,
        delta_token_expires_after: None,
        pim_methods: PimMethodSupport {
            category_definitions: true,
            message_reactions: true,
            add_to_container: true,
            remove_from_container: false,
            set_keyword: false,
            set_label_membership: false,
            set_category: true,
            set_extended_property: true,
            set_importance: true,
            set_is_read: true,
            send_message: true,
            attachment_upload: false,
            host_attachment: true,
            draft_create: true,
            draft_update: true,
            draft_discard: true,
            draft_send: true,
            // Mailbox-backed Graph accounts always support deferred
            // send via the PidTagDeferredSendTime extended property.
            scheduled_send: true,
            // Graph routes a shared-mailbox send through the mailbox's
            // /users/{id} draft-create-and-send and stamps from/sender,
            // so the feature exists exactly when such a mailbox is held.
            send_as: has_shared_mailboxes,
            search: true,
            search_messages: true,
            containers_list: true,
            container_create: true,
            container_rename: true,
            container_move: true,
            container_delete: true,
            identities_list: true,
            identity_update: false,
            vacation_get: true,
            vacation_set: true,
            quota_get: false,
            thread_hydrate: true,
            message_hydrate: true,
            open_raw_rfc822: true,
            filters_list: true,
            filter_create: true,
            filter_update: true,
            filter_delete: true,
            filter_validate: true,
            address_books_list: true,
            contacts_list: true,
            contact_get: true,
            contact_create: true,
            contact_update: true,
            contact_delete: true,
            contact_search: true,
            contact_autocomplete: true,
            directory_search: true,
            directory_enumerate: true,
            directory_groups_list: true,
            directory_group_expand: true,
            calendars_list: true,
            events_in_range: true,
            event_get: true,
            event_create: true,
            event_update: true,
            event_delete: true,
            event_rsvp: true,
            event_search: true,
            event_autocomplete: true,
        },
        filter_rule_shape: FilterRuleShape::Rules,
        conveniences: bifrost_types::ConvenienceShape {
            starred: StarredFlagShape::Category,
            replied_via_keyword: false,
            replied_via_extended_property: true,
            forwarded_via_keyword: false,
            forwarded_via_extended_property: true,
            // Graph's read-receipt bit (isReadReceiptRequested) is
            // read-only; mark_mdn_sent surfaces Unsupported(MarkMdnSent).
            mdn_sent_via_keyword: false,
        },
        // Graph's foreign surfaces (delegate mailboxes, public-folder
        // pins) are configuration-driven, not open-time namespace
        // discovery, so a consumer-side rediscovery reattach buys nothing.
        discovers_foreign_namespaces_on_rediscovery: false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bifrost_types::{
        BlobRangeSupport, CursorFreshness, MutationConcurrency, MutationReplaySafety,
        PushCapability, QuotaSignal, RateLimitClass,
    };

    use super::*;

    /// Every test below that is not about `send_as` wants the snapshot of a
    /// fully configured account, so it shadows the real constructor with the
    /// shared-mailbox arm fixed. The `send_as` tests call `super::` directly
    /// and pin both arms.
    fn build_capabilities(push_mode: PushMode) -> AccountCapabilities {
        super::build_capabilities(push_mode, true)
    }

    #[test]
    fn send_as_requires_a_shared_mailbox() {
        // The flag mirrors a routing table, not a provider feature list.
        // Advertised with an empty table it would promise a send path on
        // which `send_as_unknown_mailbox` rejects EVERY request.
        for mode in [PushMode::GraphSubscriptions, PushMode::EwsStreaming] {
            assert!(!super::build_capabilities(mode, false).pim_methods.send_as);
            assert!(super::build_capabilities(mode, true).pim_methods.send_as);
        }
    }

    #[test]
    fn host_attachment_capability_true() {
        // OneDrive hosting is the bifrost replacement for ratatoskr's
        // consumer-side `supports_cloud_upload(Graph) == true`.
        for mode in [PushMode::GraphSubscriptions, PushMode::EwsStreaming] {
            assert!(build_capabilities(mode).pim_methods.host_attachment);
        }
    }

    #[test]
    fn graph_subscription_capabilities_are_out_of_process() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.blob_range, BlobRangeSupport::Conditional);
        assert_eq!(caps.push, PushCapability::WebhookOrEwsStream);
        assert!(!caps.push_in_process());
        assert_eq!(
            caps.mutation.concurrency,
            MutationConcurrency::ExpectedVersion {
                contact_update: true,
                event_update: true,
                contact_delete: true,
                event_delete: true,
            }
        );
        assert_eq!(caps.batching_policy.max_items, 20);
        assert!(caps.delta_token_expires_after.is_none());
    }

    #[test]
    fn ews_capabilities_are_in_process() {
        let caps = build_capabilities(PushMode::EwsStreaming);
        assert_eq!(caps.push, PushCapability::InProcess);
        assert!(caps.push_in_process());
    }

    #[test]
    fn cursor_freshness_is_server_issued() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.cursor_freshness, CursorFreshness::ServerIssued);
    }

    #[test]
    fn blob_digest_pre_download_is_false() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert!(!caps.blob_digest_pre_download);
    }

    #[test]
    fn mutation_replay_safety_is_none() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.mutation.replay_safety, MutationReplaySafety::None);
    }

    #[test]
    fn pim_method_support_matches_graph_surface() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert!(caps.pim_methods.add_to_container);
        assert!(!caps.pim_methods.remove_from_container);
        assert!(caps.pim_methods.set_category);
        assert!(caps.pim_methods.set_extended_property);
        assert!(caps.pim_methods.set_importance);
        // Graph's read-receipt bit is read-only.
        assert!(!caps.conveniences.mdn_sent_via_keyword);
        assert!(caps.pim_methods.send_message);
        assert!(!caps.pim_methods.attachment_upload);
        assert!(caps.pim_methods.open_raw_rfc822);
        assert!(caps.pim_methods.vacation_get);
        assert!(!caps.pim_methods.quota_get);
        assert_eq!(caps.filter_rule_shape, FilterRuleShape::Rules);
        assert!(caps.pim_methods.filters_list);
        assert!(caps.pim_methods.filter_create);
        assert!(caps.pim_methods.filter_update);
        assert!(caps.pim_methods.filter_delete);
        assert!(caps.pim_methods.filter_validate);
        assert!(caps.pim_methods.address_books_list);
        assert!(caps.pim_methods.contacts_list);
        assert!(caps.pim_methods.contact_get);
        assert!(caps.pim_methods.contact_create);
        assert!(caps.pim_methods.contact_update);
        assert!(caps.pim_methods.contact_delete);
        assert!(caps.pim_methods.contact_search);
        assert!(caps.pim_methods.contact_autocomplete);
        assert!(caps.pim_methods.directory_search);
        assert!(caps.pim_methods.directory_enumerate);
        assert!(caps.pim_methods.directory_groups_list);
        assert!(caps.pim_methods.directory_group_expand);
        assert!(caps.pim_methods.calendars_list);
        assert!(caps.pim_methods.events_in_range);
        assert!(caps.pim_methods.event_get);
        assert!(caps.pim_methods.event_create);
        assert!(caps.pim_methods.event_update);
        assert!(caps.pim_methods.event_delete);
        assert!(caps.pim_methods.event_rsvp);
        assert!(caps.pim_methods.event_search);
        assert!(caps.pim_methods.event_autocomplete);
        assert_eq!(caps.conveniences.starred, StarredFlagShape::Category);
        assert!(caps.conveniences.replied_via_extended_property);
        assert!(caps.conveniences.forwarded_via_extended_property);
    }

    #[test]
    fn batching_policy_matches_graph_batch_limit() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.batching_policy.max_items, 20);
        assert_eq!(caps.batching_policy.max_wait, Duration::from_millis(100));
        assert!(caps.batching_policy.flush_on_input_close);
    }

    #[test]
    fn rate_limit_class_is_tiered() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.rate_limit_class, RateLimitClass::Tiered);
    }

    #[test]
    fn quota_signal_is_retry_after() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert_eq!(caps.quota_signal, QuotaSignal::RetryAfter);
    }

    #[test]
    fn no_uidvalidity_recheck_required() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert!(!caps.requires_uidvalidity_recheck);
    }

    #[test]
    fn historyid_expires_after_is_unset() {
        let caps = build_capabilities(PushMode::GraphSubscriptions);
        assert!(caps.historyid_expires_after.is_none());
    }

    #[test]
    fn push_mode_does_not_affect_other_capability_fields() {
        let webhook = build_capabilities(PushMode::GraphSubscriptions);
        let ews = build_capabilities(PushMode::EwsStreaming);
        assert_eq!(webhook.cursor_freshness, ews.cursor_freshness);
        assert_eq!(webhook.blob_range, ews.blob_range);
        assert_eq!(webhook.mutation.concurrency, ews.mutation.concurrency);
        assert_eq!(webhook.mutation.replay_safety, ews.mutation.replay_safety);
        assert_eq!(
            webhook.batching_policy.max_items,
            ews.batching_policy.max_items
        );
        assert_eq!(webhook.rate_limit_class, ews.rate_limit_class);
        assert_eq!(webhook.quota_signal, ews.quota_signal);
        assert_eq!(
            webhook.delta_token_expires_after,
            ews.delta_token_expires_after
        );
    }
}
