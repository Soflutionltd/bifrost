use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{
    AclEntry, ListRightsResponse, MetadataEntry, MetadataResult, QuotaResource, QuotaRootResponse,
    TaggedResponse, UntaggedResponse,
};

use super::{Consumer, ConsumerContext, Finalized};
use crate::types::MailboxName;

/// Consumer for GETQUOTA (RFC 2087 Section4.2) and SETQUOTA (RFC 2087 Section4.1).
///
/// Both commands solicit a single untagged QUOTA response for the
/// requested root. Accumulates the first matching QUOTA response;
/// non-matching QUOTA responses are reclassified as events.
pub(crate) struct QuotaConsumer {
    /// The quota root we are looking for.
    root: String,
    /// The matching QUOTA response, if received.
    result: Option<Vec<QuotaResource>>,
    /// Non-matching or non-QUOTA responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl QuotaConsumer {
    pub(crate) fn new(root: String) -> Self {
        Self {
            root,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for QuotaConsumer {
    type Output = Vec<QuotaResource>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 2087 Section4.2 / Section4.1: accept only the QUOTA for the requested root.
        // RFC 3501 Section5.2: unrelated untagged responses may be interleaved.
        match resp {
            UntaggedResponse::Quota { root, resources }
                if root == self.root && self.result.is_none() =>
            {
                self.result = Some(resources);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<QuotaResource>> {
        // `buffered` is the generic `Either` catch-all: the solicited QUOTA
        // for this root lives in `result`, so nothing here is this command's
        // own output and it is surrendered on every path.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }
        let Some(resources) = self.result else {
            return Finalized::failure(
                Error::ProtocolMissing(format!(
                    "server sent OK but no QUOTA response for root '{}' \
                     (RFC 2087 Section 4.2)",
                    self.root,
                )),
                self.buffered,
            );
        };
        Finalized::success(resources, self.buffered)
    }
}

/// Consumer for GETQUOTAROOT (RFC 2087 Section4.3 / RFC 9208 Section4.1.2).
///
/// Accumulates the QUOTAROOT response (root names) and all QUOTA
/// responses (resource triplets). Correlates QUOTA responses to the
/// roots listed in the QUOTAROOT response in `finalize`.
pub(crate) struct QuotaRootConsumer {
    /// The mailbox argument, for correlation.
    mailbox: MailboxName,
    /// The QUOTAROOT response roots, if received.
    roots: Option<Vec<String>>,
    /// All QUOTA responses received.
    quotas: Vec<(String, Vec<QuotaResource>)>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl QuotaRootConsumer {
    pub(crate) fn new(mailbox: MailboxName) -> Self {
        Self {
            mailbox,
            roots: None,
            quotas: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for QuotaRootConsumer {
    type Output = QuotaRootResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 2087 Section 4.3: QUOTAROOT response correlates by mailbox
            // address, preserving noncanonical wire identity and RFC 3501
            // Section 5.1's case-insensitive INBOX rule.
            UntaggedResponse::QuotaRoot { mailbox, roots }
                if self.mailbox.matches_mailbox(&mailbox) && self.roots.is_none() =>
            {
                self.roots = Some(roots);
            }
            // RFC 2087 Section4.3: QUOTA responses for the returned roots.
            // We consume all QUOTA here. Filtering against the root
            // list happens in finalize, where non-matching ones are
            // reclassified as events.
            UntaggedResponse::Quota { root, resources } => {
                self.quotas.push((root, resources));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<QuotaRootResponse> {
        // `buffered` holds only responses this command never solicited, so it
        // is surrendered on every path, failure included.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }

        let Some(roots) = self.roots else {
            return Finalized::failure(
                Error::ProtocolMissing(format!(
                    "server sent OK but no QUOTAROOT response for mailbox '{}' \
                     (RFC 2087 Section 4.3)",
                    self.mailbox,
                )),
                self.buffered,
            );
        };

        // Partition QUOTA responses: matching roots are the result,
        // non-matching are reclassified as events.
        let mut resources: Vec<(String, Vec<QuotaResource>)> = Vec::new();
        let mut buffered = self.buffered;
        for (root, res) in self.quotas {
            if roots.iter().any(|expected| expected == &root) {
                resources.push((root, res));
            } else {
                buffered.push(UntaggedResponse::Quota {
                    root,
                    resources: res,
                });
            }
        }

        if roots.is_empty() {
            return Finalized::success(QuotaRootResponse { roots, resources }, buffered);
        }
        if resources.is_empty() {
            // Real `Error` in `output` - the driver classifies it before the
            // result is published. `buffered` still goes out; the only thing
            // lost is the (empty) resource partition.
            return Finalized::failure(
                Error::ProtocolMissing(format!(
                    "server sent OK but no QUOTA response for QUOTAROOT mailbox \
                     '{}' (RFC 2087 Section 4.3)",
                    self.mailbox,
                )),
                buffered,
            );
        }

        Finalized::success(QuotaRootResponse { roots, resources }, buffered)
    }
}

/// Consumer for GETACL (RFC 4314 Section3.3).
///
/// Accumulates the ACL response for the requested mailbox.
pub(crate) struct AclConsumer {
    /// The mailbox argument, for correlation.
    mailbox: MailboxName,
    /// The matching ACL entries, if received.
    result: Option<Vec<AclEntry>>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl AclConsumer {
    pub(crate) fn new(mailbox: MailboxName) -> Self {
        Self {
            mailbox,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for AclConsumer {
    type Output = Vec<AclEntry>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section 3.3 / RFC 3501 Section 5.2: correlate by mailbox
        // address, preserving noncanonical wire identity.
        match resp {
            UntaggedResponse::Acl { mailbox, entries }
                if self.mailbox.matches_mailbox(&mailbox) && self.result.is_none() =>
            {
                self.result = Some(entries);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<Vec<AclEntry>> {
        // The solicited ACL lives in `result`; `buffered` is the generic
        // `Either` catch-all and is surrendered on every path.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }
        let Some(entries) = self.result else {
            return Finalized::failure(
                Error::ProtocolMissing(format!(
                    "server sent OK but no ACL response for mailbox '{}' \
                     (RFC 4314 Section 3.3)",
                    self.mailbox,
                )),
                self.buffered,
            );
        };
        Finalized::success(entries, self.buffered)
    }
}

/// Consumer for LISTRIGHTS (RFC 4314 Section3.4).
///
/// Accumulates the LISTRIGHTS response for the requested mailbox and
/// identifier.
pub(crate) struct ListRightsConsumer {
    /// The mailbox argument, for correlation.
    mailbox: MailboxName,
    /// The identifier argument, for correlation.
    identifier: String,
    /// The matching LISTRIGHTS response, if received.
    result: Option<ListRightsResponse>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl ListRightsConsumer {
    pub(crate) fn new(mailbox: MailboxName, identifier: String) -> Self {
        Self {
            mailbox,
            identifier,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListRightsConsumer {
    type Output = ListRightsResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section3.4: correlate by mailbox AND identifier.
        match resp {
            UntaggedResponse::ListRights {
                mailbox,
                identifier,
                required,
                optional,
            } if self.mailbox.matches_mailbox(&mailbox)
                && identifier == self.identifier
                && self.result.is_none() =>
            {
                self.result = Some(ListRightsResponse { required, optional });
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<ListRightsResponse> {
        // The solicited LISTRIGHTS lives in `result`; `buffered` is the
        // generic `Either` catch-all and is surrendered on every path.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }
        let Some(result) = self.result else {
            return Finalized::failure(
                Error::ProtocolMissing(format!(
                    "server sent OK but no LISTRIGHTS response for mailbox '{}' \
                     and identifier '{}' (RFC 4314 Section 3.4)",
                    self.mailbox, self.identifier,
                )),
                self.buffered,
            );
        };
        Finalized::success(result, self.buffered)
    }
}

/// Consumer for MYRIGHTS (RFC 4314 Section3.5).
///
/// Accumulates the MYRIGHTS response for the requested mailbox.
pub(crate) struct MyRightsConsumer {
    /// The mailbox argument, for correlation.
    mailbox: MailboxName,
    /// The matching MYRIGHTS rights string, if received.
    result: Option<String>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl MyRightsConsumer {
    pub(crate) fn new(mailbox: MailboxName) -> Self {
        Self {
            mailbox,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for MyRightsConsumer {
    type Output = String;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section3.5: correlate by mailbox.
        match resp {
            UntaggedResponse::MyRights { mailbox, rights }
                if self.mailbox.matches_mailbox(&mailbox) && self.result.is_none() =>
            {
                self.result = Some(rights);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<String> {
        // The solicited MYRIGHTS lives in `result`; `buffered` is the generic
        // `Either` catch-all and is surrendered on every path.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }
        let Some(rights) = self.result else {
            return Finalized::failure(
                Error::ProtocolMissing(format!(
                    "server sent OK but no MYRIGHTS response for mailbox '{}' \
                     (RFC 4314 Section 3.5)",
                    self.mailbox,
                )),
                self.buffered,
            );
        };
        Finalized::success(rights, self.buffered)
    }
}

/// Consumer for GETMETADATA (RFC 5464 Section4.2).
///
/// Accumulates same-mailbox METADATA responses and tracks NOTIFY
/// ambiguity via per-response `notify_snapshot`. Different-mailbox
/// METADATA responses are reclassified as events.
///
/// RFC 5465 Section5.6-5.8: when NOTIFY metadata is active, the protocol
/// provides no marker to distinguish solicited METADATA from unsolicited
/// NOTIFY METADATA for the same mailbox. The consumer exposes this via
/// `MetadataResult::notify_ambiguity`.
pub(crate) struct MetadataConsumer {
    /// The mailbox argument, for correlation.
    mailbox: MailboxName,
    /// Accumulated metadata entries from same-mailbox responses.
    entries: Vec<MetadataEntry>,
    /// Whether any same-mailbox response arrived while NOTIFY metadata
    /// was active, making the result potentially ambiguous.
    notify_ambiguity: bool,
    /// Whether we saw at least one matching METADATA response.
    saw_matching: bool,
    /// Different-mailbox METADATA and non-METADATA responses.
    buffered: Vec<UntaggedResponse>,
}

impl MetadataConsumer {
    pub(crate) fn new(mailbox: MailboxName) -> Self {
        Self {
            mailbox,
            entries: Vec::new(),
            notify_ambiguity: false,
            saw_matching: false,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for MetadataConsumer {
    type Output = MetadataResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // Same-mailbox METADATA: accumulate entries.
            // RFC 5464 Section4.2: GETMETADATA can produce multiple METADATA
            // response lines for the same mailbox.
            UntaggedResponse::Metadata { mailbox, entries }
                if self.mailbox.matches_mailbox(&mailbox) =>
            {
                // RFC 5465 Section5.6-5.8: if NOTIFY metadata was active when
                // this response was generated, the result is ambiguous.
                // Some entries may be from interleaved NOTIFY events.
                // Post-NOTIFICATIONOVERFLOW, apply_side_effects clears the
                // metadata flag, so notify_snapshot.metadata will be false
                // for post-overflow responses (they are unambiguously
                // solicited per RFC 5465 Section5.8).
                if notify_snapshot.metadata {
                    self.notify_ambiguity = true;
                }
                self.saw_matching = true;
                self.entries.extend(entries);
            }
            // Different-mailbox METADATA or non-METADATA: reclassify.
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<MetadataResult> {
        // On failure the `entries` accumulator is dropped: same-mailbox
        // METADATA is wire-identical between the solicited reply and a NOTIFY
        // event (RFC 5465 Section5.6-5.7, RFC 5464 Section4.2), so re-emitting
        // it would leak potentially-solicited data into the NOTIFY event
        // channel. `buffered` is a different set - different-mailbox METADATA
        // and non-METADATA `Either` responses, none of which this command
        // solicited - so it is surrendered.
        if let Err(e) = tagged.require_ok() {
            return Finalized::failure(e, self.buffered);
        }

        if !self.saw_matching {
            return Finalized::failure(
                Error::ProtocolMissing(
                    "server completed GETMETADATA without the required METADATA \
                     response for the requested mailbox (RFC 5464 Section 4.2)"
                        .into(),
                ),
                self.buffered,
            );
        }

        Finalized::success(
            MetadataResult {
                entries: self.entries,
                notify_ambiguity: self.notify_ambiguity,
            },
            self.buffered,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::dispatch::ConsumerContext;
    use crate::types::response::StatusKind;

    #[test]
    fn quota_root_correlation_uses_wire_identity() {
        let canonical = MailboxName::new("a").expect("valid mailbox");
        let noncanonical = MailboxName::from_wire("a".into(), b"&AGE-", false);
        let mut consumer = QuotaRootConsumer::new(canonical.clone());
        let context = ConsumerContext {
            capabilities: &[],
            enabled: &[],
            command_target: Some(&canonical),
            command_tag: "A001",
        };
        consumer.on_response(
            UntaggedResponse::QuotaRoot {
                mailbox: noncanonical,
                roots: Vec::new(),
            },
            NotifyFlags::default(),
            &context,
        );
        consumer.on_response(
            UntaggedResponse::QuotaRoot {
                mailbox: canonical.clone(),
                roots: vec!["user".into()],
            },
            NotifyFlags::default(),
            &context,
        );
        consumer.on_response(
            UntaggedResponse::Quota {
                root: "user".into(),
                resources: Vec::new(),
            },
            NotifyFlags::default(),
            &context,
        );

        let finalized = Box::new(consumer).finalize(
            TaggedResponse {
                tag: "A001".into(),
                status: StatusKind::Ok,
                code: None,
                text: String::new(),
            },
            &context,
        );
        let roots = finalized.output.expect("GETQUOTAROOT succeeds");
        assert_eq!(roots.roots, vec!["user"]);
    }
}
