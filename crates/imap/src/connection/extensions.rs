#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // COMPRESS (RFC 4978)
    // -----------------------------------------------------------------------

    /// COMPRESS DEFLATE  -  negotiate and activate compression (RFC 4978 Section 4).
    ///
    /// Sends the `COMPRESS DEFLATE` command. If the server accepts, wraps the
    /// current transport stream in a deflate compression layer using raw deflate
    /// (RFC 1951) as required by RFC 4978 Section 3.
    ///
    /// After successful return, all subsequent I/O on this connection is
    /// compressed. Any already-buffered post-OK compressed bytes are preserved
    /// so the first compressed server response is not lost.
    ///
    /// Requires the `COMPRESS=DEFLATE` capability. RFC 4978 Section 3 permits
    /// COMPRESS to be negotiated either before or after TLS; the effective
    /// on-the-wire layering is still compression before encryption.
    ///
    /// The upgrade is atomic via the `Poisoned` sentinel pattern (I9, I10)
    ///  -  handled entirely by the driver task.
    pub async fn compress(&self, timeout: Duration) -> Result<(), Error> {
        // RFC 4978 Section 4: COMPRESS is valid in Authenticated and Selected states.
        self.require_state(crate::types::CommandKind::Compress)?;

        // Check COMPRESS=DEFLATE capability from the snapshot.
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::CompressDeflate) {
                return Err(Error::MissingCapability("COMPRESS=DEFLATE".into()));
            }
        }

        self.submit_upgrade(
            driver::UpgradePayload::Compress,
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    // -----------------------------------------------------------------------
    // NOTIFY (RFC 5465)
    // -----------------------------------------------------------------------

    /// NOTIFY SET  -  register interest in mailbox and message events
    /// (RFC 5465 Section 3).
    ///
    /// Replaces any previous NOTIFY configuration. A successful NOTIFY SET
    /// has an implicit NOOP effect: the server flushes any pending changes
    /// to the selected mailbox before the tagged OK (RFC 5465 Section 3).
    ///
    /// Notifications arrive as typed events on the event queue and can be
    /// retrieved via the event receiver.
    ///
    /// # Errors
    ///
    /// - [`Error::MissingCapability`] if the server does not advertise `NOTIFY`.
    /// - [`Error::InvalidState`] if the command is issued in an invalid state
    ///   ([`Error::Closed`] once the session is in Logout).
    /// - [`Error::No`] if the server rejects the request (e.g. `[BADEVENT]`
    ///   for unsupported event types, RFC 5465 Section 5).
    pub async fn notify_set(
        &self,
        params: NotifySetParams,
        timeout: Duration,
    ) -> Result<(), Error> {
        // RFC 5465 Section 3: NOTIFY is a `command-auth` extension, valid
        // in Authenticated or Selected state.
        self.require_state(crate::types::CommandKind::NotifySet)?;
        self.require_notify()?;

        // RFC 6855 Section 6: UTF8=ONLY servers reject commands that might
        // require UTF-8 support until ENABLE UTF8=ACCEPT. Only check when
        // the command actually carries mailbox strings (Subtree/Mailboxes
        // filters). Atom-only registrations like `(selected (MessageNew))`
        // contain no mailbox strings and are valid without ENABLE.
        let has_mailbox_strings = params.event_groups.iter().any(|g| {
            matches!(
                g.filter,
                MailboxFilter::Subtree(_) | MailboxFilter::Mailboxes(_)
            )
        });
        if has_mailbox_strings {
            self.check_utf8_only_enforced()?;
        }

        let cmd = Command::NotifySet(params);
        let consumer = super::dispatch::NotifySetConsumer::default();
        // Driver sets in_notify_set before sending; apply_tagged updates
        // notify flags on tagged OK. NOTIFICATIONOVERFLOW clears them.
        // NO/BAD arrive as the ordinary `Err` of this call: they ride
        // `Finalized::output`, so the consumer's `reclassified_as_events` is
        // emitted on that path too, and the caller no longer unwraps a second
        // inner `Result` to find out.
        let overflow = self
            .submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await?;

        if overflow {
            // RFC 5465 Section 5.8: server cannot keep up. Notify flags
            // already cleared by apply_side_effects in the driver.
            warn!(
                "NOTIFICATIONOVERFLOW during NOTIFY SET  -  registration \
                 cleared (RFC 5465 Section 5.8)"
            );
        }

        debug!("NOTIFY SET completed (RFC 5465)");
        Ok(())
    }

    /// NOTIFY NONE  -  cancel all event subscriptions (RFC 5465 Section 3).
    ///
    /// Reverts to baseline IMAP behavior where the server only sends
    /// notifications during command processing, and only for the selected
    /// mailbox.
    ///
    /// # Errors
    ///
    /// - [`Error::MissingCapability`] if the server does not advertise `NOTIFY`.
    /// - [`Error::InvalidState`] if the command is issued in an invalid state
    ///   ([`Error::Closed`] once the session is in Logout).
    pub async fn notify_none(&self, timeout: Duration) -> Result<(), Error> {
        // RFC 5465 Section 3: NOTIFY is a `command-auth` extension.
        self.require_state(crate::types::CommandKind::NotifyNone)?;
        self.require_notify()?;

        let cmd = Command::NotifyNone;
        // Driver sets in_notify_set(default) before sending;
        // apply_tagged resets notify flags on tagged OK.
        self.submit_regular(
            cmd,
            super::dispatch::TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await?;

        debug!("NOTIFY NONE  -  notifications disabled (RFC 5465)");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ENABLE (RFC 5161 Section 3 / RFC 9051 Section 6.3.1)
    // -----------------------------------------------------------------------

    /// ENABLE UTF8=ACCEPT  -  negotiate UTF-8 support (RFC 6855 Section 3).
    ///
    /// Convenience wrapper around [`enable`](Self::enable) that sends
    /// `ENABLE UTF8=ACCEPT` and returns `true` if the server confirmed
    /// the extension, `false` if the server did not include it in its
    /// `ENABLED` response.
    ///
    /// Must be issued in authenticated state before SELECT/EXAMINE
    /// (RFC 5161 Section 2).
    pub async fn enable_utf8(&self, timeout: Duration) -> Result<bool, Error> {
        let enabled = self.enable(&["UTF8=ACCEPT"], timeout).await?;
        Ok(enabled
            .iter()
            .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT")))
    }

    /// ENABLE  -  request the server to activate one or more IMAP extensions
    /// (RFC 5161 Section 3 / RFC 9051 Section 6.3.1).
    ///
    /// Sends the ENABLE command with the given capability atoms and returns
    /// the list of extensions the server actually activated (per the
    /// untagged `ENABLED` response).
    ///
    /// # State requirement
    ///
    /// RFC 5161 Section 3.1: ENABLE is valid only in the Authenticated state,
    /// before any mailbox is selected. Attempting to ENABLE in the Selected
    /// state, or after a mailbox was selected and then closed or unselected,
    /// returns [`Error::InvalidState`].
    ///
    /// # Ordering constraint
    ///
    /// RFC 5161 Section 2.2: ENABLE MUST be issued before any command that
    /// depends on the extension (e.g. SELECT with QRESYNC requires
    /// `ENABLE QRESYNC` first).
    ///
    /// # Return value
    ///
    /// Returns only the extensions the server activated in this call. The
    /// server may return a subset of the requested capabilities. Already-
    /// enabled extensions are included in the cached state but may not
    /// appear in the per-call return value.
    ///
    /// # Snapshot timing
    ///
    /// The `enabled` list in the cached state snapshot is updated by the
    /// driver after the tagged OK is processed.
    pub async fn enable(
        &self,
        capabilities: &[&str],
        timeout: Duration,
    ) -> Result<Vec<String>, Error> {
        // RFC 5161 Section 2: ENABLE is only valid in Authenticated state.
        self.require_state(crate::types::CommandKind::Enable)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Enable) {
                return Err(Error::MissingCapability("ENABLE".into()));
            }
        }
        let cmd = Command::Enable {
            capabilities: capabilities.iter().map(|c| (*c).to_owned()).collect(),
        };
        let consumer = super::dispatch::EnableConsumer::default();
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    // -----------------------------------------------------------------------
    // NAMESPACE (RFC 2342 / RFC 9051 Section 6.3.11)
    // -----------------------------------------------------------------------

    /// NAMESPACE  -  query the server's namespace configuration
    /// (RFC 2342 Section 4 / RFC 9051 Section 6.3.11).
    ///
    /// Returns the server's personal, other-users, and shared namespace
    /// descriptors.
    ///
    /// Requires the `NAMESPACE` capability or an `IMAP4rev2` connection
    /// (RFC 9051 folds NAMESPACE into the base protocol).
    pub async fn namespace(&self, timeout: Duration) -> Result<NamespaceResponse, Error> {
        self.require_state(crate::types::CommandKind::Namespace)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Namespace) {
                return Err(Error::MissingCapability("NAMESPACE".into()));
            }
        }
        self.submit_regular(
            Command::Namespace,
            super::dispatch::NamespaceConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    // -----------------------------------------------------------------------
    // ID (RFC 2971)
    // -----------------------------------------------------------------------

    /// ID  -  exchange client/server identification (RFC 2971 Section 3.1).
    ///
    /// Sends client identification parameters to the server and returns
    /// the server's identification parameters. Each parameter is a
    /// `(field_name, value)` pair; a `None` value encodes as `NIL` on the
    /// wire (RFC 2971 Section 3.1).
    ///
    /// Valid in any state (RFC 2971 Section 3.1).
    pub async fn id(
        &self,
        params: &[(&str, Option<&str>)],
        timeout: Duration,
    ) -> Result<Vec<(String, Option<String>)>, Error> {
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Id) {
                return Err(Error::MissingCapability("ID".into()));
            }
        }
        let cmd = Command::Id(
            params
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.map(str::to_owned)))
                .collect(),
        );
        self.submit_regular(
            cmd,
            super::dispatch::IdConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    // -----------------------------------------------------------------------
    // GETMETADATA / SETMETADATA (RFC 5464)
    // -----------------------------------------------------------------------

    /// GETMETADATA  -  retrieve mailbox or server metadata
    /// (RFC 5464 Section 4.2).
    ///
    /// Fetches metadata entries for the given mailbox. Use an empty string
    /// `""` for server-level metadata (RFC 5464 Section 4.2).
    ///
    /// `max_size` limits the size of returned values in bytes
    /// (RFC 5464 Section 4.2.2). `depth` controls entry hierarchy
    /// traversal: `"0"` (default), `"1"`, or `"infinity"`
    /// (RFC 5464 Section 4.2.2).
    ///
    /// Requires `METADATA` or `METADATA-SERVER` capability
    /// (RFC 5464 Section 1).
    pub async fn get_metadata(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        entries: &[&str],
        max_size: Option<u64>,
        depth: Option<&str>,
        timeout: Duration,
    ) -> Result<MetadataResult, Error> {
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::GetMetadata)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Metadata)
                && !super::auth::snapshot_supports(&snap, &Capability::MetadataServer)
            {
                return Err(Error::MissingCapability("METADATA".into()));
            }
        }
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = Command::GetMetadata {
            mailbox: mailbox_name.clone(),
            entries: entries.iter().map(|e| (*e).to_owned()).collect(),
            max_size,
            depth: depth.map(str::to_owned),
        };
        let consumer = super::dispatch::MetadataConsumer::new(mailbox_name.clone());
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    /// SETMETADATA  -  set or delete mailbox/server metadata entries
    /// (RFC 5464 Section 4.3).
    ///
    /// Each entry is a `(name, value)` pair. A `None` value deletes the
    /// entry (RFC 5464 Section 4.3).
    ///
    /// Requires `METADATA` or `METADATA-SERVER` capability
    /// (RFC 5464 Section 1).
    pub async fn set_metadata(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        entries: &[(&str, Option<&[u8]>)],
        timeout: Duration,
    ) -> Result<(), Error> {
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::SetMetadata)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Metadata)
                && !super::auth::snapshot_supports(&snap, &Capability::MetadataServer)
            {
                return Err(Error::MissingCapability("METADATA".into()));
            }
        }
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = Command::SetMetadata {
            mailbox: mailbox_name,
            entries: entries
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.map(<[u8]>::to_vec)))
                .collect(),
        };
        self.submit_regular(
            cmd,
            super::dispatch::TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    // -----------------------------------------------------------------------
    // QUOTA (RFC 2087 / RFC 9208)
    // -----------------------------------------------------------------------

    /// GETQUOTA  -  query quota resources for a quota root
    /// (RFC 2087 Section 4.2 / RFC 9208 Section 4.2).
    ///
    /// Returns the resource limits and usage for the specified quota root.
    ///
    /// Requires the `QUOTA` capability (RFC 2087 Section 5.1).
    pub async fn get_quota(
        &self,
        root: &str,
        timeout: Duration,
    ) -> Result<Vec<QuotaResource>, Error> {
        self.require_state(crate::types::CommandKind::GetQuota)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Quota) {
                return Err(Error::MissingCapability("QUOTA".into()));
            }
        }
        let cmd = Command::GetQuota {
            root: root.to_owned(),
        };
        let consumer = super::dispatch::QuotaConsumer::new(root.to_owned());
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    /// GETQUOTAROOT  -  query quota roots for a mailbox
    /// (RFC 2087 Section 4.3 / RFC 9208 Section 4.3).
    ///
    /// Returns the quota root names and their associated quota resources
    /// for the given mailbox.
    ///
    /// Requires the `QUOTA` capability (RFC 2087 Section 5.1).
    pub async fn get_quota_root(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<QuotaRootResponse, Error> {
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::GetQuotaRoot)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Quota) {
                return Err(Error::MissingCapability("QUOTA".into()));
            }
        }
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = Command::GetQuotaRoot {
            mailbox: mailbox_name.clone(),
        };
        let consumer = super::dispatch::QuotaRootConsumer::new(mailbox_name.clone());
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    /// SETQUOTA  -  set resource limits on a quota root
    /// (RFC 2087 Section 4.1 / RFC 9208 Section 4.1).
    ///
    /// Each element of `resources` is a `(resource_name, limit)` pair  -
    /// e.g. `("STORAGE", 51200)`.
    ///
    /// Requires the `QUOTASET` capability (RFC 9208 Section 3.2).
    pub async fn set_quota(
        &self,
        root: &str,
        resources: &[(&str, u64)],
        timeout: Duration,
    ) -> Result<Vec<QuotaResource>, Error> {
        self.require_state(crate::types::CommandKind::SetQuota)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::QuotaSet) {
                return Err(Error::MissingCapability("QUOTASET".into()));
            }
        }
        let cmd = Command::SetQuota {
            root: root.to_owned(),
            resources: resources
                .iter()
                .map(|(name, limit)| ((*name).to_owned(), *limit))
                .collect(),
        };
        let consumer = super::dispatch::QuotaConsumer::new(root.to_owned());
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    // -----------------------------------------------------------------------
    // ACL (RFC 4314)
    // -----------------------------------------------------------------------

    /// SETACL  -  set access control list entries for a mailbox
    /// (RFC 4314 Section 3.1).
    ///
    /// Sets the rights for `identifier` on `mailbox`. The `rights` string
    /// uses the format defined in RFC 4314 Section 2 (e.g., `"+lrswipkxte"`
    /// to add rights, `"-d"` to remove, or a bare string to replace).
    ///
    /// Requires the `ACL` capability (RFC 4314 Section 1).
    pub async fn set_acl(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        identifier: &str,
        rights: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::SetAcl)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Acl) {
                return Err(Error::MissingCapability("ACL".into()));
            }
        }
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = Command::SetAcl {
            mailbox: mailbox_name,
            identifier: identifier.to_owned(),
            rights: rights.to_owned(),
        };
        self.submit_regular(
            cmd,
            super::dispatch::TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// DELETEACL  -  remove an access control list entry for a mailbox
    /// (RFC 4314 Section 3.2).
    ///
    /// Removes all rights for `identifier` on `mailbox`.
    ///
    /// Requires the `ACL` capability (RFC 4314 Section 1).
    pub async fn delete_acl(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        identifier: &str,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::DeleteAcl)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Acl) {
                return Err(Error::MissingCapability("ACL".into()));
            }
        }
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = Command::DeleteAcl {
            mailbox: mailbox_name,
            identifier: identifier.to_owned(),
        };
        self.submit_regular(
            cmd,
            super::dispatch::TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// GETACL  -  retrieve the access control list for a mailbox
    /// (RFC 4314 Section 3.3).
    ///
    /// Returns the list of identifier/rights pairs for the given mailbox.
    ///
    /// Requires the `ACL` capability (RFC 4314 Section 1).
    pub async fn get_acl(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<Vec<AclEntry>, Error> {
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::GetAcl)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Acl) {
                return Err(Error::MissingCapability("ACL".into()));
            }
        }
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = Command::GetAcl {
            mailbox: mailbox_name.clone(),
        };
        let consumer = super::dispatch::AclConsumer::new(mailbox_name.clone());
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    /// LISTRIGHTS  -  query the set of rights grantable to an identifier
    /// on a mailbox (RFC 4314 Section 3.4).
    ///
    /// Returns the required (always-granted) rights and the groups of
    /// optional rights that can be independently granted or revoked.
    ///
    /// Requires the `ACL` capability (RFC 4314 Section 1).
    pub async fn list_rights(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        identifier: &str,
        timeout: Duration,
    ) -> Result<ListRightsResponse, Error> {
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::ListRights)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Acl) {
                return Err(Error::MissingCapability("ACL".into()));
            }
        }
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = Command::ListRights {
            mailbox: mailbox_name.clone(),
            identifier: identifier.to_owned(),
        };
        let consumer =
            super::dispatch::ListRightsConsumer::new(mailbox_name.clone(), identifier.to_owned());
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    /// MYRIGHTS  -  query the logged-in user's rights on a mailbox
    /// (RFC 4314 Section 3.5).
    ///
    /// Returns the rights string for the current user on the given mailbox.
    ///
    /// Requires the `ACL` capability (RFC 4314 Section 1).
    pub async fn my_rights(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<String, Error> {
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::MyRights)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Acl) {
                return Err(Error::MissingCapability("ACL".into()));
            }
        }
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = Command::MyRights {
            mailbox: mailbox_name.clone(),
        };
        let consumer = super::dispatch::MyRightsConsumer::new(mailbox_name.clone());
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    /// Verify that the server advertises the NOTIFY capability (RFC 5465).
    fn require_notify(&self) -> Result<(), Error> {
        let has_notify = self
            .state_rx
            .borrow()
            .capabilities
            .contains(&Capability::Notify);
        if !has_notify {
            return Err(Error::MissingCapability("NOTIFY".into()));
        }
        Ok(())
    }
}

/// Compute which ongoing response types a NOTIFY registration can produce.
///
/// Returns `(list, status, metadata)` booleans for *ongoing* notifications:
/// - `list`: any non-selected events are registered. `MailboxName`
///   (RFC 5465 Section 5.4) and `SubscriptionChange` (RFC 5465 Section 5.5)
///   produce explicit LIST responses, but ALL non-selected registrations
///   can trigger LIST responses for ACL changes  -  RFC 5465 Section 5.9
///   requires `LIST \NoAccess` / `LIST` when the logged-in user loses or
///   regains the `l` (lookup) ACL right on any monitored mailbox,
///   regardless of which event types were requested.
/// - `status`: message events on non-selected mailboxes  -  `FlagChange`
///   and `AnnotationChange` (RFC 5465 Section 5.1), `MessageNew`
///   (RFC 5465 Section 5.2), and `MessageExpunge` (RFC 5465 Section 5.3),
///   all delivered as STATUS responses.
/// - `metadata`: `MailboxMetadataChange` (RFC 5465 Section 5.6) or
///   `ServerMetadataChange` (RFC 5465 Section 5.7) events registered  -
///   delivered as METADATA.
///
/// The `params.status` indicator is NOT included here  -  it only triggers
/// an initial STATUS snapshot for non-selected mailboxes with message
/// events (RFC 5465 Section 4), not ongoing STATUS notifications. Those
/// initial responses are handled by the re-push logic in `notify_set()`.
pub(crate) fn compute_notify_flags(params: &NotifySetParams) -> (bool, bool, bool) {
    let mut list = false;
    let mut status = false;
    let mut metadata = false;

    for group in &params.event_groups {
        let is_non_selected = !matches!(
            group.filter,
            MailboxFilter::Selected | MailboxFilter::SelectedDelayed
        );

        // RFC 5465 Section 5.9: any non-selected registration can trigger
        // LIST \NoAccess or LIST responses for ACL changes on monitored
        // mailboxes. Enable LIST buffering for all non-selected groups.
        if is_non_selected && !group.events.is_empty() {
            list = true;
        }

        for event in &group.events {
            match event {
                NotifyEvent::MessageNew { .. }
                | NotifyEvent::MessageExpunge
                | NotifyEvent::FlagChange
                | NotifyEvent::AnnotationChange => {
                    // RFC 5465 Sections 5.1-5.3: on non-selected mailboxes
                    // all message events are delivered as STATUS responses.
                    if is_non_selected {
                        status = true;
                    }
                }
                NotifyEvent::MailboxMetadataChange | NotifyEvent::ServerMetadataChange => {
                    // RFC 5465 Sections 5.6-5.7: delivered as METADATA.
                    metadata = true;
                }
                NotifyEvent::Other(_) => {
                    // Unknown extension event  -  we don't know which
                    // response type the server will use for delivery.
                    // Enable ALL known buffering flags defensively so
                    // the notification is not silently dropped regardless
                    // of delivery mechanism.  Note: Other(_) can only
                    // appear under non-selected filters  -  the encoder
                    // rejects it for selected / selected-delayed per
                    // RFC 5465 Section 6.1 / Section 8 (event-ext is a
                    // separate ABNF production from message-event).
                    // Truly extension-defined responses
                    // (`UntaggedResponse::Unknown`) are also routed
                    // to the unsolicited buffer by the dispatcher.
                    list = true;
                    status = true;
                    metadata = true;
                }
                _ => {}
            }
        }
    }

    (list, status, metadata)
}

#[cfg(test)]
#[path = "extensions_tests.rs"]
mod tests;
