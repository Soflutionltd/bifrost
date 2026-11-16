#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // COMPRESS (RFC 4978)
    // -----------------------------------------------------------------------

    #[cfg(test)]
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

        // Check COMPRESS=DEFLATE capability from the snapshot.

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
    /// - [`crate::error::LocalRefusal::MissingCapability`] if the server does not advertise `NOTIFY`.
    /// - [`crate::error::LocalRefusal::InvalidState`] if the command is issued in an invalid state
    ///   ([`crate::error::TransportFailure::Closed`] once the session is in Logout).
    /// - [`crate::error::ServerStatus::No`] if the server rejects the request (e.g. `[BADEVENT]`
    ///   for unsupported event types, RFC 5465 Section 5).
    pub async fn notify_set(
        &self,
        params: NotifySetParams,
        timeout: Duration,
    ) -> Result<(), Error> {
        let cmd = super::request::NotifySet { params };

        // Driver arms NOTIFY SET before sending; apply_tagged updates
        // notify flags on tagged OK. NOTIFICATIONOVERFLOW clears them.
        // NO/BAD arrive as the ordinary `Err` of this call: they ride
        // `Finalized::output`, so the consumer's `reclassified_as_events` is
        // emitted on that path too, and the caller no longer unwraps a second
        // inner `Result` to find out.
        let overflow = self.execute_request(cmd, timeout).await?;

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

    #[cfg(test)]
    /// NOTIFY NONE  -  cancel all event subscriptions (RFC 5465 Section 3).
    ///
    /// Reverts to baseline IMAP behavior where the server only sends
    /// notifications during command processing, and only for the selected
    /// mailbox.
    ///
    /// # Errors
    ///
    /// - [`crate::error::LocalRefusal::MissingCapability`] if the server does not advertise `NOTIFY`.
    /// - [`crate::error::LocalRefusal::InvalidState`] if the command is issued in an invalid state
    ///   ([`crate::error::TransportFailure::Closed`] once the session is in Logout).
    pub async fn notify_none(&self, timeout: Duration) -> Result<(), Error> {
        // RFC 5465 Section 3: NOTIFY is a `command-auth` extension.

        let cmd = super::request::NotifyNone {};
        // Driver arms NOTIFY NONE before sending;
        // apply_tagged resets notify flags on tagged OK.
        self.execute_request(cmd, timeout).await?;

        debug!("NOTIFY NONE  -  notifications disabled (RFC 5465)");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ENABLE (RFC 5161 Section 3 / RFC 9051 Section 6.3.1)
    // -----------------------------------------------------------------------

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
    /// returns [`crate::error::LocalRefusal::InvalidState`].
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

        let cmd = super::request::Enable {
            capabilities: capabilities
                .iter()
                .map(|c| Capability::from_imap_str(c))
                .collect(),
        };

        self.execute_request(cmd, timeout).await
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
        self.execute_request(super::request::Namespace {}, timeout)
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
        let cmd = super::request::Id {
            params: params
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.map(str::to_owned)))
                .collect(),
        };
        self.execute_request(cmd, timeout).await
    }

    // -----------------------------------------------------------------------
    // GETMETADATA / SETMETADATA (RFC 5464)
    // -----------------------------------------------------------------------

    #[cfg(test)]
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
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = super::request::GetMetadata {
            mailbox: mailbox_name,
            entries: entries.iter().map(|e| (*e).to_owned()).collect(),
            max_size,
            depth: depth.map(str::to_owned),
        };

        self.execute_request(cmd, timeout).await
    }

    #[cfg(test)]
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
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = super::request::SetMetadata {
            mailbox: mailbox_name,
            entries: entries
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.map(<[u8]>::to_vec)))
                .collect(),
        };
        self.execute_request(cmd, timeout).await
    }

    // -----------------------------------------------------------------------
    // QUOTA (RFC 2087 / RFC 9208)
    // -----------------------------------------------------------------------

    #[cfg(test)]
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
        let cmd = super::request::GetQuota {
            root: root.to_owned(),
        };

        self.execute_request(cmd, timeout).await
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
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = super::request::GetQuotaRoot {
            mailbox: mailbox_name,
        };

        self.execute_request(cmd, timeout).await
    }

    #[cfg(test)]
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
        let cmd = super::request::SetQuota {
            root: root.to_owned(),
            resources: resources
                .iter()
                .map(|(name, limit)| ((*name).to_owned(), *limit))
                .collect(),
        };

        self.execute_request(cmd, timeout).await
    }

    // -----------------------------------------------------------------------
    // ACL (RFC 4314)
    // -----------------------------------------------------------------------

    #[cfg(test)]
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
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = super::request::GetAcl {
            mailbox: mailbox_name,
        };

        self.execute_request(cmd, timeout).await
    }

    #[cfg(test)]
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
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = super::request::ListRights {
            mailbox: mailbox_name,
            identifier: identifier.to_owned(),
        };

        self.execute_request(cmd, timeout).await
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
        let mailbox_name = mailbox.mailbox_name()?;
        let cmd = super::request::MyRights {
            mailbox: mailbox_name,
        };

        self.execute_request(cmd, timeout).await
    }
}

#[cfg(test)]
#[path = "extensions_tests.rs"]
mod tests;
