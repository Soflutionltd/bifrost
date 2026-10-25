#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Mailbox operations
    // -----------------------------------------------------------------------

    /// LIST mailboxes (RFC 3501 Section 6.3.8).
    ///
    /// For LIST-EXTENDED selection or return options, use
    /// [`ImapConnection::list_extended`].
    pub async fn list(
        &self,
        reference: &(impl crate::types::MailboxInput + ?Sized),
        pattern: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<Vec<MailboxInfo>, Error> {
        use super::dispatch::ListConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::List)?;
        let cmd = Command::List {
            reference: reference.mailbox_name()?.into(),
            pattern: pattern.mailbox_name()?.into(),
        };
        // The ListConsumer handles NOTIFY marker classification via the
        // per-response notify_snapshot passed by the dispatcher. Mid-stream
        // NOTIFICATIONOVERFLOW (RFC 5465 Section5.8) is handled automatically:
        // apply_side_effects clears the notify flags, so subsequent
        // snapshots have list=false.
        self.submit_regular(
            cmd,
            ListConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// LIST mailboxes with RFC 5258 selection options, multiple patterns, and
    /// return options (RFC 5258 Section 3 / RFC 9051 Section 6.3.9).
    ///
    /// Examples:
    /// - `selection_options = &["SUBSCRIBED"]`
    /// - `return_options = &["CHILDREN"]`
    /// - `return_options = &["STATUS (MESSAGES UNSEEN)"]`
    ///
    /// On `IMAP4rev1`, the connection enforces capability gates for the
    /// requested options:
    /// - `LIST-EXTENDED` for RFC 5258 syntax such as selection options,
    ///   multiple patterns, and all return options, including extension forms
    ///   like `SPECIAL-USE` and `STATUS (...)`
    /// - `LIST-STATUS` for `STATUS (...)`
    /// - `SPECIAL-USE` for `SPECIAL-USE`
    pub async fn list_extended(
        &self,
        reference: &str,
        patterns: &[&str],
        selection_options: &[&str],
        return_options: &[&str],
        timeout: Duration,
    ) -> Result<Vec<MailboxInfo>, Error> {
        let names = patterns
            .iter()
            .map(|name| MailboxName::new(*name))
            .collect::<Result<Vec<_>, _>>()?;
        let names = names.iter().collect::<Vec<_>>();
        self.list_extended_names(
            reference,
            &names,
            selection_options,
            return_options,
            timeout,
        )
        .await
    }

    pub(crate) async fn list_extended_names(
        &self,
        reference: &(impl crate::types::MailboxInput + ?Sized),
        patterns: &[&MailboxName],
        selection_options: &[&str],
        return_options: &[&str],
        timeout: Duration,
    ) -> Result<Vec<MailboxInfo>, Error> {
        use super::dispatch::ListExtendedConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::List)?;
        let names = patterns
            .iter()
            .map(crate::types::MailboxInput::mailbox_name)
            .collect::<Result<Vec<_>, _>>()?;
        let display = names.iter().map(MailboxName::as_str).collect::<Vec<_>>();
        self.validate_list_extended_request(&display, selection_options, return_options)?;

        if selection_options.is_empty() && return_options.is_empty() && patterns.len() == 1 {
            return self.list(reference, patterns[0], timeout).await;
        }

        let cmd = Command::ListExtended {
            selection_options: selection_options
                .iter()
                .map(|option| (*option).to_owned())
                .collect(),
            reference: reference.mailbox_name()?.into(),
            patterns: names.into_iter().map(Into::into).collect(),
            return_options: return_options
                .iter()
                .map(|option| (*option).to_owned())
                .collect(),
        };
        // RFC 5258 Section 3: with SUBSCRIBED, the server may return
        // subscribed-but-deleted mailboxes with \NonExistent and
        // subscribed-but-inaccessible ones with \NoAccess. In that
        // context these are legitimate solicited attributes, not NOTIFY
        // markers.
        let filter_extended = !selection_options
            .iter()
            .any(|o| o.eq_ignore_ascii_case("SUBSCRIBED"));

        let consumer = ListExtendedConsumer::new(
            filter_extended,
            selection_options.iter().map(|o| (*o).to_owned()).collect(),
        );
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    /// LIST with STATUS return option (RFC 5819 Section 2).
    ///
    /// Returns mailbox information paired with STATUS data for each mailbox.
    /// `status_items` is the raw status items string, e.g. `"MESSAGES UNSEEN"`.
    /// The server returns interleaved LIST and STATUS untagged responses;
    /// this method correlates them by mailbox name.
    pub async fn list_status(
        &self,
        reference: &(impl crate::types::MailboxInput + ?Sized),
        pattern: &(impl crate::types::MailboxInput + ?Sized),
        status_items: &str,
        timeout: Duration,
    ) -> Result<Vec<(MailboxInfo, Vec<StatusItem>)>, Error> {
        use super::dispatch::ListStatusConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::ListStatus)?;
        // RFC 5819 Section 2 extends the LIST command syntax from RFC 5258
        // Section 3, so IMAP4rev1 needs both LIST-STATUS and LIST-EXTENDED.
        // RFC 9051 Section 6.3.9 folds LIST-STATUS into the IMAP4rev2 base.
        {
            let snap = self.state_rx.borrow();
            if !auth::snapshot_supports(&snap, &Capability::ListStatus) {
                return Err(Error::MissingCapability("LIST-STATUS".into()));
            }
            if !auth::snapshot_supports(&snap, &Capability::ListExtended) {
                return Err(Error::MissingCapability("LIST-EXTENDED".into()));
            }
        }
        self.validate_requested_status_items(status_items)?;
        let cmd = Command::ListStatus {
            reference: reference.mailbox_name()?.into(),
            pattern: pattern.mailbox_name()?.into(),
            status_items: status_items.to_owned(),
        };
        self.submit_regular(
            cmd,
            ListStatusConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// SELECT a mailbox (RFC 3501 Section 6.3.1).
    ///
    /// For CONDSTORE or QRESYNC options, use [`select_with`](Self::select_with).
    pub async fn select(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<SelectedMailbox, Error> {
        self.select_with(mailbox, &SelectOptions::default(), timeout)
            .await
    }

    /// SELECT a mailbox with extension options (RFC 3501 Section 6.3.1,
    /// RFC 7162 Sections 3.1.8 and 3.2.5.2).
    ///
    /// Pass [`SelectOptions::default()`] for a plain SELECT, or use the
    /// convenience constructors:
    /// - [`SelectOptions::condstore()`] for `SELECT <mailbox> (CONDSTORE)`
    /// - [`SelectOptions::qresync(params)`] for `SELECT <mailbox> (QRESYNC ...)`
    pub async fn select_with(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        options: &SelectOptions,
        timeout: Duration,
    ) -> Result<SelectedMailbox, Error> {
        self.select_or_examine(
            mailbox,
            false,
            options.condstore,
            options.qresync.clone(),
            timeout,
        )
        .await
    }

    /// EXAMINE a mailbox (read-only SELECT, RFC 3501 Section 6.3.2).
    ///
    /// For CONDSTORE or QRESYNC options, use [`examine_with`](Self::examine_with).
    pub async fn examine(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<SelectedMailbox, Error> {
        self.examine_with(mailbox, &SelectOptions::default(), timeout)
            .await
    }

    /// EXAMINE a mailbox with extension options (RFC 3501 Section 6.3.2,
    /// RFC 7162 Sections 3.1.8 and 3.2.5.2).
    ///
    /// Read-only variant of [`select_with`](Self::select_with). Pass
    /// [`SelectOptions::default()`] for a plain EXAMINE, or use the
    /// convenience constructors:
    /// - [`SelectOptions::condstore()`] for `EXAMINE <mailbox> (CONDSTORE)`
    /// - [`SelectOptions::qresync(params)`] for `EXAMINE <mailbox> (QRESYNC ...)`
    pub async fn examine_with(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        options: &SelectOptions,
        timeout: Duration,
    ) -> Result<SelectedMailbox, Error> {
        self.select_or_examine(
            mailbox,
            true,
            options.condstore,
            options.qresync.clone(),
            timeout,
        )
        .await
    }

    /// Shared implementation for SELECT and EXAMINE commands
    /// (RFC 3501 Sections 6.3.1-6.3.2, RFC 7162 Sections 3.1.8 and 3.2.5.2).
    ///
    /// Handles common validation (UTF8=ONLY enforcement, session state),
    /// extension-specific capability checks (CONDSTORE, QRESYNC), command
    /// construction, and dispatch via [`SelectConsumer`].
    ///
    /// State transitions are handled by the driver task via the `in_select`
    /// flag in `ProtocolState::apply_tagged`:
    /// - Tagged OK -> `Selected` (RFC 3501 Section6.3.1)
    /// - Tagged NO -> `Authenticated` (deselects, RFC 3501 Section6.3.1)
    /// - Tagged BAD -> no change (RFC 3501 Section6)
    pub(super) async fn select_or_examine(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        is_examine: bool,
        condstore: bool,
        qresync: Option<QresyncParams>,
        timeout: Duration,
    ) -> Result<SelectedMailbox, Error> {
        use super::dispatch::SelectConsumer;

        self.check_utf8_only_enforced()?;
        // SELECT and EXAMINE share one row of the table.
        self.require_state(crate::types::CommandKind::Select)?;
        if condstore {
            self.require_condstore()?;
        }
        if let Some(ref params) = qresync {
            self.validate_qresync_params(params)?;
        }
        let wire_mailbox = mailbox.mailbox_name()?;
        let cmd = if is_examine {
            Command::Examine {
                mailbox: wire_mailbox,
                condstore,
                qresync,
            }
        } else {
            Command::Select {
                mailbox: wire_mailbox,
                condstore,
                qresync,
            }
        };

        let consumer = SelectConsumer::new(is_examine);
        // NO/BAD and validation failures arrive as the ordinary `Err` of this
        // call now that `Finalized::output` carries them. The consumer still
        // reclassifies its accumulated responses as events on those paths -
        // that is the point of the envelope - but the caller no longer has to
        // unwrap a second, inner `Result` to see the failure.
        //
        // State transitions (Selected on OK, Authenticated on NO) are
        // handled by the driver's apply_tagged via the in_select flag.
        self.submit_regular(cmd, consumer, Some(tokio::time::Instant::now() + timeout))
            .await
    }

    /// Validate QRESYNC parameters before SELECT/EXAMINE (RFC 7162 Section 3.2.5.2).
    ///
    /// Ensures QRESYNC has been `ENABLE`d and that seq-match-data is only
    /// present when known-uids is also present (per the ABNF in RFC 7162
    /// Section 3.2.5.2).
    pub(super) fn validate_qresync_params(&self, params: &QresyncParams) -> Result<(), Error> {
        // RFC 7162 Section 3.2.3: the client MUST issue ENABLE QRESYNC
        // before using QRESYNC parameters in SELECT/EXAMINE.
        {
            let snap = self.state_rx.borrow();
            // The ENABLED echo is stored as the server spelled it, and RFC 9051
            // Section 9 makes the name an atom, so match it case-insensitively
            // exactly as `ServerProfile::enabled` does.
            if !snap
                .enabled
                .iter()
                .any(|e| e.eq_ignore_ascii_case("QRESYNC"))
            {
                return Err(Error::MissingCapability("QRESYNC (not ENABLEd)".into()));
            }
        }
        // RFC 7162 Section 3.2.5.2 ABNF: seq-match-data is only valid after
        // known-uids. Reject invalid combinations instead of fabricating data.
        if params.seq_match_data.is_some() && params.known_uids.is_none() {
            return Err(Error::InvalidInput(
                "QRESYNC seq-match-data requires known-uids \
                 (RFC 7162 Section 3.2.5.2)"
                    .into(),
            ));
        }
        Ok(())
    }

    /// CREATE a mailbox (RFC 3501 Section 6.3.3).
    pub async fn create(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<(), Error> {
        self.create_with_mailbox_id(mailbox, timeout)
            .await
            .map(|_| ())
    }

    /// CREATE a mailbox and return the server-assigned `MAILBOXID` when present
    /// (RFC 8474 Section 4.1).
    ///
    /// RFC 8474 Section 4.1: a server advertising `OBJECTID` MUST include a
    /// tagged `MAILBOXID` response code on successful CREATE. Servers without
    /// `OBJECTID` support, or non-conformant servers, may omit it, so this
    /// method returns `Ok(None)` in that case.
    pub async fn create_with_mailbox_id(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<Option<String>, Error> {
        use super::dispatch::CreateConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Create)?;
        let cmd = Command::Create {
            mailbox: mailbox.mailbox_name()?,
        };
        self.submit_regular(
            cmd,
            CreateConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// CREATE a mailbox with special-use attributes (RFC 6154 Section 3).
    ///
    /// RFC 6154 Section 3 / Section 6 ABNF:
    /// `create-param =/ "USE" SP "(" [use-attr *(SP use-attr)] ")"`
    /// where `use-attr = "\All" / "\Archive" / "\Drafts" / "\Flagged" /
    ///                    "\Junk" / "\Sent" / "\Trash" / use-attr-ext`
    ///
    /// Requires the server to advertise `CREATE-SPECIAL-USE` capability.
    /// RFC 6154 Section 3: "Clients MUST NOT use the USE parameter unless the
    /// server advertises the CREATE-SPECIAL-USE capability."
    pub async fn create_special_use(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        special_use: &[MailboxAttribute],
        timeout: Duration,
    ) -> Result<(), Error> {
        self.create_special_use_with_mailbox_id(mailbox, special_use, timeout)
            .await
            .map(|_| ())
    }

    /// CREATE a mailbox with special-use attributes and return the server's
    /// `MAILBOXID` when present (RFC 6154 Section 3, RFC 8474 Section 4.1).
    pub async fn create_special_use_with_mailbox_id(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        special_use: &[MailboxAttribute],
        timeout: Duration,
    ) -> Result<Option<String>, Error> {
        use super::dispatch::CreateConsumer;

        // RFC 6855 Section 3: UTF8=ONLY requires ENABLE UTF8=ACCEPT first.
        self.check_utf8_only_enforced()?;
        // RFC 6154 Section 3: CREATE is a `command-auth`  -  valid only in
        // Authenticated or Selected state.
        self.require_state(crate::types::CommandKind::Create)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::CreateSpecialUse) {
                return Err(Error::MissingCapability("CREATE-SPECIAL-USE".into()));
            }
        }
        // RFC 6154 Section 3: "The USE parameter MUST NOT contain any
        // non-use-attr values." Reject base LIST attributes like \Noselect,
        // \HasChildren, etc. before sending the command.
        if let Some(bad) = special_use.iter().find(|a| !a.is_special_use()) {
            return Err(Error::InvalidInput(format!(
                "CREATE USE parameter contains non-special-use attribute {} \
                 (RFC 6154 Section 3: USE MUST only contain use-attr values)",
                bad.as_imap_str()
            )));
        }
        let cmd = Command::CreateSpecialUse {
            mailbox: mailbox.mailbox_name()?,
            special_use: special_use.to_vec(),
        };
        self.submit_regular(
            cmd,
            CreateConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// DELETE a mailbox (RFC 3501 Section 6.3.4).
    pub async fn delete(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::TaggedOkConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Delete)?;
        let cmd = Command::Delete {
            mailbox: mailbox.mailbox_name()?,
        };
        self.submit_regular(
            cmd,
            TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// RENAME a mailbox (RFC 3501 Section 6.3.5).
    pub async fn rename(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        new_name: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::TaggedOkConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Rename)?;
        let cmd = Command::Rename {
            mailbox: mailbox.mailbox_name()?,
            new_name: new_name.mailbox_name()?,
        };
        self.submit_regular(
            cmd,
            TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// SUBSCRIBE to a mailbox (RFC 3501 Section 6.3.6).
    ///
    /// Adds the mailbox to the server's set of "active" or "subscribed" mailboxes
    /// returned by LSUB.
    pub async fn subscribe(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::TaggedOkConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Subscribe)?;
        let cmd = Command::Subscribe {
            mailbox: mailbox.mailbox_name()?,
        };
        self.submit_regular(
            cmd,
            TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UNSUBSCRIBE from a mailbox (RFC 3501 Section 6.3.7).
    pub async fn unsubscribe(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<(), Error> {
        use super::dispatch::TaggedOkConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Unsubscribe)?;
        let cmd = Command::Unsubscribe {
            mailbox: mailbox.mailbox_name()?,
        };
        self.submit_regular(
            cmd,
            TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// LSUB  -  list subscribed mailboxes (RFC 3501 Section 6.3.9).
    ///
    /// Obsoleted by LIST-EXTENDED (RFC 5258) but still required for servers
    /// that don't support `\Subscribed` attribute in LIST.
    pub async fn lsub(
        &self,
        reference: &(impl crate::types::MailboxInput + ?Sized),
        pattern: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<Vec<MailboxInfo>, Error> {
        use super::dispatch::LsubConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Lsub)?;

        // RFC 9051 Appendix F item 19: LSUB was deprecated in IMAP4rev2.
        // Use LIST with \Subscribed return option instead.
        if self.is_rev2() {
            return Err(Error::MissingCapability(
                "LSUB was deprecated in IMAP4rev2 (RFC 9051 Appendix F); \
                 use list() with \\Subscribed attribute instead"
                    .into(),
            ));
        }

        let cmd = Command::Lsub {
            reference: reference.mailbox_name()?.into(),
            pattern: pattern.mailbox_name()?.into(),
        };
        self.submit_regular(
            cmd,
            LsubConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// CLOSE the selected mailbox (RFC 3501 Section 6.4.2).
    ///
    /// Permanently removes all messages with the `\Deleted` flag and returns
    /// to the authenticated state. Use [`unselect`](Self::unselect) to deselect
    /// without expunging.
    ///
    /// State transition to `Authenticated` is handled by the driver task
    /// via the `in_close` flag in `ProtocolState::apply_tagged`.
    pub async fn close(&self, timeout: Duration) -> Result<(), Error> {
        use super::dispatch::TaggedOkConsumer;

        self.require_state(crate::types::CommandKind::Close)?;
        self.submit_regular(
            Command::Close,
            TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await?;
        Ok(())
    }

    /// UNSELECT  -  deselect the current mailbox without expunging
    /// (RFC 3691 Section 3).
    ///
    /// Closes the currently selected mailbox and returns to the
    /// Authenticated state, but unlike [`close`](Self::close) does NOT
    /// permanently remove messages with the `\Deleted` flag.
    ///
    /// Requires the `UNSELECT` capability or an `IMAP4rev2` connection
    /// (RFC 9051 folds UNSELECT into the base protocol).
    ///
    /// State transition to `Authenticated` is handled by the driver task
    /// via the `in_close` flag in `ProtocolState::apply_tagged`.
    ///
    /// # Snapshot timing
    ///
    /// The state snapshot transitions to `Authenticated` only after the
    /// tagged OK is received and processed by the driver.
    pub async fn unselect(&self, timeout: Duration) -> Result<(), Error> {
        self.require_state(crate::types::CommandKind::Unselect)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Unselect) {
                return Err(Error::MissingCapability("UNSELECT".into()));
            }
        }
        self.submit_regular(
            Command::Unselect,
            super::dispatch::TaggedOkConsumer::default(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await?;
        Ok(())
    }

    /// STATUS of a mailbox without selecting it (RFC 3501 Section 6.3.10).
    ///
    /// `items` may be either a raw status item list such as
    /// `"MESSAGES UNSEEN UIDNEXT"` or an already parenthesized
    /// `"(MESSAGES UNSEEN UIDNEXT)"` list.
    ///
    /// Returns a [`StatusResult`] that includes any ambiguous same-mailbox
    /// `STATUS` responses when NOTIFY is active  -  see its documentation for
    /// details on the inherent protocol ambiguity (RFC 5465 Section 4).
    pub async fn status(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        items: &str,
        timeout: Duration,
    ) -> Result<StatusResult, Error> {
        use super::dispatch::StatusConsumer;

        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Status)?;
        self.validate_requested_status_items(items)?;
        let cmd = Command::Status {
            mailbox: mailbox.mailbox_name()?,
            items: items.to_owned(),
        };
        self.submit_regular(
            cmd,
            StatusConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }
}

#[cfg(test)]
#[path = "mailbox_tests.rs"]
mod tests;
