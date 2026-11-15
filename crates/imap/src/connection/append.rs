#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Append
    // -----------------------------------------------------------------------

    /// APPEND a message to a mailbox (RFC 3501 Section 6.3.11).
    ///
    /// The handle only carries the request to the driver as a
    /// `Command::Append`; the driver checks and encodes it from live protocol
    /// state when it executes, like every other command (literal
    /// synchronization, LITERAL+/LITERAL- markers, `literal8` for NUL bodies,
    /// the RFC 6855 `UTF8 (` wrapper, mailbox encoding).
    ///
    /// `message` is borrowed, so it is copied once into a shared buffer to
    /// cross the driver channel. A caller that already holds the body as
    /// [`bytes::Bytes`] should use [`append_message`](Self::append_message),
    /// which sends that allocation to the socket without copying it.
    ///
    /// Returns `Some((uid_validity, uid))` when the server supports UIDPLUS
    /// (RFC 4315) and includes an `[APPENDUID]` response code, otherwise `None`.
    pub async fn append(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        flags: &[Flag],
        date: Option<&str>,
        message: &[u8],
        timeout: Duration,
    ) -> Result<Option<crate::types::AppendUid>, Error> {
        let mut msg = AppendMessage::new(bytes::Bytes::copy_from_slice(message));
        msg.flags = flags.to_vec();
        msg.date = date.map(str::to_owned);
        self.append_message(mailbox, msg, timeout).await
    }

    /// APPEND one owned message (RFC 3501 Section 6.3.11), without copying its
    /// body: the `Bytes` allocation is written to the socket as it is.
    ///
    /// Otherwise identical to [`append`](Self::append), including the
    /// APPENDLIMIT (RFC 7889) preflight and its `Unsent` transmission evidence.
    pub async fn append_message(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        message: AppendMessage,
        timeout: Duration,
    ) -> Result<Option<crate::types::AppendUid>, Error> {
        use super::dispatch::AppendConsumer;

        self.check_utf8_only_enforced()?;
        // RFC 3501 Section 6.3.11: APPEND is valid in Authenticated and Selected
        // states. Only an early refusal - the driver re-checks against live
        // state when it executes the command.
        self.require_state(crate::types::CommandKind::Append)?;

        let deadline = tokio::time::Instant::now() + timeout;
        if let Some(limit) = self.append_limit_for_mailbox(mailbox, deadline).await? {
            self.check_append_limit(message.data.len(), limit)?;
        }

        self.submit_regular(
            Command::Append {
                mailbox: mailbox.mailbox_name()?.into(),
                messages: vec![message],
                multi: false,
            },
            AppendConsumer::default(),
            Some(deadline),
        )
        .await
    }

    /// MULTIAPPEND  -  append multiple messages in a single APPEND command (RFC 3502).
    ///
    /// Sends all messages as consecutive literals in one APPEND command.
    /// Each message carries its own flags and optional internal date.
    /// The first message includes the mailbox name; subsequent messages
    /// follow immediately with their own flag/date/literal (RFC 3502 Section 3).
    ///
    /// Checks APPENDLIMIT (RFC 7889) per message. Everything else - the
    /// MULTIAPPEND capability, LITERAL+ markers (RFC 7888), BINARY for
    /// NUL-bearing bodies - is validated and encoded by the driver against
    /// live state when it executes the command. The bodies are shared
    /// `Bytes`, so this clones reference counts, not message data.
    ///
    /// Returns a `Vec<(uid_validity, uid)>` extracted from `[APPENDUID]` response
    /// codes (RFC 4315 UIDPLUS). The vec may be empty if the server does not
    /// support UIDPLUS.
    pub async fn multi_append(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        messages: &[AppendMessage],
        timeout: Duration,
    ) -> Result<Vec<crate::types::AppendUid>, Error> {
        use super::dispatch::MultiAppendConsumer;

        self.check_utf8_only_enforced()?;
        // RFC 3502 Section 3: MULTIAPPEND is valid in Authenticated and Selected
        // states. Only an early refusal - the driver re-checks against live
        // state when it executes the command.
        self.require_state(crate::types::CommandKind::Append)?;

        if messages.is_empty() {
            return Err(Error::LocalRefusal(
                crate::error::LocalRefusal::InvalidInput(
                    "MULTIAPPEND requires at least one message".into(),
                ),
            ));
        }

        // Snapshot the advertised APPENDLIMIT before any STATUS lookup for a
        // mailbox-specific limit. This is the only handle-side read of
        // capabilities: it feeds the client-side size check, not wire bytes.
        let advertised_limits = self.server_profile().append_limit;

        let deadline = tokio::time::Instant::now() + timeout;
        if let Some(limit) = self
            .append_limit_from_advertisement(mailbox, advertised_limits, deadline)
            .await?
        {
            for msg in messages {
                self.check_append_limit(msg.data.len(), limit)?;
            }
        }

        self.submit_regular(
            Command::Append {
                mailbox: mailbox.mailbox_name()?.into(),
                messages: messages.to_vec(),
                multi: true,
            },
            MultiAppendConsumer::new(messages.len()),
            Some(deadline),
        )
        .await
    }

    /// Resolve the effective APPENDLIMIT for one destination mailbox.
    ///
    /// RFC 7889 defines `APPENDLIMIT=<n>` as a global limit and bare
    /// `APPENDLIMIT` as a request to obtain the mailbox value with STATUS.
    /// A conforming server advertises one form, but a contradictory capability
    /// list is handled conservatively: obtain a mailbox value when requested
    /// and apply the smallest numeric value available.
    async fn append_limit_for_mailbox(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        deadline: tokio::time::Instant,
    ) -> Result<Option<u64>, Error> {
        let advertised = self.server_profile().append_limit;
        self.append_limit_from_advertisement(mailbox, advertised, deadline)
            .await
    }

    async fn append_limit_from_advertisement(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        advertised: crate::types::profile::AppendLimitPolicy,
        deadline: tokio::time::Instant,
    ) -> Result<Option<u64>, Error> {
        use crate::types::profile::AppendLimitPolicy;

        let global = match advertised {
            AppendLimitPolicy::NotAdvertised => return Ok(None),
            AppendLimitPolicy::Limit(limit) => return Ok(Some(limit)),
            AppendLimitPolicy::PerMailbox { global_ceiling } => global_ceiling,
        };
        // APPEND submission already uses the original deadline; carry it
        // through the helper too, rather than starting a fresh relative budget.
        // The helper and APPEND share one budget. Its failures describe an
        // APPEND never sent, even if the STATUS itself reached the server.
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| unsent_preflight(Error::timeout()))?;
        let status =
            tokio::time::timeout_at(deadline, self.status(mailbox, "APPENDLIMIT", remaining))
                .await
                .map_err(|_| unsent_preflight(Error::timeout()))?
                .map_err(unsent_preflight)?;
        let mailbox_limit = mailbox_append_limit(&status)?;

        Ok(match (global, mailbox_limit) {
            (Some(global), Some(mailbox)) => Some(global.min(mailbox)),
            (Some(global), None) => Some(global),
            (None, Some(mailbox)) => Some(mailbox),
            (None, None) => None,
        })
    }

    fn check_append_limit(&self, size: usize, limit: u64) -> Result<(), Error> {
        let size = u64::try_from(size).unwrap_or(u64::MAX);
        if size > limit {
            return Err(Error::LocalRefusal(
                crate::error::LocalRefusal::AppendLimit { size, limit },
            ));
        }
        Ok(())
    }
}

/// The most restrictive APPENDLIMIT any plausible reply to this STATUS named.
///
/// RFC 5465 Section 4: with NOTIFY STATUS active the solicited reply is
/// wire-identical to a notification, so the heuristic that picked
/// [`StatusResult::items`] may have picked a stale notification and left the
/// real answer in [`StatusResult::ambiguous`]. Ignoring `ambiguous` therefore
/// either invents a protocol error (the answer is there, just not where we
/// looked) or applies a higher, stale limit. RFC 7889 Section 3.1: an APPEND
/// over the limit is refused, so the smallest named value is the safe choice.
///
/// `None` means "no limit for this mailbox" (an explicit `APPENDLIMIT NIL`),
/// which is different from the absence of the item altogether: the latter is a
/// server contradicting its own advertised capability.
pub(super) fn mailbox_append_limit(status: &StatusResult) -> Result<Option<u64>, Error> {
    let mut saw_append_limit = false;
    let mut limit = None;
    for item in status.items.iter().chain(status.ambiguous.iter().flatten()) {
        if let StatusItem::AppendLimit(value) = item {
            saw_append_limit = true;
            if let Some(value) = *value {
                limit = Some(limit.map_or(value, |current: u64| current.min(value)));
            }
        }
    }
    if !saw_append_limit {
        return Err(Error::Provider(crate::error::ProviderFailure::Missing(
            "APPENDLIMIT capability but STATUS response omitted APPENDLIMIT".into(),
        )));
    }
    Ok(limit)
}

/// Restamp a preflight failure as `Unsent` relative to the APPEND.
///
/// The helper command carries its own transmission evidence, which is about
/// the helper, not about the APPEND. APPEND is non-idempotent, so publishing
/// the helper's `InFlight` / `Acknowledged` evidence would make recovery
/// reconcile a message that was never written.
fn unsent_preflight(error: Error) -> Error {
    error.for_unsent_operation()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[tokio::test]
    async fn expired_append_preflight_is_unsent_without_submitting_status() {
        let connection = crate::connection::test_support::detached(
            SessionState::Authenticated,
            vec![Capability::AppendLimit(None)],
            &[],
        );
        let result = connection
            .append_limit_for_mailbox("INBOX", tokio::time::Instant::now())
            .await;
        // The detached driver would return DriverGone if STATUS was queued.
        let error = result.expect_err("expired preflight must fail");
        assert!(matches!(
            &error,
            Error::OperationNotSent(source) if matches!(source.as_ref(), Error::Transport {
                kind: crate::error::TransportFailure::Timeout,
                ..
            })
        ));
        assert_eq!(
            error.attempt(),
            Some(bifrost_types::TransmissionState::Unsent)
        );
    }
}
