#![allow(clippy::wildcard_imports)]
use super::*;

/// Drain a bounded FETCH stream, releasing the receiver before returning early.
///
/// Dropping the receiver tells the driver that no consumer capacity will ever
/// be available again; its next `reserve_owned` fails, it marks the pipe
/// drained, and it keeps reading through the tagged completion to preserve
/// IMAP framing. Already-buffered items are discarded because the caller has
/// explicitly stopped consuming them.
///
/// The receiver is dropped rather than closed-and-drained: the driver
/// pre-reserves an `OwnedPermit` before each socket read, and `recv()` on a
/// closed receiver stays pending while a permit is outstanding. Awaiting full
/// drainage would therefore outlive the command timeout whenever the server
/// stalls mid-response, hanging the joined future forever.
pub(super) async fn drain_fetch_stream<F>(
    mut rx: tokio::sync::mpsc::Receiver<Result<FetchResponse, Error>>,
    mut on_item: F,
) -> Result<(), Error>
where
    F: FnMut(Result<FetchResponse, Error>) -> Result<bool, Error>,
{
    let mut result = Ok(());
    while let Some(item) = rx.recv().await {
        match on_item(item) {
            Ok(true) => {}
            Ok(false) => break,
            Err(error) => {
                result = Err(error);
                break;
            }
        }
    }

    drop(rx);
    result
}

impl ImapConnection {
    /// SELECT or EXAMINE a mailbox using sync-oriented options.
    ///
    /// QRESYNC parameters are sent when the options carry a complete QRESYNC
    /// cursor AND this connection has QRESYNC enabled. Otherwise it falls
    /// back to CONDSTORE when requested and available. It never issues
    /// `ENABLE`: on a connection that has selected a mailbox before, that is
    /// illegal (RFC 5161 Section 3.1). Connections are brought to QRESYNC
    /// when minted (`account::pool::enable_qresync`); the result's
    /// `qresync` says what happened here.
    ///
    /// Direct API counterpart to Account cursor establishment and changes:
    /// callers keep native IMAP types and own cursor persistence.
    pub async fn select_for_sync(
        &self,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        options: &crate::types::SyncSelectOptions,
        timeout: Duration,
    ) -> Result<crate::types::SyncSelectResult, Error> {
        use crate::types::QresyncOutcome;

        let profile = self.server_profile();
        let qresync = options.qresync_params();
        let outcome = match qresync {
            None => QresyncOutcome::NotRequested,
            Some(_) if profile.enabled("QRESYNC") => QresyncOutcome::Used,
            Some(_) => QresyncOutcome::NotEnabledOnConnection,
        };
        let qresync_used = outcome == QresyncOutcome::Used;
        let condstore_used = !qresync_used && options.condstore && profile.supports_condstore();

        let select_options = if qresync_used {
            SelectOptions::qresync(qresync.expect("checked qresync"))
        } else if condstore_used {
            SelectOptions::condstore()
        } else {
            SelectOptions::default()
        };

        let mailbox = if options.read_only {
            self.examine_with(mailbox, &select_options, timeout).await?
        } else {
            self.select_with(mailbox, &select_options, timeout).await?
        };

        Ok(crate::types::SyncSelectResult {
            mailbox,
            qresync: outcome,
            condstore_used,
        })
    }

    /// Execute a sync-oriented UID FETCH.
    ///
    /// Direct API counterpart to Account change streams: callers provide
    /// native UID sets and receive protocol FETCH data.
    pub async fn sync_fetch(
        &self,
        request: &crate::types::SyncFetchRequest,
        timeout: Duration,
    ) -> Result<crate::types::SyncFetchResult, Error> {
        let sequence_set = request.uids.as_sequence_set();
        if request.include_vanished {
            let Some(mod_seq) = request.changed_since else {
                return Err(Error::InvalidInput(
                    "include_vanished requires changed_since for UID FETCH".into(),
                ));
            };
            let (fetches, vanished) = self
                .uid_fetch_vanished(sequence_set, &request.attrs, mod_seq.get(), timeout)
                .await?;
            Ok(crate::types::SyncFetchResult { fetches, vanished })
        } else if let Some(mod_seq) = request.changed_since {
            let fetches = self
                .uid_fetch_changed_since(sequence_set, &request.attrs, mod_seq.get(), timeout)
                .await?;
            Ok(crate::types::SyncFetchResult {
                fetches,
                vanished: Vec::new(),
            })
        } else {
            let fetches = self
                .uid_fetch(sequence_set, &request.attrs, timeout)
                .await?;
            Ok(crate::types::SyncFetchResult {
                fetches,
                vanished: Vec::new(),
            })
        }
    }

    /// Stream a UID FETCH into a callback without exposing channel ceremony.
    ///
    /// Direct API counterpart to Account hydration: callers consume native
    /// FETCH responses instead of shared HydratedObject batches.
    pub async fn uid_fetch_each<F>(
        &self,
        uids: &crate::types::UidSet,
        attrs: &[FetchAttr],
        timeout: Duration,
        mut on_fetch: F,
    ) -> Result<(), Error>
    where
        F: FnMut(FetchResponse) -> Result<(), Error>,
    {
        let (rx, fetch_fut) = self.uid_fetch_stream(uids.as_sequence_set(), attrs, timeout)?;
        let drain_fut = drain_fetch_stream(rx, |fetch| {
            on_fetch(fetch?)?;
            Ok(true)
        });
        let (fetch_result, drain_result) = tokio::join!(fetch_fut, drain_fut);
        match (drain_result, fetch_result) {
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    /// Collect a UID FETCH with a caller-supplied memory budget.
    ///
    /// The budget is enforced in the FETCH consumer: responses beyond the
    /// budget are not retained, and the method returns [`Error::FetchLimit`]
    /// after the tagged completion. A literal declaration larger than the
    /// remaining budget returns [`Error::FetchLiteralLimit`] before its
    /// payload is read and retires the connection, because the unread literal
    /// cannot be skipped safely. Callers that need strict back-pressure should
    /// use [`uid_fetch_each`](Self::uid_fetch_each).
    /// Both paths frame and parse a whole response before delivery. The
    /// budget is not a ceiling on syntax, parser allocations or peak memory.
    ///
    /// Direct API counterpart to Account get/blob streams for buffered callers.
    pub async fn uid_fetch_limited(
        &self,
        uids: &crate::types::UidSet,
        attrs: &[FetchAttr],
        max_estimated_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.require_state(crate::types::CommandKind::Fetch)?;
        self.validate_requested_fetch_items(attrs)?;
        if uids.as_sequence_set().as_str().contains('$') {
            self.require_searchres()?;
        }
        self.submit_regular(
            Command::UidFetch {
                sequence_set: uids.as_sequence_set().clone(),
                items: format_fetch_attrs(attrs),
                changed_since: None,
                vanished: false,
            },
            dispatch::FetchConsumer::with_limit(max_estimated_bytes),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// Fetch full messages by UID without setting `\Seen`.
    ///
    /// `max_estimated_bytes` caps the buffered response size. Use
    /// [`uid_fetch_each`](Self::uid_fetch_each) for unbounded or large
    /// result sets.
    ///
    /// Direct API counterpart to Account full-message hydration.
    pub async fn uid_fetch_full_messages(
        &self,
        uids: &crate::types::UidSet,
        max_estimated_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        let request = crate::types::SyncFetchRequest::full_messages(uids.clone());
        self.uid_fetch_limited(uids, &request.attrs, max_estimated_bytes, timeout)
            .await
    }

    /// Drain pending typed events and return their sync impacts.
    pub async fn drain_event_impacts(&self) -> Vec<crate::types::EventImpact> {
        self.drain_events()
            .await
            .into_iter()
            .map(|event| event.impact())
            .collect()
    }

    /// Wait for the next event and return its sync impact.
    pub async fn next_event_impact(
        &self,
        timeout: Duration,
    ) -> Result<Option<crate::types::EventImpact>, Error> {
        Ok(self.next_event(timeout).await?.map(|event| event.impact()))
    }
}

#[cfg(test)]
#[path = "ergonomics_tests.rs"]
mod tests;
