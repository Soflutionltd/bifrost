#![allow(clippy::wildcard_imports)]
use super::*;

const DEFAULT_FETCH_STREAM_CAPACITY: usize = 64;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Message operations (sequence-number variants)
    // -----------------------------------------------------------------------

    /// FETCH by sequence number (RFC 3501 Section 6.4.5).
    ///
    /// Prefer [`uid_fetch`](Self::uid_fetch) for stable message references.
    pub async fn fetch(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.validate_requested_fetch_items(items)?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        self.fetch_impl(
            Command::Fetch {
                sequence_set: sequence_set.clone(),
                items: format_fetch_attrs(items),
                changed_since: None,
            },
            None,
            timeout,
        )
        .await
    }

    /// FETCH with CHANGEDSINCE modifier by sequence number (RFC 7162 Section 3.1.4).
    ///
    /// Returns only messages whose mod-sequence is greater than `mod_seq`.
    /// Requires the server to support CONDSTORE (RFC 7162).
    /// Prefer [`uid_fetch_changed_since`](Self::uid_fetch_changed_since) for stable
    /// message references.
    pub async fn fetch_changed_since(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        mod_seq: u64,
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.validate_requested_fetch_items(items)?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        self.fetch_impl(
            Command::Fetch {
                sequence_set: sequence_set.clone(),
                items: format_fetch_attrs(items),
                changed_since: Some(mod_seq),
            },
            Some(mod_seq),
            timeout,
        )
        .await
    }

    /// FETCH streaming by sequence number (RFC 3501 Section 6.4.5).
    ///
    /// Returns a bounded receiver and the driver-side future that must be
    /// polled to execute the command.
    #[allow(clippy::type_complexity)]
    pub fn fetch_stream<'a>(
        &'a self,
        sequence_set: &'a SequenceSet,
        items: &'a [FetchAttr],
        timeout: Duration,
    ) -> Result<
        (
            tokio::sync::mpsc::Receiver<Result<FetchResponse, Error>>,
            impl std::future::Future<Output = Result<(), Error>> + Send + 'a,
        ),
        Error,
    > {
        self.fetch_stream_with_capacity(sequence_set, items, DEFAULT_FETCH_STREAM_CAPACITY, timeout)
    }

    /// FETCH streaming by sequence number with explicit bounded capacity.
    #[allow(clippy::type_complexity)]
    pub fn fetch_stream_with_capacity<'a>(
        &'a self,
        sequence_set: &'a SequenceSet,
        items: &'a [FetchAttr],
        capacity: usize,
        timeout: Duration,
    ) -> Result<
        (
            tokio::sync::mpsc::Receiver<Result<FetchResponse, Error>>,
            impl std::future::Future<Output = Result<(), Error>> + Send + 'a,
        ),
        Error,
    > {
        self.validate_requested_fetch_items(items)?;
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
        let cmd = Command::Fetch {
            sequence_set: sequence_set.clone(),
            items: format_fetch_attrs(items),
            changed_since: None,
        };
        let fut = self.fetch_stream_bounded_impl(cmd, tx, timeout);
        Ok((rx, fut))
    }

    /// FETCH streaming by sequence number (RFC 3501 Section 6.4.5).
    ///
    /// Pushes each [`FetchResponse`] through the provided unbounded channel as
    /// it arrives from the server, rather than buffering the entire result set
    /// in memory. The channel is closed when the tagged OK is received.
    ///
    /// If the receiver is dropped, the driver still drains the command to the
    /// tagged completion so the IMAP stream remains synchronized.
    ///
    /// Prefer [`uid_fetch_streaming`](Self::uid_fetch_streaming) for stable
    /// message references.
    pub async fn fetch_streaming(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
        timeout: Duration,
    ) -> Result<(), Error> {
        let (rx, fetch_fut) = self.fetch_stream(sequence_set, items, timeout)?;
        let drain_fut = super::ergonomics::drain_fetch_stream(rx, |item| Ok(tx.send(item).is_ok()));
        let (fetch_result, drain_result) = tokio::join!(fetch_fut, drain_fut);
        drain_result?;
        fetch_result
    }

    /// SEARCH by sequence number (RFC 3501 Section 6.4.4).
    ///
    /// Returns a [`SearchResult`] containing matching sequence numbers and an
    /// optional MODSEQ value (RFC 7162 Section 3.1.5).
    /// Prefer [`uid_search`](Self::uid_search) for stable message references.
    /// Handles both SEARCH and ESEARCH responses. For ESEARCH, the ALL uid-set
    /// is expanded into individual sequence numbers for backward compatibility.
    ///
    /// Accepts anything that implements `AsRef<str>`, including `&str`,
    /// `String`, and [`SearchCriteria`](crate::types::SearchCriteria).
    pub async fn search(
        &self,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<SearchResult, Error> {
        let criteria = criteria.as_ref();
        self.search_impl(
            Command::Search {
                criteria: criteria.to_owned(),
            },
            "SEARCH",
            timeout,
        )
        .await
    }

    /// SEARCH with RETURN options (RFC 4731 Section 3.2).
    ///
    /// Returns the full [`EsearchResponse`] with MIN, MAX, COUNT, and ALL fields.
    /// Requires the server to advertise the `ESEARCH` capability.
    ///
    /// RFC 4731 Section 3.2: an extended SEARCH with RETURN options causes the
    /// server to return a single ESEARCH response instead of a SEARCH response.
    ///
    /// Accepts anything that implements `AsRef<str>`, including `&str`,
    /// `String`, and [`SearchCriteria`](crate::types::SearchCriteria).
    pub async fn search_esearch(
        &self,
        criteria: impl AsRef<str>,
        return_opts: &[&str],
        timeout: Duration,
    ) -> Result<EsearchResponse, Error> {
        let criteria = criteria.as_ref();
        self.search_esearch_impl(
            Command::SearchReturn {
                criteria: criteria.to_owned(),
                return_opts: return_opts.iter().map(|s| (*s).to_owned()).collect(),
            },
            "SEARCH RETURN",
            timeout,
        )
        .await
    }

    /// SEARCH RETURN (SAVE) by sequence number (RFC 5182 Section 2).
    ///
    /// Saves the search results server-side as `$` for use in subsequent commands.
    /// Does not return UIDs  -  the results are stored on the server.
    ///
    /// Accepts anything that implements `AsRef<str>`, including `&str`,
    /// `String`, and [`SearchCriteria`](crate::types::SearchCriteria).
    pub async fn search_save(
        &self,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<(), Error> {
        let criteria = criteria.as_ref();
        self.search_save_impl(
            criteria,
            Command::SearchSave {
                criteria: criteria.to_owned(),
            },
            timeout,
        )
        .await
    }

    /// UID SEARCH RETURN (SAVE) (RFC 5182 Section 2).
    ///
    /// Saves the search results server-side as `$` for use in subsequent commands.
    /// Does not return UIDs  -  the results are stored on the server.
    ///
    /// Accepts anything that implements `AsRef<str>`, including `&str`,
    /// `String`, and [`SearchCriteria`](crate::types::SearchCriteria).
    pub async fn uid_search_save(
        &self,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<(), Error> {
        let criteria = criteria.as_ref();
        self.search_save_impl(
            criteria,
            Command::UidSearchSave {
                criteria: criteria.to_owned(),
            },
            timeout,
        )
        .await
    }

    /// Shared implementation for SEARCH RETURN (SAVE) and UID SEARCH RETURN (SAVE)
    /// (RFC 5182 Section 2).
    pub(super) async fn search_save_impl(
        &self,
        _criteria: &str,
        cmd: Command,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.require_state(cmd.kind())?;
        // RFC 6855 Section 6: SEARCH RETURN (SAVE) remains part of the SEARCH
        // command family and must be rejected until UTF8=ACCEPT is enabled.
        self.check_utf8_only_enforced()?;
        // RFC 5182 Section 2: SEARCHRES capability is required.
        self.require_searchres()?;
        self.submit_regular(
            cmd,
            dispatch::SearchSaveConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// STORE by sequence number (RFC 3501 Section 6.4.6).
    ///
    /// Prefer [`uid_store`](Self::uid_store) for stable message references.
    ///
    /// `\Recent` and `\*` are silently filtered per RFC 3501 Section 2.3.2
    /// (they are not valid in STORE flag lists).
    pub async fn store(
        &self,
        sequence_set: &SequenceSet,
        operation: StoreOperation,
        flags: &[Flag],
        unchanged_since: Option<u64>,
        timeout: Duration,
    ) -> Result<StoreResult, Error> {
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        // RFC 3501 Section 2.3.2 / Section 9: \Recent is server-only and \*
        // is not valid in STORE flag lists. Filter them out before encoding,
        // consistent with append() (RFC 3501 Section 6.3.11).
        let filtered_flags = filter_store_flags(flags);
        self.store_impl(
            Command::Store {
                sequence_set: sequence_set.clone(),
                operation,
                flags: filtered_flags,
                unchanged_since,
            },
            unchanged_since,
            timeout,
        )
        .await
    }

    /// COPY by sequence number (RFC 3501 Section 6.4.7, RFC 4315 Section 3).
    ///
    /// Prefer [`uid_copy`](Self::uid_copy) for stable message references.
    ///
    /// On success, returns a [`CopyResult`] carrying the tagged OK's response
    /// code and, separately in `copy_uid`, the
    /// `[COPYUID uid-validity source-uids dest-uids]` code (RFC 4315
    /// Section 3), whether the server sent it tagged or untagged.
    pub async fn copy(
        &self,
        sequence_set: &SequenceSet,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<CopyResult, Error> {
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        let mbox = mailbox.mailbox_name()?;
        self.copy_impl(
            Command::Copy {
                sequence_set: sequence_set.clone(),
                mailbox: mbox,
            },
            timeout,
        )
        .await
    }

    /// MOVE by sequence number (RFC 6851 Section 3, RFC 6851 Section 4.3).
    ///
    /// Prefer [`uid_move_messages`](Self::uid_move_messages) for stable message references.
    ///
    /// Returns a [`MoveResult`] containing:
    /// - `code`: the server's response code (see [`MoveResult::code`]).
    /// - `copy_uid`: the `[COPYUID ...]` code per RFC 6851 Section 4.3 /
    ///   RFC 4315 Section 3, tagged or untagged, even when the tagged OK
    ///   carries a different code.
    /// - `expunged`: the EXPUNGE or VANISHED responses sent before the tagged
    ///   OK (RFC 6851 Section 3). When QRESYNC is enabled (RFC 7162
    ///   Section 3.2.10), the server sends VANISHED instead of EXPUNGE.
    pub async fn move_messages(
        &self,
        sequence_set: &SequenceSet,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<MoveResult, Error> {
        self.require_state(crate::types::CommandKind::Move)?;
        self.check_utf8_only_enforced()?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        // MOVE is a base feature in IMAP4rev2 (RFC 9051 Appendix E item 2).
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Move) {
                return Err(Error::MissingCapability("MOVE".into()));
            }
        }
        let mbox = mailbox.mailbox_name()?;
        self.move_native_impl(
            Command::Move {
                sequence_set: sequence_set.clone(),
                mailbox: mbox,
            },
            timeout,
        )
        .await
    }
}
