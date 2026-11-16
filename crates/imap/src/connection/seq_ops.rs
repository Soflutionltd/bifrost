#![allow(clippy::wildcard_imports)]
use super::dispatch::filter_store_flags;
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
        sequence_set: &SequenceSet<crate::types::Seq>,
        items: &[FetchAttr],
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.execute_request(
            super::request::Fetch {
                sequence_set: sequence_set.clone(),
                items: items.to_vec().into(),
                changed_since: None,
            },
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
        sequence_set: &SequenceSet<crate::types::Seq>,
        items: &[FetchAttr],
        mod_seq: crate::types::ModSeq,
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.execute_request(
            super::request::Fetch {
                sequence_set: sequence_set.clone(),
                items: items.to_vec().into(),
                changed_since: Some(mod_seq),
            },
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
        sequence_set: &'a SequenceSet<crate::types::Seq>,
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
        sequence_set: &'a SequenceSet<crate::types::Seq>,
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
        let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
        let cmd = Command::Fetch {
            sequence_set: sequence_set.clone(),
            items: items.to_vec().into(),
            changed_since: None,
        };
        self.admit_command(&cmd)?;
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
        sequence_set: &SequenceSet<crate::types::Seq>,
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
        criteria: impl Into<crate::types::SearchCriteria>,
        timeout: Duration,
    ) -> Result<SearchResult<crate::types::Seq>, Error> {
        let criteria = criteria.into();
        self.execute_request(super::request::Search { criteria }, timeout)
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
        criteria: impl Into<crate::types::SearchCriteria>,
        return_opts: &[&str],
        timeout: Duration,
    ) -> Result<EsearchResponse, Error> {
        let criteria = criteria.into();
        self.execute_request(
            super::request::SearchReturn {
                criteria,
                return_opts: return_opts.iter().map(|s| (*s).into()).collect(),
            },
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
        criteria: impl Into<crate::types::SearchCriteria>,
        timeout: Duration,
    ) -> Result<(), Error> {
        let criteria = criteria.into();
        self.execute_request(super::request::SearchSave { criteria }, timeout)
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
        criteria: impl Into<crate::types::SearchCriteria>,
        timeout: Duration,
    ) -> Result<(), Error> {
        let criteria = criteria.into();
        self.execute_request(super::request::UidSearchSave { criteria }, timeout)
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
        sequence_set: &SequenceSet<crate::types::Seq>,
        operation: StoreOperation,
        flags: &[Flag],
        unchanged_since: Option<crate::types::ModSeqValzer>,
        timeout: Duration,
    ) -> Result<StoreResult, Error> {
        // RFC 3501 Section 2.3.2 / Section 9: \Recent is server-only and \*
        // is not valid in STORE flag lists. Filter them out before encoding,
        // consistent with append() (RFC 3501 Section 6.3.11).
        let filtered_flags = filter_store_flags(flags);
        self.execute_request(
            super::request::Store {
                sequence_set: sequence_set.clone(),
                operation,
                flags: filtered_flags,
                unchanged_since,
            },
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
        sequence_set: &SequenceSet<crate::types::Seq>,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<CopyResult, Error> {
        let mbox = mailbox.mailbox_name()?;
        self.execute_request(
            super::request::Copy {
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
        sequence_set: &SequenceSet<crate::types::Seq>,
        mailbox: &(impl crate::types::MailboxInput + ?Sized),
        timeout: Duration,
    ) -> Result<MoveResult, Error> {
        // MOVE is a base feature in IMAP4rev2 (RFC 9051 Appendix E item 2).

        let mbox = mailbox.mailbox_name()?;
        self.execute_request(
            super::request::Move {
                sequence_set: sequence_set.clone(),
                mailbox: mbox,
            },
            timeout,
        )
        .await
    }
}
