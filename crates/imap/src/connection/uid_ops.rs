#![allow(clippy::wildcard_imports)]
use super::*;
use crate::types::validated::ParsedUidSet;

const DEFAULT_FETCH_STREAM_CAPACITY: usize = 64;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Message operations (UID variants)
    // -----------------------------------------------------------------------

    /// UID FETCH (RFC 3501 Section 6.4.5).
    ///
    /// Collects all responses into a `Vec`. Logs a warning when the buffered
    /// data exceeds 10 MB to nudge callers toward the streaming variant for
    /// large result sets.
    pub async fn uid_fetch(
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
            Command::UidFetch {
                sequence_set: sequence_set.clone(),
                items: format_fetch_attrs(items),
                changed_since: None,
                vanished: false,
            },
            None,
            timeout,
        )
        .await
    }

    /// UID FETCH with CHANGEDSINCE modifier (RFC 7162 Section 3.1.4).
    ///
    /// Returns only messages whose mod-sequence is greater than `mod_seq`.
    /// Requires the server to support CONDSTORE (RFC 7162).
    pub async fn uid_fetch_changed_since(
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
            Command::UidFetch {
                sequence_set: sequence_set.clone(),
                items: format_fetch_attrs(items),
                changed_since: Some(mod_seq),
                vanished: false,
            },
            Some(mod_seq),
            timeout,
        )
        .await
    }

    /// UID FETCH with CHANGEDSINCE as a bounded stream.
    #[allow(clippy::type_complexity)]
    pub(crate) fn uid_fetch_changed_since_stream<'a>(
        &'a self,
        sequence_set: &'a SequenceSet,
        items: &'a [FetchAttr],
        mod_seq: u64,
        timeout: Duration,
    ) -> Result<
        (
            tokio::sync::mpsc::Receiver<Result<FetchResponse, Error>>,
            impl std::future::Future<Output = Result<(), Error>> + Send + 'a,
        ),
        Error,
    > {
        self.require_state(crate::types::CommandKind::Fetch)?;
        self.require_condstore()?;
        self.validate_requested_fetch_items(items)?;
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_FETCH_STREAM_CAPACITY);
        let cmd = Command::UidFetch {
            sequence_set: sequence_set.clone(),
            items: format_fetch_attrs(items),
            changed_since: Some(mod_seq),
            vanished: false,
        };
        let fut = self.fetch_stream_bounded_impl(cmd, tx, timeout);
        Ok((rx, fut))
    }

    /// Shared implementation for FETCH and UID FETCH (RFC 3501 Section 6.4.5).
    ///
    /// When `changed_since` is `Some`, validates that the server supports
    /// CONDSTORE (RFC 7162 Section 3.1.4) before issuing the command.
    pub(super) async fn fetch_impl(
        &self,
        cmd: Command,
        changed_since: Option<u64>,
        timeout: Duration,
    ) -> Result<Vec<FetchResponse>, Error> {
        self.require_state(cmd.kind())?;
        if changed_since.is_some() {
            self.require_condstore()?;
        }
        self.submit_regular(
            cmd,
            dispatch::FetchConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID FETCH with CHANGEDSINCE and VANISHED modifiers (RFC 7162 Section 3.2.6).
    ///
    /// Issues `UID FETCH <set> (<items>) (CHANGEDSINCE <mod_seq> VANISHED)`.
    /// The server returns `VANISHED (EARLIER)` responses for UIDs in the
    /// requested set that have been expunged since `mod_seq`, plus regular
    /// FETCH responses for messages whose flags changed.
    ///
    /// `VANISHED (EARLIER)` UIDs are defensively filtered to only include
    /// UIDs within the requested `sequence_set`  -  non-conformant servers
    /// may return UIDs outside the set (RFC 7162 Section 3.2.6). When the
    /// sequence set contains `$` (RFC 5182 search result reference),
    /// filtering is best-effort (skipped, since `$` cannot be resolved
    /// client-side).
    ///
    /// Requires:
    /// - QRESYNC must have been `ENABLE`d (RFC 7162 Section 3.2.6).
    /// - The mailbox must be selected.
    pub async fn uid_fetch_vanished(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        mod_seq: u64,
        timeout: Duration,
    ) -> Result<(Vec<FetchResponse>, Vec<UidRange>), Error> {
        self.require_state(crate::types::CommandKind::Fetch)?;
        self.validate_requested_fetch_items(items)?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        // RFC 7162 Section 3.2.6: VANISHED modifier requires QRESYNC to be ENABLEd.
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
        // Parse the requested set for defensive filtering of VANISHED (EARLIER)
        // UIDs. Returns None when the set contains `$` (RFC 5182), in which case
        // filtering is skipped inside the consumer.
        let parsed_set = ParsedUidSet::new(sequence_set);
        let cmd = Command::UidFetch {
            sequence_set: sequence_set.clone(),
            items: format_fetch_attrs(items),
            changed_since: Some(mod_seq),
            vanished: true,
        };
        self.submit_regular(
            cmd,
            dispatch::FetchVanishedConsumer::new(parsed_set),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID FETCH with CHANGEDSINCE and VANISHED as a bounded stream.
    ///
    /// This has the same wire semantics as [`uid_fetch_vanished`], but
    /// backpressures before reading more responses so a large
    /// `VANISHED (EARLIER)` set is not buffered behind the caller.
    #[allow(clippy::type_complexity)]
    pub(crate) fn uid_fetch_vanished_stream<'a>(
        &'a self,
        sequence_set: &'a SequenceSet,
        items: &'a [FetchAttr],
        mod_seq: u64,
        timeout: Duration,
    ) -> Result<
        (
            tokio::sync::mpsc::Receiver<Result<dispatch::FetchStreamItem, Error>>,
            impl std::future::Future<Output = Result<(), Error>> + Send + 'a,
        ),
        Error,
    > {
        self.require_state(crate::types::CommandKind::Fetch)?;
        self.validate_requested_fetch_items(items)?;
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
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
        let parsed_set = ParsedUidSet::new(sequence_set);
        let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_FETCH_STREAM_CAPACITY);
        let cmd = Command::UidFetch {
            sequence_set: sequence_set.clone(),
            items: format_fetch_attrs(items),
            changed_since: Some(mod_seq),
            vanished: true,
        };
        let fut = async move {
            self.submit_streaming(
                cmd,
                dispatch::BoundedStreamingFetchVanishedConsumer::new(tx, parsed_set),
                Some(tokio::time::Instant::now() + timeout),
            )
            .await
        };
        Ok((rx, fut))
    }

    /// UID FETCH streaming (RFC 3501 Section 6.4.5).
    ///
    /// Returns a bounded receiver and the driver-side future that must be
    /// polled to execute the command. When the receiver is not drained, the
    /// driver stops reading before the bounded channel fills further.
    /// Each item is a complete, parsed response; channel capacity does not
    /// bound the size of a single response or emit a literal incrementally.
    #[allow(clippy::type_complexity)]
    pub fn uid_fetch_stream<'a>(
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
        self.uid_fetch_stream_with_capacity(
            sequence_set,
            items,
            DEFAULT_FETCH_STREAM_CAPACITY,
            timeout,
        )
    }

    /// UID FETCH streaming with an explicit bounded-channel capacity.
    #[allow(clippy::type_complexity)]
    pub fn uid_fetch_stream_with_capacity<'a>(
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
        self.uid_fetch_stream_budgeted(sequence_set, items, capacity, None, timeout)
    }

    /// Streaming variant whose caller budget reaches the reader before decoding.
    #[allow(clippy::type_complexity)]
    pub(crate) fn uid_fetch_stream_budgeted<'a>(
        &'a self,
        sequence_set: &'a SequenceSet,
        items: &'a [FetchAttr],
        capacity: usize,
        budget: Option<usize>,
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
        let cmd = Command::UidFetch {
            sequence_set: sequence_set.clone(),
            items: format_fetch_attrs(items),
            changed_since: None,
            vanished: false,
        };
        let fut = async move {
            self.require_state(cmd.kind())?;
            self.submit_streaming(
                cmd,
                dispatch::BoundedStreamingFetchConsumer::with_budget(tx, budget),
                Some(tokio::time::Instant::now() + timeout),
            )
            .await
        };
        Ok((rx, fut))
    }

    /// UID FETCH streaming (RFC 3501 Section 6.4.5).
    ///
    /// Pushes each [`FetchResponse`] through the provided unbounded channel as
    /// it arrives from the server, rather than buffering the entire result set
    /// in memory. The channel is closed when the tagged OK is received.
    ///
    /// If the receiver is dropped, the driver still drains the command to the
    /// tagged completion so the IMAP stream remains synchronized.
    ///
    /// Prefer this over [`uid_fetch`](Self::uid_fetch) when the result set
    /// may be large enough to cause memory pressure.
    pub async fn uid_fetch_streaming(
        &self,
        sequence_set: &SequenceSet,
        items: &[FetchAttr],
        tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
        timeout: Duration,
    ) -> Result<(), Error> {
        let (rx, fetch_fut) = self.uid_fetch_stream(sequence_set, items, timeout)?;
        let drain_fut = super::ergonomics::drain_fetch_stream(rx, |item| Ok(tx.send(item).is_ok()));
        let (fetch_result, drain_result) = tokio::join!(fetch_fut, drain_fut);
        drain_result?;
        fetch_result
    }

    /// Shared implementation for streaming FETCH and UID FETCH
    /// (RFC 3501 Section 6.4.5).
    ///
    /// Validates session state and dispatches the command with a
    /// bounded streaming consumer that pushes each response through `tx`.
    pub(super) async fn fetch_stream_bounded_impl(
        &self,
        cmd: Command,
        tx: tokio::sync::mpsc::Sender<Result<FetchResponse, Error>>,
        timeout: Duration,
    ) -> Result<(), Error> {
        self.require_state(cmd.kind())?;
        self.submit_streaming(
            cmd,
            dispatch::BoundedStreamingFetchConsumer::new(tx),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID SEARCH (RFC 3501 Section 6.4.4).
    ///
    /// `criteria` is the raw IMAP search criteria string, e.g. `"UNSEEN"` or
    /// `"FROM \"alice\" SINCE 1-Mar-2026"`.
    /// Returns a [`SearchResult`] containing matching UIDs and an optional
    /// MODSEQ value (RFC 7162 Section 3.1.5).
    /// Handles both SEARCH and ESEARCH responses. For ESEARCH, the ALL uid-set
    /// is expanded into individual UIDs for backward compatibility.
    ///
    /// Accepts anything that implements `AsRef<str>`, including `&str`,
    /// `String`, and [`SearchCriteria`](crate::types::SearchCriteria).
    pub async fn uid_search(
        &self,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<SearchResult, Error> {
        let criteria = criteria.as_ref();
        self.search_impl(
            Command::UidSearch {
                criteria: criteria.to_owned(),
            },
            "UID SEARCH",
            timeout,
        )
        .await
    }

    /// Shared implementation for SEARCH and UID SEARCH (RFC 3501 Section 6.4.4).
    pub(super) async fn search_impl(
        &self,
        cmd: Command,
        _label: &str,
        timeout: Duration,
    ) -> Result<SearchResult, Error> {
        self.require_state(cmd.kind())?;
        // RFC 6855 Section 6: SEARCH criteria are free-form strings, so a
        // UTF8=ONLY server requires ENABLE UTF8=ACCEPT before SEARCH.
        self.check_utf8_only_enforced()?;
        self.submit_regular(
            cmd,
            dispatch::SearchConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID SEARCH with RETURN options (RFC 4731 Section 3.2).
    ///
    /// Returns the full [`EsearchResponse`] with MIN, MAX, COUNT, and ALL fields.
    /// Requires the server to advertise the `ESEARCH` capability.
    ///
    /// RFC 4731 Section 3.2: an extended SEARCH with RETURN options causes the
    /// server to return a single ESEARCH response instead of a SEARCH response.
    ///
    /// Accepts anything that implements `AsRef<str>`, including `&str`,
    /// `String`, and [`SearchCriteria`](crate::types::SearchCriteria).
    pub async fn uid_search_esearch(
        &self,
        criteria: impl AsRef<str>,
        return_opts: &[&str],
        timeout: Duration,
    ) -> Result<EsearchResponse, Error> {
        let criteria = criteria.as_ref();
        self.search_esearch_impl(
            Command::UidSearchReturn {
                criteria: criteria.to_owned(),
                return_opts: return_opts.iter().map(|s| (*s).to_owned()).collect(),
            },
            "UID SEARCH RETURN",
            timeout,
        )
        .await
    }

    /// Shared implementation for SEARCH RETURN and UID SEARCH RETURN
    /// (RFC 4731 Section 3.2).
    ///
    /// Requires the server to advertise the `ESEARCH` capability, or to be
    /// running `IMAP4rev2` (RFC 9051 Section 6.4.4).
    pub(super) async fn search_esearch_impl(
        &self,
        cmd: Command,
        _label: &str,
        timeout: Duration,
    ) -> Result<EsearchResponse, Error> {
        self.require_state(cmd.kind())?;
        // RFC 6855 Section 6: ESEARCH is an extended SEARCH form and uses the
        // same free-form criteria strings, so it must honor UTF8=ONLY too.
        self.check_utf8_only_enforced()?;
        {
            let snap = self.state_rx.borrow();
            // ESEARCH is a base feature in IMAP4rev2 (RFC 9051 Section 6.4.4).
            if !super::auth::snapshot_supports(&snap, &Capability::Esearch) {
                return Err(Error::MissingCapability("ESEARCH".into()));
            }
        }
        // RFC 5182 Section 2 extends SEARCH RETURN with the SAVE result option.
        // Generic ESEARCH callers can request SAVE through return_opts, so
        // SEARCHRES remains mandatory unless IMAP4rev2 folds it into the base
        // protocol (RFC 9051 Appendix E).
        if Self::search_return_requests_save(&cmd) {
            self.require_searchres()?;
        }
        self.submit_regular(
            cmd,
            dispatch::EsearchConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID STORE (RFC 3501 Section 6.4.6).
    ///
    /// If `unchanged_since` is `Some`, uses CONDSTORE (RFC 7162 Section 3.1.3)
    /// to avoid overwriting concurrent flag changes.
    ///
    /// `\Recent` and `\*` are silently filtered per RFC 3501 Section 2.3.2
    /// (they are not valid in STORE flag lists).
    pub async fn uid_store(
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
            Command::UidStore {
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

    /// Shared implementation for STORE and UID STORE (RFC 3501 Section 6.4.6).
    ///
    /// Validates session state, optionally checks CONDSTORE, executes the
    /// command, and collects the resulting FETCH responses and response code.
    pub(super) async fn store_impl(
        &self,
        cmd: Command,
        unchanged_since: Option<u64>,
        timeout: Duration,
    ) -> Result<StoreResult, Error> {
        self.require_state(cmd.kind())?;
        if unchanged_since.is_some() {
            self.require_condstore()?;
        }
        self.submit_regular(
            cmd,
            dispatch::StoreConsumer::new(unchanged_since),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID MOVE (RFC 6851 Section 3, RFC 9051 Appendix E item 2).
    ///
    /// Falls back to COPY + Store `\Deleted` + UID EXPUNGE (RFC 6851 Section 4)
    /// when the server doesn't advertise the MOVE capability and isn't `IMAP4rev2`.
    /// The fallback requires UIDPLUS (RFC 4315)  -  plain EXPUNGE is never used
    /// because it can delete unrelated `\Deleted` messages (data-loss risk).
    ///
    /// Returns a [`MoveResult`] containing:
    /// - `code`: the server's response code (see [`MoveResult::code`]). On
    ///   the fallback path, the final UID EXPUNGE's tagged code, else the
    ///   COPY's `code`.
    /// - `copy_uid`: the `[COPYUID ...]` code per RFC 6851 Section 4.3 /
    ///   RFC 4315 Section 3, tagged or untagged, even when the tagged OK
    ///   carries a different code. On the fallback path, the COPY's.
    /// - `expunged`: the EXPUNGE or VANISHED responses (RFC 6851 Section 3).
    ///   When QRESYNC is enabled (RFC 7162 Section 3.2.10), the server sends
    ///   VANISHED instead of EXPUNGE.
    pub async fn uid_move_messages(
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

        // Read capabilities from snapshot to decide which path to take.
        let (has_move, has_uidplus) = {
            let snap = self.state_rx.borrow();
            (
                super::auth::snapshot_supports(&snap, &Capability::Move),
                super::auth::snapshot_supports(&snap, &Capability::UidPlus),
            )
        };

        // MOVE is a base feature in IMAP4rev2 (RFC 9051 Appendix E item 2).
        if has_move {
            let mbox = mailbox.mailbox_name()?;
            self.move_native_impl(
                Command::UidMove {
                    sequence_set: sequence_set.clone(),
                    mailbox: mbox,
                },
                timeout,
            )
            .await
        } else if has_uidplus {
            // Fallback: COPY + STORE +FLAGS.SILENT \Deleted + UID EXPUNGE
            // per RFC 6851 Section 3.3.  The `.SILENT` modifier is required
            // to suppress the implicit untagged FETCH responses that a plain
            // `+FLAGS` would trigger (RFC 3501 Section 6.4.6).  Only safe
            // with UID EXPUNGE which targets specific UIDs.
            // Capture the COPYUID response code from uid_copy
            // (RFC 6851 Section 4.3, RFC 4315 Section 3).
            let copy_result = self.uid_copy(sequence_set, mailbox, timeout).await?;
            self.uid_store(
                sequence_set,
                StoreOperation::AddSilent,
                &[Flag::Deleted],
                Option::None,
                timeout,
            )
            .await
            .map_err(after_acknowledged_side_effect)?;
            let (expunged, expunge_code) = self
                .uid_expunge_with_code(sequence_set, timeout)
                .await
                .map_err(after_acknowledged_side_effect)?;
            // `code` mirrors the native path, where it is the MOVE's tagged
            // code: here the final UID EXPUNGE's tagged OK is that
            // counterpart (under QRESYNC, the source mailbox's new
            // HIGHESTMODSEQ). The COPY's code is only the fallback when the
            // EXPUNGE carried none; its COPYUID is not lost either way,
            // because `copy_uid` carries it.
            Ok(MoveResult {
                code: expunge_code.or(copy_result.code),
                copy_uid: copy_result.copy_uid,
                expunged,
            })
        } else {
            // No MOVE, no UIDPLUS  -  cannot safely move messages.
            // Plain EXPUNGE would delete ALL \Deleted messages, not just the
            // requested set (RFC 9051 Section 6.4.3).
            Err(Error::MissingCapability(
                "MOVE or UIDPLUS (plain EXPUNGE is unsafe for move fallback)".into(),
            ))
        }
    }

    /// Shared implementation for the native MOVE path used by both
    /// `move_messages()` and `uid_move_messages()` (RFC 6851 Section 3).
    ///
    /// Executes the MOVE command and collects EXPUNGE/VANISHED responses.
    pub(super) async fn move_native_impl(
        &self,
        cmd: Command,
        timeout: Duration,
    ) -> Result<MoveResult, Error> {
        self.submit_regular(
            cmd,
            dispatch::MoveConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID COPY (RFC 3501 Section 6.4.7, RFC 4315 Section 3).
    ///
    /// On success, returns a [`CopyResult`] carrying the tagged OK's response
    /// code and, separately in `copy_uid`, the
    /// `[COPYUID uid-validity source-uids dest-uids]` code (RFC 4315
    /// Section 3), whether the server sent it tagged or untagged.
    pub async fn uid_copy(
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
            Command::UidCopy {
                sequence_set: sequence_set.clone(),
                mailbox: mbox,
            },
            timeout,
        )
        .await
    }

    /// Shared implementation for COPY and UID COPY (RFC 3501 Section 6.4.7,
    /// RFC 4315 Section 3).
    ///
    /// On success, returns a [`CopyResult`]; see [`uid_copy`](Self::uid_copy).
    pub(super) async fn copy_impl(
        &self,
        cmd: Command,
        timeout: Duration,
    ) -> Result<CopyResult, Error> {
        self.require_state(cmd.kind())?;
        self.check_utf8_only_enforced()?;
        self.submit_regular(
            cmd,
            dispatch::CopyConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID EXPUNGE (RFC 4315 UIDPLUS / RFC 9051 Section 6.4.9).
    pub async fn uid_expunge(
        &self,
        sequence_set: &SequenceSet,
        timeout: Duration,
    ) -> Result<ExpungeResult, Error> {
        let cmd = self.uid_expunge_command(sequence_set)?;
        self.submit_regular(
            cmd,
            dispatch::ExpungeConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// UID EXPUNGE that also returns the tagged OK's response code, for the
    /// UID MOVE fallback: under QRESYNC that is the source mailbox's new
    /// `HIGHESTMODSEQ` (RFC 7162), the counterpart of the native MOVE's
    /// tagged code.
    async fn uid_expunge_with_code(
        &self,
        sequence_set: &SequenceSet,
        timeout: Duration,
    ) -> Result<(ExpungeResult, Option<ResponseCode>), Error> {
        let cmd = self.uid_expunge_command(sequence_set)?;
        self.submit_regular(
            cmd,
            dispatch::ExpungeConsumer::with_tagged_code(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }

    /// The local admission checks for UID EXPUNGE, then the command.
    fn uid_expunge_command(&self, sequence_set: &SequenceSet) -> Result<Command, Error> {
        self.require_state(crate::types::CommandKind::Expunge)?;
        // RFC 5182 Section 2: `$` references saved search results and requires SEARCHRES.
        if sequence_set.as_str().contains('$') {
            self.require_searchres()?;
        }
        {
            let snap = self.state_rx.borrow();
            // RFC 4315 Section 2: UID EXPUNGE requires the UIDPLUS extension.
            // RFC 9051 Appendix E item 3: IMAP4rev2 incorporates UIDPLUS into
            // the base command set, so rev2 servers implicitly support it.
            if !super::auth::snapshot_supports(&snap, &Capability::UidPlus) {
                return Err(Error::MissingCapability("UIDPLUS".into()));
            }
        }
        Ok(Command::UidExpunge {
            sequence_set: sequence_set.clone(),
        })
    }

    /// EXPUNGE (RFC 3501 Section 6.4.3 / RFC 7162 Section 3.2.10).
    ///
    /// Returns every removal the server reported, in arrival order and in
    /// the form it arrived: sequence numbers (`* n EXPUNGE`, RFC 3501
    /// Section 7.4.1), or UID ranges (`* VANISHED`, which a QRESYNC-enabled
    /// connection receives for a mailbox with persistent mod-sequences, RFC
    /// 7162 Section 3.2.10).
    pub async fn expunge(&self, timeout: Duration) -> Result<ExpungeResult, Error> {
        self.require_state(crate::types::CommandKind::Expunge)?;
        self.submit_regular(
            Command::Expunge,
            dispatch::ExpungeConsumer::new(),
            Some(tokio::time::Instant::now() + timeout),
        )
        .await
    }
}

/// A step of a composite that runs after a non-idempotent step was
/// acknowledged must not report the composite as unsent: the earlier side
/// effect happened. In the UID MOVE fallback the COPY is that step, and a
/// MOVE settled as unsent is retried, repeating the COPY and duplicating
/// messages. Restamp evidence that reads `Unsent` (explicit, or absent on a
/// variant that can carry it) as `InFlight`, so the operation is read back
/// rather than replayed. Acknowledged evidence is left alone, and variants
/// that cannot carry evidence (`StateChangedBeforeSend`, local refusals such
/// as `MissingCapability`) pass through unchanged: `with_attempt` is a no-op
/// for them. Those remaining cases need an aggregate partial-completion
/// contract, which leaf transmission evidence cannot express.
fn after_acknowledged_side_effect(error: Error) -> Error {
    match error.attempt() {
        None | Some(bifrost_types::TransmissionState::Unsent) => {
            error.with_attempt(bifrost_types::TransmissionState::InFlight)
        }
        Some(_) => error,
    }
}

#[cfg(test)]
#[path = "uid_ops_tests.rs"]
mod tests;
