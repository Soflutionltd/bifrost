#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // SORT (RFC 5256 Section 2)
    // -----------------------------------------------------------------------

    /// SORT  -  server-side sorting by sequence number (RFC 5256 Section 2).
    ///
    /// Returns matching sequence numbers sorted by the given sort keys,
    /// along with an optional MODSEQ value (RFC 7162 Section 3.1.5).
    ///
    /// `sort_criteria` is a space-separated list of sort keys (e.g.
    /// `"REVERSE DATE SUBJECT"`). The encoder wraps them in parentheses
    /// per the SORT ABNF: `sort-criteria = "(" sort-key *(SP sort-key) ")"`.
    ///
    /// Requires the `SORT` capability (RFC 5256 Section 1).
    pub async fn sort(
        &self,
        sort_criteria: &str,
        charset: &str,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<SearchResult, Error> {
        let criteria = criteria.as_ref();
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Sort)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Sort) {
                return Err(Error::MissingCapability("SORT".into()));
            }
        }
        let cmd = Command::Sort {
            sort_criteria: sort_criteria.to_owned(),
            charset: charset.to_owned(),
            criteria: criteria.to_owned(),
        };
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, super::dispatch::SortConsumer::default()),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
    }

    /// UID SORT  -  server-side sorting returning UIDs (RFC 5256 Section 2).
    ///
    /// Like [`sort`](Self::sort) but returns UIDs instead of sequence
    /// numbers.
    ///
    /// Requires the `SORT` capability (RFC 5256 Section 1).
    pub async fn uid_sort(
        &self,
        sort_criteria: &str,
        charset: &str,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<SearchResult, Error> {
        let criteria = criteria.as_ref();
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Sort)?;
        {
            let snap = self.state_rx.borrow();
            if !super::auth::snapshot_supports(&snap, &Capability::Sort) {
                return Err(Error::MissingCapability("SORT".into()));
            }
        }
        let cmd = Command::UidSort {
            sort_criteria: sort_criteria.to_owned(),
            charset: charset.to_owned(),
            criteria: criteria.to_owned(),
        };
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, super::dispatch::SortConsumer::default()),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
    }

    // -----------------------------------------------------------------------
    // THREAD (RFC 5256 Section 3)
    // -----------------------------------------------------------------------

    /// THREAD  -  server-side threading by sequence number
    /// (RFC 5256 Section 3).
    ///
    /// Returns a tree of [`ThreadNode`]s representing the threading
    /// structure of matching messages.
    ///
    /// `algorithm` is the threading algorithm (e.g. `"REFERENCES"`,
    /// `"ORDEREDSUBJECT"`). Requires the server to advertise
    /// `THREAD=<algorithm>` (RFC 5256 Section 1).
    pub async fn thread(
        &self,
        algorithm: &str,
        charset: &str,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<Vec<ThreadNode>, Error> {
        let criteria = criteria.as_ref();
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Thread)?;
        {
            let snap = self.state_rx.borrow();
            if !snap
                .capabilities
                .contains(&Capability::Thread(algorithm.to_uppercase()))
            {
                return Err(Error::MissingCapability(format!(
                    "THREAD={}",
                    algorithm.to_uppercase()
                )));
            }
        }
        let cmd = Command::Thread {
            algorithm: algorithm.to_owned(),
            charset: charset.to_owned(),
            criteria: criteria.to_owned(),
        };
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, super::dispatch::ThreadConsumer::default()),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
    }

    /// UID THREAD  -  server-side threading returning UIDs
    /// (RFC 5256 Section 3).
    ///
    /// Like [`thread`](Self::thread) but returns UIDs instead of sequence
    /// numbers in the [`ThreadNode`] tree.
    ///
    /// Requires the server to advertise `THREAD=<algorithm>`
    /// (RFC 5256 Section 1).
    pub async fn uid_thread(
        &self,
        algorithm: &str,
        charset: &str,
        criteria: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<Vec<ThreadNode>, Error> {
        let criteria = criteria.as_ref();
        self.check_utf8_only_enforced()?;
        self.require_state(crate::types::CommandKind::Thread)?;
        {
            let snap = self.state_rx.borrow();
            if !snap
                .capabilities
                .contains(&Capability::Thread(algorithm.to_uppercase()))
            {
                return Err(Error::MissingCapability(format!(
                    "THREAD={}",
                    algorithm.to_uppercase()
                )));
            }
        }
        let cmd = Command::UidThread {
            algorithm: algorithm.to_owned(),
            charset: charset.to_owned(),
            criteria: criteria.to_owned(),
        };
        tokio::time::timeout(
            timeout,
            self.submit_regular(cmd, super::dispatch::ThreadConsumer::default()),
        )
        .await
        .map_err(|_| Error::timeout_inflight())?
    }
}
