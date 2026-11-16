#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // SORT (RFC 5256 Section 2)
    // -----------------------------------------------------------------------

    #[cfg(test)]
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
        criteria: impl Into<crate::types::SearchCriteria>,
        timeout: Duration,
    ) -> Result<SearchResult, Error> {
        let criteria = criteria.into();

        let cmd = super::request::UidSort {
            sort_criteria: sort_criteria.to_owned(),
            charset: charset.to_owned(),
            criteria,
        };
        self.execute_request(cmd, timeout).await
    }

    // -----------------------------------------------------------------------
    // THREAD (RFC 5256 Section 3)
    // -----------------------------------------------------------------------

    #[cfg(test)]
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
        criteria: impl Into<crate::types::SearchCriteria>,
        timeout: Duration,
    ) -> Result<Vec<ThreadNode<crate::types::Seq>>, Error> {
        let criteria = criteria.into();

        let cmd = super::request::Thread {
            algorithm: algorithm.to_owned(),
            charset: charset.to_owned(),
            criteria,
        };
        self.execute_request(cmd, timeout).await
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
        criteria: impl Into<crate::types::SearchCriteria>,
        timeout: Duration,
    ) -> Result<Vec<ThreadNode>, Error> {
        let criteria = criteria.into();

        let cmd = super::request::UidThread {
            algorithm: algorithm.to_owned(),
            charset: charset.to_owned(),
            criteria,
        };
        self.execute_request(cmd, timeout).await
    }
}
