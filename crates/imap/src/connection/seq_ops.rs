#![allow(clippy::wildcard_imports)]
use super::*;

impl ImapConnection {
    // -----------------------------------------------------------------------
    // Message operations (sequence-number variants)
    // -----------------------------------------------------------------------

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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

    #[cfg(test)]
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
