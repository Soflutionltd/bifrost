//! The href-only filtered query lanes, shared by both protocol crates.
//!
//! A filtered listing lane (CalDAV `calendar-query`, CardDAV
//! `addressbook-query`) asks the server to decide membership and to answer with
//! `getetag` only. The account layer then sorts the candidate hrefs, slices the
//! page that follows the cursor watermark, and multigets ONLY that page - which
//! is what keeps a page bounded by the page size rather than by the size of the
//! collection.
//!
//! Everything here is protocol-neutral: the two crates differ only in the query
//! body and in which parser turns the 207 into hrefs.

use bifrost_types::AccountError;

/// What a filtered, href-only query answered.
pub enum FilteredHrefs {
    /// The server ran the filter and named these resources.
    Matched(HrefQuery),
    /// The server will not run this filter. Both specs leave the filter grammar
    /// a server must support largely open, and plenty support none of it, so a
    /// refusal is a capability answer rather than a failure: the caller degrades
    /// to listing the collection and matching locally over the page, which is
    /// what these lanes did unconditionally before the filter was pushed to the
    /// server.
    FilterUnsupported,
}

/// The candidate hrefs one filtered query produced, plus the per-resource and
/// whole-leg failures its 207 carried.
#[derive(Default)]
pub struct HrefQuery {
    pub hrefs: Vec<String>,
    /// Resources the server named but refused inside the 207. They feed
    /// `Page::failed_ids` even though nothing was hydrated for them.
    pub failed_hrefs: Vec<String>,
    /// The worst classified failure of a leg that did not answer, kept so a
    /// partially-refused walk reaches the consumer as a skipped scope rather
    /// than as a silently short result.
    pub degraded: Option<AccountError>,
}

impl HrefQuery {
    pub fn extend(
        &mut self,
        hrefs: impl IntoIterator<Item = String>,
        failed_hrefs: impl IntoIterator<Item = String>,
    ) {
        self.hrefs.extend(hrefs);
        self.failed_hrefs.extend(failed_hrefs);
    }

    /// Same rule as the multiget lanes: a query whose every leg failed and which
    /// observed nothing at all is still a failed call, while a leg that failed
    /// beside legs that answered keeps its classification in `degraded` and lets
    /// the page be served.
    ///
    /// # Errors
    ///
    /// The degraded classification, when nothing was observed anywhere.
    pub fn settle(self) -> Result<Self, AccountError> {
        let no_observations = self.hrefs.is_empty() && self.failed_hrefs.is_empty();
        match self.degraded {
            Some(error) if no_observations => Err(error),
            degraded => Ok(Self { degraded, ..self }),
        }
    }
}

/// Put candidate hrefs into the one order the page cursor is valid in.
///
/// The sort is the load-bearing half of the cursor contract: the cursor is
/// local and every continuation re-runs the remote request, while DAV
/// guarantees no ordering on a multistatus, so slicing raw response order lets
/// an unchanged result set come back permuted between pages - skipping the
/// members the permutation moved behind the cursor and serving twice the ones it
/// moved past. The dedup matters for the text lanes, whose per-property REPORTs
/// name the same resource once per property it matched.
#[must_use]
pub fn sorted_candidate_hrefs(mut hrefs: Vec<String>) -> Vec<String> {
    // Outlook users expect one row per resource, including when REPORT legs
    // disagree only on URL spelling. Keep a deterministic native spelling.
    hrefs.sort_unstable();
    hrefs.sort_by_cached_key(|href| crate::dav_href_key(href));
    hrefs.dedup_by(|left, right| crate::same_dav_url(left, right));
    hrefs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DavProtocol, local_error};
    use bifrost_types::AccountOperation;

    fn failure() -> AccountError {
        local_error(
            AccountOperation::EventSearch,
            "refused",
            DavProtocol::CalDav,
        )
    }

    #[test]
    fn equivalent_url_spellings_are_one_candidate_in_identity_order() {
        let hrefs = sorted_candidate_hrefs(vec![
            "https://dav.test/c/b".into(),
            "https://dav.test/c/a".into(),
            "https://DAV.test:443/c/%61".into(),
        ]);
        assert_eq!(hrefs.len(), 2);
        assert!(crate::same_dav_url(&hrefs[0], "https://dav.test/c/a"));
        assert!(crate::same_dav_url(&hrefs[1], "https://dav.test/c/b"));
        assert_eq!(hrefs, sorted_candidate_hrefs(hrefs.clone()));
    }

    #[test]
    fn candidates_are_sorted_and_deduped() {
        let hrefs = sorted_candidate_hrefs(vec![
            "b".to_string(),
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]);
        assert_eq!(hrefs, vec!["a", "b", "c"]);
    }

    /// A leg that failed beside legs that answered must not throw their answers
    /// away; a lane that observed nothing at all is still a failure.
    #[test]
    fn a_failed_leg_beside_an_answering_one_still_serves_the_page() {
        let mut query = HrefQuery {
            degraded: Some(failure()),
            ..HrefQuery::default()
        };
        query.extend(["a".to_string()], []);
        let settled = query.settle().expect("an observed href survives a bad leg");
        assert!(settled.degraded.is_some());

        let empty = HrefQuery {
            degraded: Some(failure()),
            ..HrefQuery::default()
        };
        assert!(empty.settle().is_err());

        // A leg that observed only a REFUSED resource has still answered for
        // it, so the caller is not sent back to re-walk it.
        let mut only_failed = HrefQuery {
            degraded: Some(failure()),
            ..HrefQuery::default()
        };
        only_failed.extend([], ["a".to_string()]);
        assert!(only_failed.settle().is_ok());
    }
}
