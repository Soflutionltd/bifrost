//! Shared pagination admission policy.

use std::collections::HashSet;
use std::hash::Hash;

/// Why a walk must stop before following another page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PageRefusal {
    Repeated,
    Budget,
}

/// Check a continuation after `pages` completed pages, before fetching another.
///
/// Repeat wins over budget: a known cycle proves a provider contract breach,
/// whereas a large collection only proves that the client reached its limit.
/// Enterprise users need the actionable provider diagnosis when both apply.
/// Callers retain their existing seen set and counter across stream suspension.
/// No continuation means normal completion, even on the budget boundary.
pub fn check_next<K: Eq + Hash>(
    seen: &mut HashSet<K>,
    pages: usize,
    budget: usize,
    next: Option<K>,
) -> Result<(), PageRefusal> {
    let Some(next) = next else {
        return Ok(());
    };
    if seen.contains(&next) {
        return Err(PageRefusal::Repeated);
    }
    if pages >= budget {
        return Err(PageRefusal::Budget);
    }
    seen.insert(next);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PageRefusal, check_next};
    use std::collections::HashSet;

    #[test]
    fn completion_repeat_and_fresh_budget_boundary() {
        let mut seen = HashSet::new();
        assert_eq!(check_next(&mut seen, 1, 2, Some("cycle")), Ok(()));
        assert_eq!(check_next(&mut seen, 2, 2, None), Ok(()));
        assert_eq!(
            check_next(&mut seen, 2, 2, Some("cycle")),
            Err(PageRefusal::Repeated),
        );
        assert_eq!(
            check_next(&mut seen, 2, 2, Some("fresh")),
            Err(PageRefusal::Budget),
        );
        assert!(!seen.contains("fresh"));
    }
}
