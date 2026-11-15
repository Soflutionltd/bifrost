//! Per-`(accountId, ObjectType)` JMAP state cache.
//!
//! JMAP `Email/changes`, `Mailbox/changes`, and `Thread/changes` state
//! strings are per-`accountId` on the server (RFC 8620 §1.5.2 - state is
//! a property of an `{accountId, type}` pair). A single shared
//! `Option<String>` cannot hold the primary account's state and a
//! foreign (shared/delegate) account's state at once, so the cache is
//! keyed by JMAP `accountId`. The primary account is one ordinary key
//! (`self.mail.id_str()`); there is no primary-vs-foreign branch in the
//! cache itself, only in *which* key a call site looks up.
//!
//! The inner `Option<String>` preserves the tri-state the scalar form
//! had: an absent key means "never probed"; `Some(None)` means "probed,
//! empty"; `Some(Some(s))` means "known state".

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

/// Per-`accountId` state cache for one JMAP object type.
pub(crate) type StateMap = Arc<Mutex<HashMap<String, Option<String>>>>;

/// The current cached state for an account, or `None` when the account
/// has never been probed (absent key) or was probed empty (`Some(None)`).
/// Both cases intentionally supply no mutation guard; provenance is not a
/// usable server state. Set observations never initialize either case.
pub(crate) async fn get(map: &StateMap, account_id: &str) -> Option<String> {
    let guard = map.lock().await;
    guard.get(account_id).cloned().flatten()
}

/// Unconditionally set the cached state for an account.
pub(crate) async fn set(map: &StateMap, account_id: &str, state: String) {
    let mut guard = map.lock().await;
    guard.insert(account_id.to_string(), Some(state));
}

/// Advance the cached state for an account to `state`, but only when the
/// current value exactly matches `expected` (last-writer-wins guard against
/// a concurrent advance racing ahead). `None` matches only an absent or
/// explicitly empty entry.
pub(crate) async fn advance(
    map: &StateMap,
    account_id: &str,
    expected: Option<&str>,
    state: String,
) {
    let mut guard = map.lock().await;
    match guard.get(account_id).and_then(|entry| entry.as_deref()) {
        current if current == expected => {
            guard.insert(account_id.to_string(), Some(state));
        }
        _ => {}
    }
}

/// A set response proves only its own oldState -> newState transition.
/// Missing/empty endpoints prove no continuity with our observed changes.
pub(crate) fn set_transition(old: Option<&str>, new: Option<&str>) -> Option<(String, String)> {
    Some((
        old.filter(|s| !s.is_empty())?.to_string(),
        new.filter(|s| !s.is_empty())?.to_string(),
    ))
}

/// Preserve unobserved concurrent changes for the next whole-property guard.
/// An Outlook user expects a concurrent edit to conflict rather than have a
/// later unrelated write silently bless it as already observed.
pub(crate) async fn observe_set(
    map: &StateMap,
    account_id: &str,
    transition: Option<(String, String)>,
) {
    if let Some((old, new)) = transition {
        advance(map, account_id, Some(&old), new).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map() -> StateMap {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[tokio::test]
    async fn set_observation_requires_continuous_server_states() {
        let m = map();
        set(&m, "acct-a", "s1".into()).await;
        observe_set(&m, "acct-a", set_transition(Some("unobserved"), Some("s3"))).await;
        assert_eq!(get(&m, "acct-a").await.as_deref(), Some("s1"));
        observe_set(&m, "acct-a", set_transition(Some("s1"), Some("s2"))).await;
        observe_set(&m, "acct-a", set_transition(Some("s2"), Some("s3"))).await;
        assert_eq!(get(&m, "acct-a").await.as_deref(), Some("s3"));
        for (old, new) in [
            (None, Some("s4")),
            (Some(""), Some("s4")),
            (Some("s3"), None),
            (Some("s3"), Some("")),
        ] {
            observe_set(&m, "acct-a", set_transition(old, new)).await;
            observe_set(&m, "absent", set_transition(old, new)).await;
        }
        observe_set(&m, "absent", set_transition(Some("s3"), Some("s4"))).await;
        assert_eq!(get(&m, "acct-a").await.as_deref(), Some("s3"));
        assert_eq!(get(&m, "absent").await, None);
    }

    #[tokio::test]
    async fn absent_account_reads_none() {
        let m = map();
        assert_eq!(get(&m, "acct-a").await, None);
    }

    #[tokio::test]
    async fn set_and_get_round_trip_per_account() {
        let m = map();
        set(&m, "acct-a", "s-a".to_string()).await;
        set(&m, "acct-b", "s-b".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, Some("s-a".to_string()));
        assert_eq!(get(&m, "acct-b").await, Some("s-b".to_string()));
    }

    #[tokio::test]
    async fn advance_respects_expected_guard() {
        let m = map();
        set(&m, "acct-a", "s1".to_string()).await;
        // Expected mismatch: do not advance.
        advance(&m, "acct-a", Some("other"), "s2".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, Some("s1".to_string()));
        // Expected match: advance.
        advance(&m, "acct-a", Some("s1"), "s2".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, Some("s2".to_string()));
    }

    #[tokio::test]
    async fn advance_with_expected_state_does_not_initialize_absent_entry() {
        let m = map();
        advance(&m, "acct-a", Some("ignored"), "s1".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, None);
    }

    #[tokio::test]
    async fn advance_without_expected_state_initializes_absent_entry() {
        let m = map();
        advance(&m, "acct-a", None, "s1".to_string()).await;
        assert_eq!(get(&m, "acct-a").await, Some("s1".to_string()));
    }
}
