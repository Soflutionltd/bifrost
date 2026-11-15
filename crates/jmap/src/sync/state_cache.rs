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
//! Unknown and unavailable observations supply no guard. StateCell owns every
//! transition; seeds are the only observations installed before concurrency.

use crate::core::id::ObjectState;
use crate::email::Email;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

/// One account's observed object state. Unknown and unavailable observations both supply
/// no guard, so there is no distinct empty provenance to maintain.
pub(crate) struct StateCell<O> {
    state: Option<ObjectState<O>>,
}

impl<O> Default for StateCell<O> {
    fn default() -> Self {
        Self { state: None }
    }
}

impl<O> StateCell<O> {
    fn guard(&self) -> Option<ObjectState<O>> {
        self.state
            .clone()
            .filter(|state| !state.as_str().is_empty())
    }

    fn replace_if(&mut self, expected: Option<&ObjectState<O>>, new: ObjectState<O>) {
        if self.state.as_ref() == expected && !new.as_str().is_empty() {
            self.state = Some(new);
        }
    }

    fn observe_changes(&mut self, since: &ObjectState<O>, new: ObjectState<O>) {
        self.replace_if(Some(since), new);
    }

    fn observe_probe(
        &mut self,
        expected: Option<&ObjectState<O>>,
        probed: ObjectState<O>,
    ) -> ObjectState<O> {
        self.replace_if(expected, probed.clone());
        self.guard().unwrap_or(probed)
    }

    fn observe_set(&mut self, transition: Option<(ObjectState<O>, ObjectState<O>)>) {
        if let Some((old, new)) = transition {
            self.observe_changes(&old, new);
        }
    }
}

/// Per-account cells for Email. Mailbox lifecycle positions have a separate
/// owner because only delivered lifecycle pages may publish sidebar progress.
pub(crate) type StateMap<O = Email> = Arc<Mutex<HashMap<String, StateCell<O>>>>;

pub(crate) fn seeded<O>(states: HashMap<String, Option<ObjectState<O>>>) -> StateMap<O> {
    Arc::new(Mutex::new(
        states
            .into_iter()
            .map(|(account, state)| {
                let state = state.filter(|state| !state.as_str().is_empty());
                (account, StateCell { state })
            })
            .collect(),
    ))
}

pub(crate) async fn guard<O>(map: &StateMap<O>, account_id: &str) -> Option<ObjectState<O>> {
    map.lock().await.get(account_id).and_then(StateCell::guard)
}

#[cfg(test)]
pub(crate) async fn set<O>(map: &StateMap<O>, account_id: &str, state: ObjectState<O>) {
    map.lock()
        .await
        .insert(account_id.to_string(), StateCell { state: Some(state) });
}

/// Return the winning observation under the publication lock. Network probes
/// hold no lock and cannot replace a concurrent advance.
pub(crate) async fn observe_probe<O>(
    map: &StateMap<O>,
    account_id: &str,
    expected: Option<&ObjectState<O>>,
    probed: ObjectState<O>,
) -> ObjectState<O> {
    map.lock()
        .await
        .entry(account_id.to_string())
        .or_default()
        .observe_probe(expected, probed)
}

pub(crate) async fn observe_changes<O>(
    map: &StateMap<O>,
    account_id: &str,
    since: &ObjectState<O>,
    state: ObjectState<O>,
) {
    map.lock()
        .await
        .entry(account_id.to_string())
        .or_default()
        .observe_changes(since, state);
}

/// A set response proves only its own oldState -> newState transition.
/// Missing/empty endpoints prove no continuity with our observed changes.
pub(crate) fn set_transition<O>(
    old: Option<&ObjectState<O>>,
    new: Option<&ObjectState<O>>,
) -> Option<(ObjectState<O>, ObjectState<O>)> {
    Some((
        old.map(ObjectState::as_str)
            .filter(|s| !s.is_empty())?
            .into(),
        new.map(ObjectState::as_str)
            .filter(|s| !s.is_empty())?
            .into(),
    ))
}

/// Preserve unobserved concurrent changes for the next whole-property guard.
/// An Outlook user expects a concurrent edit to conflict rather than have a
/// later unrelated write silently bless it as already observed.
pub(crate) async fn observe_set<O>(
    map: &StateMap<O>,
    account_id: &str,
    transition: Option<(ObjectState<O>, ObjectState<O>)>,
) {
    if let Some((old, new)) = transition {
        map.lock()
            .await
            .entry(account_id.to_string())
            .or_default()
            .observe_set(Some((old, new)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transition(
        old: Option<&str>,
        new: Option<&str>,
    ) -> Option<(ObjectState<Email>, ObjectState<Email>)> {
        let old = old.map(ObjectState::new);
        let new = new.map(ObjectState::new);
        set_transition(old.as_ref(), new.as_ref())
    }

    fn map() -> StateMap {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[tokio::test]
    async fn probe_publication_preserves_a_concurrent_winner() {
        let m = map();
        // An initial probe began with no state; changes published before it ended.
        set(&m, "acct-a", "newer".into()).await;
        assert_eq!(
            observe_probe(&m, "acct-a", None, "older".into()).await,
            "newer"
        );
        // A refusal refresh also must not overwrite a concurrent cache advance.
        assert_eq!(
            observe_probe(&m, "acct-a", Some(&"before".into()), "older".into()).await,
            "newer"
        );
        assert_eq!(guard(&m, "acct-a").await.as_deref(), Some("newer"));
        assert_eq!(
            observe_probe(&m, "acct-a", Some(&"newer".into()), "fresh".into()).await,
            "fresh"
        );
        assert_eq!(
            observe_probe(&m, "acct-b", None, "initial".into()).await,
            "initial"
        );
    }

    #[tokio::test]
    async fn set_observation_requires_continuous_server_states() {
        let m = map();
        set(&m, "acct-a", "s1".into()).await;
        observe_set(&m, "acct-a", transition(Some("unobserved"), Some("s3"))).await;
        assert_eq!(guard(&m, "acct-a").await.as_deref(), Some("s1"));
        observe_set(&m, "acct-a", transition(Some("s1"), Some("s2"))).await;
        observe_set(&m, "acct-a", transition(Some("s2"), Some("s3"))).await;
        assert_eq!(guard(&m, "acct-a").await.as_deref(), Some("s3"));
        for (old, new) in [
            (None, Some("s4")),
            (Some(""), Some("s4")),
            (Some("s3"), None),
            (Some("s3"), Some("")),
        ] {
            observe_set(&m, "acct-a", transition(old, new)).await;
            observe_set(&m, "absent", transition(old, new)).await;
        }
        observe_set(&m, "absent", transition(Some("s3"), Some("s4"))).await;
        assert_eq!(guard(&m, "acct-a").await.as_deref(), Some("s3"));
        assert_eq!(guard(&m, "absent").await, None);
    }

    #[tokio::test]
    async fn absent_account_reads_none() {
        let m = map();
        assert_eq!(guard(&m, "acct-a").await, None);
    }

    #[tokio::test]
    async fn set_and_get_round_trip_per_account() {
        let m = map();
        set(&m, "acct-a", "s-a".into()).await;
        set(&m, "acct-b", "s-b".into()).await;
        assert_eq!(guard(&m, "acct-a").await, Some("s-a".into()));
        assert_eq!(guard(&m, "acct-b").await, Some("s-b".into()));
    }

    #[tokio::test]
    async fn advance_respects_expected_guard() {
        let m = map();
        set(&m, "acct-a", "s1".into()).await;
        // Expected mismatch: do not advance.
        observe_changes(&m, "acct-a", &"other".into(), "s2".into()).await;
        assert_eq!(guard(&m, "acct-a").await, Some("s1".into()));
        // Expected match: advance.
        observe_changes(&m, "acct-a", &"s1".into(), "s2".into()).await;
        assert_eq!(guard(&m, "acct-a").await, Some("s2".into()));
    }

    #[tokio::test]
    async fn advance_with_expected_state_does_not_initialize_absent_entry() {
        let m = map();
        observe_changes(&m, "acct-a", &"ignored".into(), "s1".into()).await;
        assert_eq!(guard(&m, "acct-a").await, None);
    }

    #[tokio::test]
    async fn probe_without_expected_state_initializes_absent_entry() {
        let m = map();
        observe_probe(&m, "acct-a", None, "s1".into()).await;
        assert_eq!(guard(&m, "acct-a").await, Some("s1".into()));
    }
}
