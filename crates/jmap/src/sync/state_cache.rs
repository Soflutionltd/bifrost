//! Per-`(accountId, ObjectType)` JMAP state cache.
//!
//! JMAP `Email/changes`, `Mailbox/changes`, and `Thread/changes` state
//! strings are per-`accountId` on the server (RFC 8620 §1.5.2 - state is
//! a property of an `{accountId, type}` pair). A single shared
//! `Option<String>` cannot hold the primary account's state and a
//! foreign (shared/delegate) account's state at once, so the cache is
//! stored in the account slot that owns it. Each handle travels with its own
//! cell; there is no separately supplied accountId lookup key.
//!
//! Unknown and unavailable observations supply no guard. StateCell owns every
//! transition; seeds are the only observations installed before concurrency.

use crate::core::id::ObjectState;
use crate::email::Email;
#[cfg(test)]
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

/// One slot's observations. The handle chooses the cell before a request;
/// no request helper can pair a handle with another account's string key.
pub(crate) struct StateCache<O = Email>(Arc<Mutex<StateCell<O>>>);

impl<O> Clone for StateCache<O> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<O> StateCache<O> {
    pub(crate) fn seeded(state: Option<ObjectState<O>>) -> Self {
        Self(Arc::new(Mutex::new(StateCell {
            state: state.filter(|state| !state.as_str().is_empty()),
        })))
    }

    pub(crate) async fn guard(&self) -> Option<ObjectState<O>> {
        self.0.lock().await.guard()
    }

    pub(crate) async fn observe_probe(
        &self,
        expected: Option<&ObjectState<O>>,
        probed: ObjectState<O>,
    ) -> ObjectState<O> {
        self.0.lock().await.observe_probe(expected, probed)
    }

    pub(crate) async fn observe_changes(&self, since: &ObjectState<O>, state: ObjectState<O>) {
        self.0.lock().await.observe_changes(since, state);
    }

    pub(crate) async fn observe_set(&self, transition: Option<(ObjectState<O>, ObjectState<O>)>) {
        self.0.lock().await.observe_set(transition);
    }
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

// Test fixtures may prepare several accounts at once. Selecting a cell happens
// when a fixture constructs a slot; production has no account-keyed state map.
#[cfg(test)]
pub(crate) type StateMap<O = Email> = Arc<std::sync::Mutex<HashMap<String, StateCache<O>>>>;

#[cfg(test)]
pub(crate) fn seeded<O>(states: HashMap<String, Option<ObjectState<O>>>) -> StateMap<O> {
    Arc::new(std::sync::Mutex::new(
        states
            .into_iter()
            .map(|(id, state)| (id, StateCache::seeded(state)))
            .collect(),
    ))
}

#[cfg(test)]
pub(crate) fn cell<O>(states: &StateMap<O>, id: &str) -> StateCache<O> {
    states
        .lock()
        .expect("test states")
        .entry(id.to_owned())
        .or_insert_with(|| StateCache::seeded(None))
        .clone()
}

#[cfg(test)]
pub(crate) async fn guard<O>(states: &StateMap<O>, id: &str) -> Option<ObjectState<O>> {
    cell(states, id).guard().await
}
#[cfg(test)]
pub(crate) async fn set<O>(states: &StateMap<O>, id: &str, state: ObjectState<O>) {
    cell(states, id).0.lock().await.state = Some(state);
}
#[cfg(test)]
pub(crate) async fn observe_probe<O>(
    states: &StateMap<O>,
    id: &str,
    expected: Option<&ObjectState<O>>,
    probed: ObjectState<O>,
) -> ObjectState<O> {
    cell(states, id).observe_probe(expected, probed).await
}
#[cfg(test)]
pub(crate) async fn observe_changes<O>(
    states: &StateMap<O>,
    id: &str,
    since: &ObjectState<O>,
    state: ObjectState<O>,
) {
    cell(states, id).observe_changes(since, state).await;
}
#[cfg(test)]
pub(crate) async fn observe_set<O>(
    states: &StateMap<O>,
    id: &str,
    transition: Option<(ObjectState<O>, ObjectState<O>)>,
) {
    cell(states, id).observe_set(transition).await;
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
        seeded(HashMap::new())
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
