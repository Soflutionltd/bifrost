//! Messages an inventory page already carried in full.
//!
//! A message delta page returns every field hydration asks for, so a
//! first sync that then re-reads each message one request at a time pays
//! for the same bytes twice. The inventory parks each page's values here and
//! `message_hydrate` answers from them once, which turns a first sync from
//! one request per message into one per page.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde_json::Value;

/// Memory the parked values may hold. The walk runs ahead of the consumer,
/// so past this the oldest values are dropped and those messages are read
/// over the network as before.
const BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Fixed allowance for a value's fields besides its bodies.
const FIELDS_ALLOWANCE: usize = 4 * 1024;

#[derive(Debug, Default)]
pub(crate) struct PrefetchedMessages {
    inner: Mutex<Parked>,
}

#[derive(Debug, Default)]
struct Parked {
    values: HashMap<String, (Value, usize)>,
    order: VecDeque<String>,
    bytes: usize,
}

impl PrefetchedMessages {
    /// Park `value` under the message id `id` when hydration can be answered
    /// from it alone. A message with attachments is not parked: delta cannot
    /// expand them. Neither is one whose body references an inline image,
    /// because Graph leaves inline parts out of `hasAttachments`.
    pub(crate) fn park(&self, id: String, value: Value) {
        if !self_contained(&value) {
            return;
        }
        let size = body_len(&value, "body") + body_len(&value, "uniqueBody") + FIELDS_ALLOWANCE;
        if size > BUDGET_BYTES {
            return;
        }
        let mut parked = self.lock();
        if let Some((_, previous)) = parked.values.remove(&id) {
            parked.bytes -= previous;
            parked.order.retain(|queued| queued != &id);
        }
        while parked.bytes + size > BUDGET_BYTES {
            let Some(oldest) = parked.order.pop_front() else {
                break;
            };
            if let Some((_, dropped)) = parked.values.remove(&oldest) {
                parked.bytes -= dropped;
            }
        }
        parked.bytes += size;
        parked.order.push_back(id.clone());
        parked.values.insert(id, (value, size));
    }

    /// The parked value for `id`, removed so it answers one read only: a
    /// later change to the message is read from the server.
    pub(crate) fn take(&self, id: &str) -> Option<Value> {
        let mut parked = self.lock();
        let (value, size) = parked.values.remove(id)?;
        parked.bytes -= size;
        parked.order.retain(|queued| queued != id);
        Some(value)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Parked> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn self_contained(value: &Value) -> bool {
    value.get("hasAttachments").and_then(Value::as_bool) == Some(false)
        && !["body", "uniqueBody"].iter().any(|field| {
            value
                .get(field)
                .and_then(|body| body.get("content"))
                .and_then(Value::as_str)
                .is_some_and(|content| content.contains("cid:"))
        })
}

fn body_len(value: &Value, field: &str) -> usize {
    value
        .get(field)
        .and_then(|body| body.get("content"))
        .and_then(Value::as_str)
        .map_or(0, str::len)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn message(body: &str) -> Value {
        json!({ "hasAttachments": false, "body": { "contentType": "html", "content": body } })
    }

    #[test]
    fn a_parked_message_answers_one_read() {
        let prefetched = PrefetchedMessages::default();
        prefetched.park("m1".into(), message("<p>hello</p>"));
        assert_eq!(prefetched.take("m1"), Some(message("<p>hello</p>")));
        assert_eq!(prefetched.take("m1"), None);
    }

    #[test]
    fn a_message_needing_its_attachments_is_not_parked() {
        let prefetched = PrefetchedMessages::default();
        prefetched.park(
            "files".into(),
            json!({ "hasAttachments": true, "body": { "content": "x" } }),
        );
        prefetched.park("inline".into(), message("<img src=\"cid:logo\">"));
        prefetched.park("unknown".into(), json!({ "body": { "content": "x" } }));
        assert_eq!(prefetched.take("files"), None);
        assert_eq!(prefetched.take("inline"), None);
        assert_eq!(prefetched.take("unknown"), None);
    }

    #[test]
    fn the_oldest_value_goes_first_past_the_budget() {
        let prefetched = PrefetchedMessages::default();
        let half = "x".repeat(BUDGET_BYTES / 2);
        prefetched.park("first".into(), message(&half));
        prefetched.park("second".into(), message(&half));
        assert_eq!(prefetched.take("first"), None);
        assert!(prefetched.take("second").is_some());
        assert_eq!(prefetched.lock().bytes, 0);
    }
}
