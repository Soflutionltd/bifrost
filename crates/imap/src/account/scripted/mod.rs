//! Account transcripts grouped by operation.

mod test_support;

// Also used by the push owner tests.
pub(super) use test_support::session_account;

mod blob;

mod changes;

mod containers;

mod draft_send;

mod hydration;

mod lifecycle;

mod mutations;

mod poll;

mod push;

mod search;

mod selection;

mod send;
