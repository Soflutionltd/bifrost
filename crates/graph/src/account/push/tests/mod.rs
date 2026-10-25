//! The push suite, split along the same seams as the code it covers.
//!
//! `fixtures` holds the builders more than one arm needs (subscription rows,
//! scope and translation inputs, a per-request ledger); the other modules
//! mirror `push/`'s own files, so a test sits beside the module it pins.
//! `dispatch` keeps the mode-agnostic contract - the per-scope lane rules and
//! the whole-request refusals - which is exercised through `push_subscribe` in
//! both push modes and belongs to neither arm.

mod dispatch;
mod ews;
mod fixtures;
mod renewal;
mod webhook;

mod renewal_sessions;
