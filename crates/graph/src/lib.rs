#![forbid(unsafe_code)]
#![doc = "Microsoft Graph Account implementation for bifrost."]
// The crate-internal `Error` carries Graph response bodies, error envelopes,
// EWS SOAP faults, and webhook validation failures. It is translated to the
// opaque `AccountError` (8 bytes, `Arc<Inner>`-backed) at the protocol
// boundary, so the `result_large_err` concern only applies briefly inside
// this crate.
#![allow(clippy::result_large_err)]
// Boxed Send futures over deep reqwest/hyper type stacks exceed rustc's
// default auto-trait recursion depth (rust-lang/rust#159228, a
// future-incompat hard error). Raising the limit is the sanctioned fix.
#![recursion_limit = "256"]

// pub: sync-engine conformance and consumers register Graph accounts through this module.
pub mod account;
mod api;
mod client;
mod error;
mod etag;
mod ews;
mod origin;
mod paging;
mod types;
mod webhooks;
