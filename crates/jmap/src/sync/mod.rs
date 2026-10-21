//! `bifrost-types::Account` implementation for `bifrost-jmap`.
//!
//! The implementation is intentionally engine-facing only: this crate
//! depends on `bifrost-types`, not on `bifrost-sync`. JMAP change
//! cursors are encoded as protocol-tagged opaque bytes and every stream
//! emits checkpoints at server state boundaries.

mod account;
mod blob;
mod calendar_ops;
mod capabilities;
#[cfg(test)]
mod capability_contract_tests;
mod changes;
mod contacts;
mod discover;
mod error;
mod factory;
mod filters;
mod foreign;
mod hydrate;
mod inventory;
mod mutation;
mod page_cursor;
mod pim;
mod push;
mod state;
mod state_cache;
mod state_walk;

// pub: engine registration surface consumed by bifrost-sync users.
pub use factory::{JmapAccountFactory, JmapAccountFactoryBuilder, JmapCredentials};
// pub: account-open configuration for JMAP WebSocket reconnect behavior.
pub use push::ReconnectPolicy;
