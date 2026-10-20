//! Private shared SASL/SCRAM computation layer for the bifrost protocol crates.
//!
//! This crate is private to bifrost. It is computation-only: no I/O, no async,
//! no protocol command flow. The protocol crates (`bifrost-imap` and
//! `bifrost-smtp`) own the wire sequencing - driving `+` continuations, framing
//! commands - and call into the pure transition functions here.
//!
//! The surface is deliberately small: a zeroizing [`Secret`] wrapper, a minimal
//! [`SaslError`], the [`ScramHash`] selector, the SCRAM transition functions,
//! and the CRAM-MD5 response builder. Each protocol crate maps [`SaslError`]
//! back into its own error enum at the call boundary.

#![forbid(unsafe_code)]

mod channel_binding;
mod cram;
mod error;
mod oauth;
mod scram;
mod secret;

pub use channel_binding::tls_server_end_point;
pub use cram::cram_md5_response;
pub use error::SaslError;
pub use oauth::{oauthbearer_payload, xoauth2_payload};
pub use scram::{
    ChannelBinding, ScramChannelBinding, ScramHash, ScramPassword, decode_continuation,
    prepare_scram_password, prepare_scram_username, scram_client_final, select_scram_rung,
    verify_server_final,
};
pub use secret::Secret;
