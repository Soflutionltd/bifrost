//! Shared SASL clients and authentication policy for the bifrost protocol crates.
//!
//! Published as a support crate; protocol crates adapt its mechanisms and errors
//! to their own API. Exchanges are computation-only, with no I/O or async work.
//! [`SaslClient`] owns mechanism state, while adapters own wire commands,
//! base64 framing, deadlines and token-source resolution.

#![forbid(unsafe_code)]

mod channel_binding;
mod client;
mod cram;
mod error;
mod negotiation;
mod oauth;
mod plain;
mod scram;
mod secret;

pub use channel_binding::tls_server_end_point;
pub use client::{SaslClient, Step};
pub use cram::cram_md5_response;
pub use error::{RefusalClass, SaslError, ScramServerError};
pub use negotiation::{
    ChannelBindingData, Mechanism, MechanismName, NegotiatedMechanism, NegotiationPolicy,
    NegotiationSkip, OAUTH_PREFERENCE, PASSWORD_PREFERENCE, ScramRung, ScramSkip,
    authentication_allowed, binding_from_peer_certificate, negotiate, scram_rung,
};
pub use oauth::{OAuthMechanism, oauthbearer_payload, xoauth2_payload};
pub use plain::plain_payload;
pub use scram::{
    ChannelBinding, ScramChannelBinding, ScramHash, ScramPassword, decode_continuation,
    prepare_scram_password, prepare_scram_username, scram_client_final, verify_server_final,
};
pub use secret::Secret;
