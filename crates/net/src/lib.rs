#![forbid(unsafe_code)]
// The transport `Error` preserves response evidence (HeaderMap + capped
// body, inline and via FinalResponse) so the four protocol crates can
// pattern-match status, headers, and body without a Box deref. The enum
// is uniformly large rather than lopsided, so `large_enum_variant` (deny)
// already passes and there is no cheap single-variant boxing win; boxing
// to satisfy `result_large_err` would only add an allocation on the cold
// error path. The error is intentionally large by design.
#![allow(clippy::result_large_err)]
//! Shared HTTP transport for the bifrost JMAP, Gmail, and Graph
//! clients.
//!
//! `bifrost-net` owns the HTTP/2 connection pool, OAuth bearer-token
//! refresh, retry budget with `Retry-After` honor, per-host token
//! bucket rate limiter, per-account bandwidth metering, native-tls
//! configuration, and W3C `traceparent` injection. It does not own
//! protocol-level JSON shapes, IMAP, or SMTP transport.
//!
//! The Phase 2 implementation wires the retry loop, single-flight
//! refresher, per-host token bucket, sliding-window bandwidth meter,
//! and traceparent header. The public surface (`Net`, `AccountNet`,
//! `RequestBuilder`, `Response`, `StreamingResponse`, `Error`) is
//! frozen for Phase 2 consumers and matches the Phase 1 skeleton.

pub mod account_error;
pub mod auth;
pub mod bandwidth;
pub mod budget;
pub mod concurrency;
pub mod config;
pub mod error;
pub mod hold;
mod metered_stream;
pub mod net;
pub mod proxy;
pub mod rate;
pub mod redirect;
pub mod request;
pub mod retry;
pub mod status_line;
// Compiled for this crate's own unit tests as well, so the scripted
// double has exactly one definition rather than a private copy here
// and a published copy for downstream.
#[cfg(any(feature = "test-support", test))]
pub mod test_support;
pub mod trace;
mod upload;
pub mod url;

pub use account_error::{NetErrorContext, into_account_error_with_throttle_scope};
pub use auth::{AccessToken, OAuthRefresher, RefreshState, StaticTokenSource, TokenSource};
pub use bandwidth::{AccountMeter, BandwidthMeter, MeterSink, MeterSinkHandle};
pub use budget::{BandwidthBudget, BandwidthDirection, BandwidthRegistry, Reservation};
// Shared identity / priority / byte-range / future-alias types live
// in bifrost-types so the engine, the protocol crates, and this
// crate all speak the same language. Re-exported here for ergonomic
// imports from downstream code.
//
// `TransmissionState` is here for a sharper reason than ergonomics: it is
// part of THIS crate's published error surface - `Error::AuthLost` carries
// one - so a consumer that can name the error must be able to name its field
// without taking a bifrost-types dependency of its own. A crate for which
// bifrost-types is optional could otherwise construct the error only in the
// configurations where some other feature happened to pull the spelling in,
// which is a feature-graph accident rather than a contract.
pub use bifrost_types::{AccountFuture, AccountId, ByteRange, Priority, TransmissionState};
pub use concurrency::ConcurrencyLimit;
pub use config::{DEFAULT_MAX_BUFFERED_RESPONSE, NetConfig};
pub use error::{Error, FinalResponse, MalformedRedirectKind, RangeFailureKind, STATUS_BODY_CAP};
pub use hold::{Held, HoldDeadline, HoldKind, SharedHold};
pub use http::Method;
pub use metered_stream::MeteredStream;
pub use net::{AccountNet, AccountSpec, Net};
pub use rate::{RateGeneration, RateLimit, RateLimitGovernor, RequestCost};
pub use redirect::{
    DEFAULT_MAX_HOPS, FollowRedirects, RedirectAction, RedirectPolicy, RedirectStep,
};
pub use request::{
    ByteStream, ReplayPolicy, RequestBuilder, RequestByteCounter, Response, StreamingResponse,
    request_is_replayable,
};
pub use retry::RetryPolicy;
pub use status_line::{status_line_code, status_line_is_success};
