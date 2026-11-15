//! Bifrost SMTP is an email library that allows creating and sending messages. It provides:
//!
//! * An easy to use email builder
//! * Pluggable email transports
//! * Unicode support
//! * Secure defaults
//! * Async support
//!
//! Bifrost SMTP requires the workspace Rust version or newer.
//!
//! ## Features
//!
//! This section lists each crate feature and briefly explains it.
//! More info about each module can be found in the corresponding module page.
//!
//! ### SMTP transport
//!
//! _Send emails using [`SMTP`]_
//!
//! SMTP and LMTP transports use connection pooling by default.
//!
//! #### SMTP over TLS via the native-tls crate
//!
//! _Secure SMTP connections using TLS from the `native-tls` crate_
//!
//! Uses schannel on Windows, Security-Framework on macOS, and OpenSSL
//! on all other platforms.
//!
//! TLS support is always available for the synchronous API.
//! Enable **tokio** for TLS support in the async API.
//!
//! ##### Building Bifrost SMTP with OpenSSL
//!
//! When building Bifrost SMTP with native-tls on a system that makes
//! use of OpenSSL, the following packages will need to be installed
//! in order for the build and the compiled program to run properly.
//!
//! | Distro       | Build-time packages        | Runtime packages             |
//! | ------------ | -------------------------- | ---------------------------- |
//! | Debian       | `pkg-config`, `libssl-dev` | `libssl3`, `ca-certificates` |
//! | Alpine Linux | `pkgconf`, `openssl-dev`   | `libssl3`, `ca-certificates` |
//!
//! ### Async execution runtime
//!
//! _Use [tokio] as the async execution runtime for sending emails_
//!
//! * **tokio**: Allow asynchronously sending emails using [Tokio 1.x]
//!
//! ### Misc features
//!
//! _Additional features_
//!
//! * **serde**: Serialization/Deserialization of entities
//! * **tracing**: Logging using the `tracing` crate
//! * **dkim**: Add support for signing email with DKIM
//!
//! [`SMTP`]: crate::transport::smtp
//! [tokio]: https://docs.rs/tokio/1
//! [Tokio 1.x]: https://docs.rs/tokio/1
//! [DKIM]: https://datatracker.ietf.org/doc/html/rfc6376

#![doc(html_root_url = "https://docs.rs/bifrost-smtp/0.1.0")]
#![forbid(unsafe_code)]
#![deny(
    unreachable_pub,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unstable_features,
    unused_import_braces,
    rust_2018_idioms,
    clippy::string_add,
    clippy::string_add_assign,
    clippy::clone_on_ref_ptr,
    clippy::verbose_file_reads,
    clippy::unnecessary_self_imports,
    clippy::implicit_clone,
    clippy::mem_forget,
    clippy::cast_lossless,
    clippy::inefficient_to_string,
    clippy::inline_always,
    clippy::linkedlist,
    clippy::macro_use_imports,
    clippy::manual_assert,
    clippy::unnecessary_join,
    clippy::wildcard_imports,
    clippy::str_to_string,
    clippy::empty_structs_with_brackets,
    clippy::zero_sized_map_values,
    clippy::manual_let_else,
    clippy::semicolon_if_nothing_returned,
    clippy::unnecessary_wraps,
    clippy::doc_markdown,
    clippy::explicit_iter_loop,
    clippy::redundant_closure_for_method_calls,
    // Rust 1.86: clippy::unnecessary_semicolon,
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

// pub: crate users construct envelopes and inspect parsed mailbox addresses.
pub mod address;
mod base64;
// pub: message-builder errors are part of the public construction API.
pub mod error;
#[cfg(feature = "tokio")]
mod executor;
// pub: crate users build RFC 5322/MIME messages through this module.
pub mod message;
mod time;
// pub: crate users send messages through concrete and erased transports.
pub mod transport;

use std::error::Error as StdError;

#[cfg(feature = "tokio")]
// pub: async transport executors are chosen by users of the tokio API.
pub use self::executor::Executor;
#[cfg(feature = "tokio")]
// pub: default tokio executor for async SMTP and LMTP transports.
pub use self::executor::TokioExecutor;
#[cfg(feature = "tokio")]
#[doc(inline)]
// pub: users erase async transports behind a crate-provided adapter.
pub use self::transport::{AsyncTransport, BoxedAsyncTransport};
// pub: top-level convenience re-export for envelope address construction.
pub use crate::address::Address;
#[doc(inline)]
// pub: top-level convenience re-export for the message builder.
pub use crate::message::Message;
#[cfg(feature = "tokio")]
// pub: top-level convenience re-export for async SMTP and LMTP transports.
pub use crate::transport::smtp::{AsyncLmtpTransport, AsyncSmtpTransport};
// pub: top-level convenience re-export for sync SMTP and LMTP transports.
pub use crate::transport::smtp::{LmtpTransport, SmtpTransport};
#[doc(inline)]
// pub: top-level convenience re-export for transport traits and erasure.
pub use crate::transport::{BoxedTransport, Transport};
use crate::{address::Envelope, error::Error};

pub(crate) type BoxError = Box<dyn StdError + Send + Sync>;

#[cfg(test)]
mod test {
    use super::*;
    use crate::message::{Mailbox, Mailboxes, header, header::Headers};

    #[test]
    fn envelope_from_headers() {
        let from = Mailboxes::new().with("kayo@example.com".parse().unwrap());
        let to = Mailboxes::new().with("amousset@example.com".parse().unwrap());

        let mut headers = Headers::new();
        headers.set(header::From(from));
        headers.set(header::To(to));

        assert_eq!(
            Envelope::try_from(&headers).unwrap(),
            Envelope::new(
                Some(Address::new("kayo", "example.com").unwrap()),
                vec![Address::new("amousset", "example.com").unwrap()]
            )
            .unwrap()
        );
    }

    #[test]
    fn envelope_from_headers_sender() {
        let from = Mailboxes::new().with("kayo@example.com".parse().unwrap());
        let sender = Mailbox::new(None, "kayo2@example.com".parse().unwrap());
        let to = Mailboxes::new().with("amousset@example.com".parse().unwrap());

        let mut headers = Headers::new();
        headers.set(header::From::from(from));
        headers.set(header::Sender::from(sender));
        headers.set(header::To::from(to));

        assert_eq!(
            Envelope::try_from(&headers).unwrap(),
            Envelope::new(
                Some(Address::new("kayo", "example.com").unwrap()),
                vec![Address::new("amousset", "example.com").unwrap()]
            )
            .unwrap()
        );
    }

    #[test]
    fn envelope_from_headers_no_to() {
        let from = Mailboxes::new().with("kayo@example.com".parse().unwrap());
        let sender = Mailbox::new(None, "kayo2@example.com".parse().unwrap());

        let mut headers = Headers::new();
        headers.set(header::From::from(from));
        headers.set(header::Sender::from(sender));

        assert!(Envelope::try_from(&headers).is_err(),);
    }
}
