//! IMAP implementation of the shared bifrost `Account` surface.
//!
//! Consumers construct an [`ImapAccountFactory`] from [`ImapAccountConfig`],
//! [`ImapConfig`], [`Credentials`], and [`AuthPolicy`], then register it as a
//! `bifrost_types::AccountFactory`. A consumer that tracks folders by UID
//! opens through [`ImapAccountFactory::open_with_poller`] instead and gets an
//! [`ImapPoller`] beside the account. The raw IMAP connection, parser, and
//! wire types are crate-internal implementation detail.

// The Account impl drives a subset of the IMAP4rev2 + extensions surface
// implemented under `connection/` and `types/`. The remaining commands,
// response shapes, and pipeline helpers stay built so the protocol layer
// is complete; a future trimming pass can decide what to remove vs wire
// up. The annotation is at the crate root because the dead surface is
// spread across most of the connection module tree.
#![allow(dead_code)]
// The crate-private `Error` now carries typed failure evidence
// (`LocalFailure`, `CapabilityFailure`) beside its message, so it is a large
// `Err` type. It is cold-path and translated to `AccountError` at the account
// boundary, so boxing it everywhere would only add an allocation per failure.
#![allow(clippy::result_large_err)]

// pub: consumers register the IMAP AccountFactory through this module.
pub mod account;

mod codec;
mod connection;
mod error;
mod sieve;
mod types;

pub use account::{
    FolderObservation, ImapAccountConfig, ImapAccountFactory, ImapMessageRef, ImapPoller,
    InvalidImapRef, ManageSieveConfig, RawMessage, SmtpSubmissionConfig, SubmissionCredentials,
    SubmissionTls, UidListing, UidPage, UidScan,
};
pub use connection::ImapConfig;
pub use types::{AuthPolicy, Credentials};

pub(crate) use connection::ImapConnection;
pub(crate) use error::Error;

/// Result type alias for IMAP operations.
pub(crate) type Result<T> = std::result::Result<T, Error>;
