//! IMAP wire codec: parsing server responses and encoding client commands.
//!
//! # References
//! - RFC 3501 (`IMAP4rev1`)
//! - RFC 9051 (`IMAP4rev2`)
//! - RFC 2047 (MIME encoded words in ENVELOPE fields)
//! - RFC 2231 (MIME parameter encoding in BODYSTRUCTURE)
//! - RFC 6855 (UTF8=ACCEPT  -  raw UTF-8 in quoted strings)
//!
//! Audit boundary: the 2026-09-04 bug hunt read the strict-versus-tolerant
//! parse gate (which failures are connection-fatal) but did NOT line-audit
//! the decoder and encoder internals under `decode/` and `encode/`. An
//! absence of findings there is an absence of reading. Noted so a later
//! auditor knows where coverage stops.

pub(crate) mod classification;
pub(crate) mod decode;
pub(crate) mod encode;
pub(crate) mod literals;

pub(crate) mod search;
