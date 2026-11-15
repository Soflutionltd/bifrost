//! Error type for email messages

use std::{
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

/// Error type for email content
#[derive(Debug)]
#[non_exhaustive]
// pub: message construction errors are local content failures, not Account recovery failures.
pub enum Error {
    /// Missing from in envelope
    MissingFrom,
    /// Missing to in envelope
    MissingTo,
    /// Multiple From authors without a single Sender.
    /// Retained publicly and still produced by the message builder for that
    /// invalid header combination. Multiple authors with one Sender are valid;
    /// checked header-derived envelope conversion reports its own `InvalidInput`.
    TooManyFrom,
    /// Invalid email: missing at
    EmailMissingAt,
    /// Invalid email: missing local part
    EmailMissingLocalPart,
    /// Invalid email: missing domain
    EmailMissingDomain,
    /// Cannot parse filename for attachment
    CannotParseFilename,
    /// IO error
    Io(std::io::Error),
    /// Non-ASCII chars
    NonAsciiChars,
    /// Invalid user-supplied message input
    InvalidInput(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Error::MissingFrom => f.write_str("missing source address, invalid envelope"),
            Error::MissingTo => f.write_str("missing destination address, invalid envelope"),
            Error::TooManyFrom => f.write_str("there can only be one source address"),
            Error::EmailMissingAt => f.write_str("missing @ in email address"),
            Error::EmailMissingLocalPart => f.write_str("missing local part in email address"),
            Error::EmailMissingDomain => f.write_str("missing domain in email address"),
            Error::CannotParseFilename => f.write_str("could not parse attachment filename"),
            Error::NonAsciiChars => f.write_str("contains non-ASCII chars"),
            Error::InvalidInput(message) => f.write_str(message),
            Error::Io(e) => e.fmt(f),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl StdError for Error {}
