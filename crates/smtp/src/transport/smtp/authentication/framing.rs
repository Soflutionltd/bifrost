//! SMTP SASL continuation and enhanced-status payload framing.

use crate::transport::smtp::error::{self, Error};

/// Decode a `334` SASL continuation reply's payload to a UTF-8 string.
///
/// The single base64/UTF-8 decode path shared by the sync and async SCRAM
/// drivers (RFC 4954: server-first / server-final are base64 SASL messages).
/// Errors with a parse-class `Error` when the reply is not a `334`
/// continuation or its payload is not valid base64/UTF-8.
pub(crate) fn decode_auth_challenge(
    response: &crate::transport::smtp::response::Response,
) -> Result<String, Error> {
    if !response.has_code(334) {
        return Err(error::parse("expected a SCRAM 334 continuation"));
    }
    decode_scram_payload(response)
}

/// Decode the base64 SASL payload on the reply's first line, regardless of the
/// reply code. A leading enhanced status code is skipped when present.
///
/// Used for the SCRAM server-final, which RFC 4954 carries on a `334`
/// continuation but which some servers (field-observed) fold directly onto the
/// `235` success reply (`235 v=...`). The caller is responsible for validating
/// the reply code; this only extracts and decodes the payload.
pub(crate) fn decode_scram_payload(
    response: &crate::transport::smtp::response::Response,
) -> Result<String, Error> {
    let line = response
        .first_line()
        .ok_or_else(|| error::parse("could not read SCRAM challenge"))?;
    let mut words = line.split_whitespace();
    let first = words
        .next()
        .ok_or_else(|| error::parse("could not read SCRAM challenge"))?;
    // ENHANCEDSTATUSCODES puts a status token before the SASL payload on the
    // same line (for example `235 2.7.0 <base64>`). Skip it only when it is
    // the valid enhanced status parsed from this response.
    let enhanced = response.enhanced_status_code().map(|code| code.to_string());
    let encoded = if enhanced.as_deref() == Some(first) {
        words
            .next()
            .ok_or_else(|| error::parse("could not read SCRAM challenge"))?
    } else {
        first
    };
    let decoded = crate::base64::decode(encoded).map_err(error::parse)?;
    String::from_utf8(decoded).map_err(error::parse)
}
