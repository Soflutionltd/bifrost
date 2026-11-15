//! CRAM-MD5 (RFC 2195) response computation.

use crate::error::SaslError;
use crate::secret::Secret;

/// Compute the base64 CRAM-MD5 response for a challenge.
pub fn cram_md5_response(user: &str, pass: &str, challenge: &str) -> Result<Secret, SaslError> {
    use base64::Engine;
    let challenge = base64::engine::general_purpose::STANDARD
        .decode(challenge.trim())
        .map_err(|e| SaslError::Protocol(format!("invalid CRAM-MD5 challenge: {e}")))?;
    let response = cram_md5_payload(user, pass, &challenge)?;
    Ok(base64::engine::general_purpose::STANDARD
        .encode(response.as_bytes())
        .into())
}

pub(crate) fn cram_md5_payload(
    user: &str,
    pass: &str,
    challenge: &[u8],
) -> Result<Secret, SaslError> {
    use hmac::Mac as _;
    use std::fmt::Write;
    let mut mac = <hmac::Hmac<md5::Md5> as hmac::digest::KeyInit>::new_from_slice(pass.as_bytes())
        .map_err(|e| SaslError::LocalRuntime(format!("invalid CRAM-MD5 key: {e}")))?;
    mac.update(challenge);
    let digest = zeroize::Zeroizing::new(mac.finalize().into_bytes().to_vec());

    let mut response =
        zeroize::Zeroizing::new(String::with_capacity(user.len() + 1 + digest.len() * 2));
    response.push_str(user);
    response.push(' ');
    for byte in digest.iter() {
        let _ = write!(response, "{byte:02x}");
    }

    Ok(response.into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn cram_md5_response_matches_rfc_2195_vector() {
        let response = cram_md5_response(
            "tim",
            "tanstaaftanstaaf",
            "PDE4OTYuNjk3MTcwOTUyQHBvc3RvZmZpY2UucmVzdG9uLm1jaS5uZXQ+",
        )
        .unwrap();
        assert_eq!(
            response.as_str(),
            "dGltIGI5MTNhNjAyYzdlZGE3YTQ5NWI0ZTZlNzMzNGQzODkw"
        );
    }

    #[test]
    fn cram_md5_rejects_invalid_base64_challenge() {
        let err = cram_md5_response("tim", "tanstaaftanstaaf", "!!!not-base64!!!").unwrap_err();
        assert!(matches!(err, SaslError::Protocol(_)), "got {err:?}");
    }
}
