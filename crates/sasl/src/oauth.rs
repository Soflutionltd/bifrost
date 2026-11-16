//! OAuth SASL payload construction (XOAUTH2, OAUTHBEARER).
//!
//! XOAUTH2 is the Google-defined non-IETF mechanism; OAUTHBEARER is RFC 7628.
//! Both wrap a bearer access token. Payloads are returned as a zeroizing
//! `Secret` of the *raw* (un-base64'd) bytes; the protocol crate frames them
//! for the wire (IMAP base64-encodes for AUTHENTICATE; SMTP's AUTH command
//! base64-encodes downstream).

use zeroize::Zeroizing;

use crate::{SaslError, Secret};

/// OAuth SASL mechanism, including its failed-authentication acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OAuthMechanism {
    /// Google's XOAUTH2 mechanism.
    Xoauth2,
    /// RFC 7628 OAUTHBEARER.
    OAuthBearer,
}

impl OAuthMechanism {
    /// SASL mechanism token used on the wire.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Xoauth2 => "XOAUTH2",
            Self::OAuthBearer => "OAUTHBEARER",
        }
    }

    /// Raw answer to a server's OAuth error continuation, before base64 framing.
    ///
    /// Google's XOAUTH2 exchange requires an empty answer; RFC 7628 Section
    /// 3.2.3 requires a single SOH byte for OAUTHBEARER. Both are UTF-8 strings.
    pub const fn error_ack(self) -> &'static str {
        match self {
            Self::Xoauth2 => "",
            Self::OAuthBearer => "\x01",
        }
    }
}

fn validate_fields(identity: &str, token: &str) -> Result<(), SaslError> {
    // Enterprise users expect saved credentials refused locally rather than
    // silently rewritten into another identity or token, through every API.
    if identity.contains(['\x01', '\0', '\r', '\n']) || token.contains(['\x01', '\0', '\r', '\n']) {
        return Err(SaslError::InvalidCredential(
            "OAuth fields contain a frame delimiter".into(),
        ));
    }
    Ok(())
}

/// XOAUTH2 client payload: `user=<user>\x01auth=Bearer <token>\x01\x01`.
///
/// XOAUTH2 has no GS2 framing, so neither field is GS2-escaped.
/// SOH, NUL, CR and LF in either field return `InvalidCredential`.
pub fn xoauth2_payload(user: &str, access_token: &str) -> Result<Secret, SaslError> {
    validate_fields(user, access_token)?;
    Ok(Secret::from(format!(
        "user={user}\x01auth=Bearer {access_token}\x01\x01"
    )))
}

/// OAUTHBEARER (RFC 7628) client payload:
/// `n,a=<identity>,\x01auth=Bearer <token>\x01\x01`.
///
/// The `a=` authorization identity is a GS2 attribute and is GS2-escaped
/// (`,` -> `=2C`, `=` -> `=3D`). The optional RFC 7628 `host=` / `port=`
/// attributes are deliberately omitted: bearer auth does not require them and
/// omitting them keeps the builder transport-agnostic.
/// SOH, NUL, CR and LF in either field return `InvalidCredential`.
pub fn oauthbearer_payload(identity: &str, access_token: &str) -> Result<Secret, SaslError> {
    validate_fields(identity, access_token)?;
    let identity = Zeroizing::new(crate::scram::escape_username(identity));
    Ok(Secret::from(format!(
        "n,a={},\x01auth=Bearer {}\x01\x01",
        identity.as_str(),
        access_token
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_error_acknowledgements_are_mechanism_specific() {
        assert_eq!(OAuthMechanism::Xoauth2.error_ack().as_bytes(), b"");
        assert_eq!(OAuthMechanism::OAuthBearer.error_ack().as_bytes(), b"\x01");
    }

    #[test]
    fn xoauth2_payload_pins_raw_bytes() {
        assert_eq!(
            xoauth2_payload("user", "token").unwrap().as_str(),
            "user=user\x01auth=Bearer token\x01\x01"
        );
    }

    #[test]
    fn xoauth2_payload_alice_access_token() {
        // Mirrors SMTP test_accepts_zeroizing_secrets.
        assert_eq!(
            xoauth2_payload("alice", "access-token").unwrap().as_str(),
            "user=alice\x01auth=Bearer access-token\x01\x01"
        );
    }

    #[test]
    fn oauthbearer_payload_omits_host_and_port() {
        // Mirrors SMTP test_oauthbearer.
        let payload = oauthbearer_payload(
            "user@example.com",
            "vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==",
        )
        .unwrap();
        assert_eq!(
            payload.as_str(),
            "n,a=user@example.com,\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==\x01\x01"
        );
        assert!(!payload.as_str().contains("\x01host="));
        assert!(!payload.as_str().contains("\x01port="));
    }

    #[test]
    fn oauth_payload_helpers_refuse_invalid_fields() {
        for delimiter in ['\x01', '\0', '\r', '\n'] {
            let invalid = format!("before{delimiter}after");
            for (identity, token) in [(invalid.as_str(), "token"), ("user", invalid.as_str())] {
                assert!(matches!(
                    xoauth2_payload(identity, token),
                    Err(SaslError::InvalidCredential(_))
                ));
                assert!(matches!(
                    oauthbearer_payload(identity, token),
                    Err(SaslError::InvalidCredential(_))
                ));
            }
        }
    }

    #[test]
    fn oauthbearer_payload_escapes_gs2_identity() {
        // Mirrors SMTP test_oauthbearer_escapes_gs2_identity.
        assert_eq!(
            oauthbearer_payload("a,b=c", "token").unwrap().as_str(),
            "n,a=a=2Cb=3Dc,\x01auth=Bearer token\x01\x01"
        );
    }
}
