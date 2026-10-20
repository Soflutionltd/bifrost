//! OAuth SASL payload construction (XOAUTH2, OAUTHBEARER).
//!
//! XOAUTH2 is the Google-defined non-IETF mechanism; OAUTHBEARER is RFC 7628.
//! Both wrap a bearer access token. Payloads are returned as a zeroizing
//! `Secret` of the *raw* (un-base64'd) bytes; the protocol crate frames them
//! for the wire (IMAP base64-encodes for AUTHENTICATE; SMTP's AUTH command
//! base64-encodes downstream).

use zeroize::Zeroizing;

use crate::Secret;

/// Strip the `\x01` (SOH) frame delimiter from an interpolated OAuth field.
///
/// XOAUTH2 and OAUTHBEARER delimit attributes with `\x01`, so an embedded
/// `\x01` in `user` / `identity` / token would desync the frame and let a
/// caller inject extra attributes. Legitimate values never contain `\x01`
/// (tokens are base64url/JWT, identities are email-shaped), so removing it is
/// lossless for real input and closes the injection surface. The builders keep
/// their infallible `-> Secret` signature (the consumers in IMAP/SMTP use the
/// payload inline), so this defends rather than erroring.
fn strip_frame_delim(field: &str) -> Zeroizing<String> {
    // This may be an access token. Own the copy in zeroizing storage before
    // stripping delimiters so no ordinary String copy survives the builder.
    let mut cleaned = Zeroizing::new(field.to_owned());
    cleaned.retain(|character| character != '\x01');
    cleaned
}

/// XOAUTH2 client payload: `user=<user>\x01auth=Bearer <token>\x01\x01`.
///
/// XOAUTH2 has no GS2 framing, so neither field is GS2-escaped. The
/// `\x01`-delimited frame is byte-literal; any `\x01` embedded in `user` or
/// `access_token` is stripped first (see [`strip_frame_delim`]) so it cannot
/// desync the frame.
pub fn xoauth2_payload(user: &str, access_token: &str) -> Secret {
    let user = strip_frame_delim(user);
    let access_token = strip_frame_delim(access_token);
    Secret::from(format!(
        "user={}\x01auth=Bearer {}\x01\x01",
        user.as_str(),
        access_token.as_str()
    ))
}

/// OAUTHBEARER (RFC 7628) client payload:
/// `n,a=<identity>,\x01auth=Bearer <token>\x01\x01`.
///
/// The `a=` authorization identity is a GS2 attribute and is GS2-escaped
/// (`,` -> `=2C`, `=` -> `=3D`). The optional RFC 7628 `host=` / `port=`
/// attributes are deliberately omitted: bearer auth does not require them and
/// omitting them keeps the builder transport-agnostic.
pub fn oauthbearer_payload(identity: &str, access_token: &str) -> Secret {
    // `escape_username` handles the GS2 `=`/`,` escapes but not `\x01`; strip the
    // frame delimiter from both fields so neither can inject extra attributes.
    let identity = Zeroizing::new(crate::scram::escape_username(&strip_frame_delim(identity)));
    let access_token = strip_frame_delim(access_token);
    Secret::from(format!(
        "n,a={},\x01auth=Bearer {}\x01\x01",
        identity.as_str(),
        access_token.as_str()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xoauth2_payload_pins_raw_bytes() {
        assert_eq!(
            xoauth2_payload("user", "token").as_str(),
            "user=user\x01auth=Bearer token\x01\x01"
        );
    }

    #[test]
    fn xoauth2_payload_alice_access_token() {
        // Mirrors SMTP test_accepts_zeroizing_secrets.
        assert_eq!(
            xoauth2_payload("alice", "access-token").as_str(),
            "user=alice\x01auth=Bearer access-token\x01\x01"
        );
    }

    #[test]
    fn oauthbearer_payload_omits_host_and_port() {
        // Mirrors SMTP test_oauthbearer.
        let payload = oauthbearer_payload(
            "user@example.com",
            "vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==",
        );
        assert_eq!(
            payload.as_str(),
            "n,a=user@example.com,\x01auth=Bearer vF9dft4qmTc2Nvb3RlckBhbHRhdmlzdGEuY29tCg==\x01\x01"
        );
        assert!(!payload.as_str().contains("\x01host="));
        assert!(!payload.as_str().contains("\x01port="));
    }

    #[test]
    fn xoauth2_payload_strips_embedded_frame_delimiter() {
        // An embedded \x01 in either field must not survive into the frame.
        let payload = xoauth2_payload("user\x01auth=Bearer evil", "tok\x01en");
        assert_eq!(
            payload.as_str(),
            "user=userauth=Bearer evil\x01auth=Bearer token\x01\x01"
        );
        // Three structural \x01 (the user/auth separator plus the two trailing
        // delimiters); none injected from the field contents.
        assert_eq!(payload.as_str().matches('\x01').count(), 3);
    }

    #[test]
    fn oauthbearer_payload_strips_embedded_frame_delimiter() {
        let payload = oauthbearer_payload("id\x01entity", "tok\x01en");
        assert_eq!(
            payload.as_str(),
            "n,a=identity,\x01auth=Bearer token\x01\x01"
        );
        assert_eq!(payload.as_str().matches('\x01').count(), 3);
    }

    #[test]
    fn oauthbearer_payload_escapes_gs2_identity() {
        // Mirrors SMTP test_oauthbearer_escapes_gs2_identity.
        assert_eq!(
            oauthbearer_payload("a,b=c", "token").as_str(),
            "n,a=a=2Cb=3Dc,\x01auth=Bearer token\x01\x01"
        );
    }

    #[test]
    fn stripped_fields_use_zeroizing_storage() {
        let cleaned: Zeroizing<String> = strip_frame_delim("tok\x01en");
        assert_eq!(cleaned.as_str(), "token");
    }
}
